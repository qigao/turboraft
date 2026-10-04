#include <turboraft/raft_service.h>

#include "raft_service_internal.h"

#include <tinytest.h>
#include <salts_error.h>

#include <stdlib.h>
#include <string.h>

typedef struct service_test_state {
    size_t transaction_count;
    size_t commit_count;
    size_t apply_count;
    size_t message_count;
    size_t successful_message_count;
    size_t max_successful_messages;
    bool limit_successful_messages;
    int enqueue_result;
    tr_raft_node_id_t blocked_peer_id;
    size_t attempted_by_peer[TR_RAFT_MAX_MEMBERS + 1U];
    size_t successful_by_peer[TR_RAFT_MAX_MEMBERS + 1U];
    size_t snapshot_request_count;
    size_t successful_snapshot_request_count;
    int snapshot_enqueue_result;
} service_test_state_t;

typedef struct service_stage_allocator_test {
    bool fail_allocation;
    size_t allocation_count;
    size_t deallocation_count;
} service_stage_allocator_test_t;

static void *service_stage_test_allocate(
    void *context, size_t count, size_t size)
{
    service_stage_allocator_test_t *allocator =
        (service_stage_allocator_test_t *)context;

    ++allocator->allocation_count;
    if (allocator->fail_allocation) {
        return NULL;
    }
    return calloc(count, size);
}

static void service_stage_test_deallocate(void *context, void *memory)
{
    service_stage_allocator_test_t *allocator =
        (service_stage_allocator_test_t *)context;

    if (memory != NULL) {
        ++allocator->deallocation_count;
        free(memory);
    }
}

static int service_storage_begin(void *context)
{
    ++((service_test_state_t *) context)->transaction_count;
    return SALTS_OK;
}

static int service_storage_hard_state(
    void *context,
    tr_raft_term_t term,
    tr_raft_node_id_t voted_for)
{
    (void) context;
    (void) term;
    (void) voted_for;
    return SALTS_OK;
}

static int service_storage_truncate(void *context, tr_raft_index_t index)
{
    (void) context;
    (void) index;
    return SALTS_OK;
}

static int service_storage_append(
    void *context,
    const tr_raft_entry_t *entries,
    size_t entry_count)
{
    (void) context;
    return entries != NULL && entry_count != 0U ? SALTS_OK : SALTS_EINVAL;
}

static int service_storage_commit_index(
    void *context,
    tr_raft_index_t commit_index)
{
    (void) context;
    (void) commit_index;
    return SALTS_OK;
}

static int service_storage_commit(void *context)
{
    ++((service_test_state_t *) context)->commit_count;
    return SALTS_OK;
}

static int service_storage_rollback(void *context)
{
    (void) context;
    return SALTS_OK;
}

static int service_transport_enqueue(
    void *context,
    const tr_raft_message_t *message)
{
    service_test_state_t *state = (service_test_state_t *) context;

    if (message == NULL) {
        return SALTS_EINVAL;
    }
    ++state->message_count;
    if (message->to <= TR_RAFT_MAX_MEMBERS) {
        ++state->attempted_by_peer[message->to];
    }
    if (state->blocked_peer_id != 0U &&
        message->to == state->blocked_peer_id) {
        return SALTS_ENOSPC;
    }
    if (state->enqueue_result != SALTS_OK) {
        return state->enqueue_result;
    }
    if (state->limit_successful_messages &&
        state->successful_message_count == state->max_successful_messages) {
        return SALTS_ENOSPC;
    }
    ++state->successful_message_count;
    if (message->to <= TR_RAFT_MAX_MEMBERS) {
        ++state->successful_by_peer[message->to];
    }
    return SALTS_OK;
}

static int service_transport_enqueue_snapshot(
    void *context,
    const tr_raft_snapshot_request_t *request)
{
    service_test_state_t *state = (service_test_state_t *) context;

    if (request == NULL) {
        return SALTS_EINVAL;
    }
    ++state->snapshot_request_count;
    if (state->snapshot_enqueue_result == SALTS_OK) {
        ++state->successful_snapshot_request_count;
    }
    return state->snapshot_enqueue_result;
}

static int service_apply(
    void *context,
    const tr_raft_entry_t *entries,
    size_t entry_count)
{
    service_test_state_t *state = (service_test_state_t *) context;

    if (entries == NULL || entry_count == 0U) {
        return SALTS_EINVAL;
    }
    state->apply_count += entry_count;
    return SALTS_OK;
}

static void service_configure(
    tr_raft_service_config_t *config,
    service_test_state_t *state,
    const tr_raft_node_id_t *voters,
    size_t voter_count)
{
    memset(config, 0, sizeof(*config));
    config->core.self_id = 1U;
    config->core.voters = voters;
    config->core.voter_count = voter_count;
    config->core.heartbeat_ticks = 1U;
    config->core.election_min_ticks = 3U;
    config->core.election_max_ticks = 5U;
    config->core.initial_election_timeout_ticks = 3U;
    config->core.max_log_entries = 16U;
    config->storage.context = state;
    config->storage.begin = service_storage_begin;
    config->storage.write_hard_state = service_storage_hard_state;
    config->storage.truncate_log = service_storage_truncate;
    config->storage.append_log = service_storage_append;
    config->storage.write_commit_index = service_storage_commit_index;
    config->storage.commit = service_storage_commit;
    config->storage.rollback = service_storage_rollback;
    config->transport.context = state;
    config->transport.enqueue = service_transport_enqueue;
    config->transport.snapshot_context = state;
    config->transport.enqueue_snapshot = service_transport_enqueue_snapshot;
    config->state_machine.context = state;
    config->state_machine.apply_batch = service_apply;
}

spec("raft service")
{
    it("owns election proposal persistence and apply ordering")
    {
        const tr_raft_node_id_t voters[] = {1U};
        const char command[] = "abc";
        tr_raft_service_config_t config;
        tr_raft_service_t *service = NULL;
        tr_raft_service_status_t status;
        tr_raft_tick_t tick = {3U, 4U};
        tr_raft_proposal_t proposal;
        service_test_state_t state;

        memset(&state, 0, sizeof(state));
        service_configure(&config, &state, voters, 1U);
        config.core.max_pending_reads = 3U;
        check_equal(tr_raft_service_create(&config, &service), SALTS_OK);
        check_equal(tr_raft_service_tick(service, &tick), SALTS_OK);
        check_equal(tr_raft_service_status(service, &status), SALTS_OK);
        check_equal(status.core.role, TR_RAFT_LEADER);

        proposal.command_id = 1U;
        proposal.data = command;
        proposal.data_length = sizeof(command) - 1U;
        check_equal(tr_raft_service_propose(service, &proposal), SALTS_OK);
        check_equal(state.apply_count, 1U);
        check(state.commit_count >= 2U);
        check_equal(tr_raft_service_status(service, &status), SALTS_OK);
        check_equal(status.core.commit_index, 1U);
        check_equal(status.core.applied_index, 1U);
        {
            tr_raft_read_state_t read_state;

            check_equal(tr_raft_service_read_index(service, 21U), SALTS_OK);
            check_equal(tr_raft_service_read_index(service, 21U),
                        SALTS_EALREADY);
            check_equal(tr_raft_service_read_index(service, 22U), SALTS_OK);
            check_equal(tr_raft_service_read_index(service, 23U), SALTS_OK);
            check_equal(tr_raft_service_read_index(service, 24U),
                        SALTS_ENOSPC);
            check_equal(tr_raft_service_status(service, &status), SALTS_OK);
            check_equal(status.completed_read_count, 3U);
            check_equal(status.max_completed_reads, 3U);
            check_equal(tr_raft_service_prepare_backup(service), SALTS_EBUSY);

            check_equal(tr_raft_service_take_read_state(service, &read_state),
                         SALTS_OK);
            check_equal(read_state.context_id, 21U);
            check_equal(read_state.index, 1U);
            check_equal(tr_raft_service_take_read_state(service, &read_state),
                         SALTS_OK);
            check_equal(read_state.context_id, 22U);
            check_equal(read_state.index, 1U);
            check_equal(tr_raft_service_take_read_state(service, &read_state),
                         SALTS_OK);
            check_equal(read_state.context_id, 23U);
            check_equal(read_state.index, 1U);
            check_equal(tr_raft_service_take_read_state(service, &read_state),
                         SALTS_ENOENT);
        }

        config.core.initial_term = 2U;
        config.core.initial_last_log_index = 5U;
        config.core.initial_last_log_term = 2U;
        config.core.initial_commit_index = 5U;
        config.core.initial_applied_index = 5U;
        check_equal(tr_raft_service_reload(service, &config.core), SALTS_OK);
        check_equal(tr_raft_service_status(service, &status), SALTS_OK);
        check(!status.faulted);
        check_equal(status.core.last_log_index, 5U);
        check_equal(status.core.applied_index, 5U);

        tr_raft_service_destroy(service);
    }

    it("faults permanently when Runtime transport fails")
    {
        const tr_raft_node_id_t voters[] = {1U, 2U, 3U};
        tr_raft_service_config_t config;
        tr_raft_service_t *service = NULL;
        tr_raft_service_status_t status;
        tr_raft_tick_t tick = {3U, 4U};
        service_test_state_t state;

        memset(&state, 0, sizeof(state));
        state.enqueue_result = SALTS_EPIPE;
        service_configure(&config, &state, voters, 3U);
        check_equal(tr_raft_service_create(&config, &service), SALTS_OK);
        check_equal(tr_raft_service_tick(service, &tick), SALTS_EPIPE);
        check_equal(tr_raft_service_status(service, &status), SALTS_OK);
        check(status.faulted);
        check_equal(status.cause, SALTS_EPIPE);
        check_equal(tr_raft_service_tick(service, &tick), SALTS_EPROTO);

        tr_raft_service_destroy(service);
    }

    it("faults cleanly when peer staging allocation fails")
    {
        const tr_raft_node_id_t voters[] = {1U, 2U, 3U};
        tr_raft_service_config_t config;
        tr_raft_service_t *service = NULL;
        tr_raft_service_status_t status;
        tr_raft_service_peer_delivery_status_t delivery;
        tr_raft_tick_t tick = {3U, 4U};
        service_test_state_t state;
        service_stage_allocator_test_t allocator;

        memset(&state, 0, sizeof(state));
        memset(&allocator, 0, sizeof(allocator));
        state.blocked_peer_id = 3U;
        allocator.fail_allocation = true;
        service_configure(&config, &state, voters, 3U);
        check_equal(tr_raft_service_create(&config, &service), SALTS_OK);
        check_equal(tr_raft_service_set_stage_allocator_for_test(
                        service, service_stage_test_allocate,
                        service_stage_test_deallocate, &allocator),
                    SALTS_OK);

        check_equal(tr_raft_service_tick(service, &tick), SALTS_ENOMEM);
        check_equal(allocator.allocation_count, 1U);
        check_equal(allocator.deallocation_count, 0U);
        check_equal(tr_raft_service_status(service, &status), SALTS_OK);
        check(status.faulted);
        check_equal(status.cause, SALTS_ENOMEM);
        check_equal(tr_raft_service_get_peer_delivery_status(
                        service, 3U, &delivery), SALTS_OK);
        check_equal(delivery.staged_message_count, 0U);
        check_equal(delivery.staged_message_bytes, 0U);

        tr_raft_service_destroy(service);
        check_equal(allocator.deallocation_count, 0U);
    }

    it("releases a staged peer suffix exactly once on destroy")
    {
        const tr_raft_node_id_t voters[] = {1U, 2U, 3U};
        tr_raft_service_config_t config;
        tr_raft_service_t *service = NULL;
        tr_raft_service_peer_delivery_status_t delivery;
        tr_raft_tick_t tick = {3U, 4U};
        service_test_state_t state;
        service_stage_allocator_test_t allocator;

        memset(&state, 0, sizeof(state));
        memset(&allocator, 0, sizeof(allocator));
        state.blocked_peer_id = 3U;
        service_configure(&config, &state, voters, 3U);
        check_equal(tr_raft_service_create(&config, &service), SALTS_OK);
        check_equal(tr_raft_service_set_stage_allocator_for_test(
                        service, service_stage_test_allocate,
                        service_stage_test_deallocate, &allocator),
                    SALTS_OK);

        check_equal(tr_raft_service_tick(service, &tick), SALTS_OK);
        check_equal(allocator.allocation_count, 1U);
        check_equal(allocator.deallocation_count, 0U);
        check_equal(tr_raft_service_get_peer_delivery_status(
                        service, 3U, &delivery), SALTS_OK);
        check(delivery.paused);
        check_equal(delivery.staged_message_count, 1U);

        tr_raft_service_destroy(service);
        check_equal(allocator.deallocation_count, 1U);
    }

    it("keeps backup quiescent until staged peer transport drains")
    {
        const tr_raft_node_id_t voters[] = {1U, 2U, 3U};
        tr_raft_service_config_t config;
        tr_raft_service_t *service = NULL;
        tr_raft_service_status_t status;
        tr_raft_service_peer_delivery_status_t delivery;
        tr_raft_tick_t tick = {3U, 4U};
        service_test_state_t state;
        service_stage_allocator_test_t allocator;

        memset(&state, 0, sizeof(state));
        memset(&allocator, 0, sizeof(allocator));
        state.blocked_peer_id = 3U;
        service_configure(&config, &state, voters, 3U);
        check_equal(tr_raft_service_create(&config, &service), SALTS_OK);
        check_equal(tr_raft_service_set_stage_allocator_for_test(
                        service, service_stage_test_allocate,
                        service_stage_test_deallocate, &allocator),
                    SALTS_OK);

        check_equal(tr_raft_service_tick(service, &tick), SALTS_OK);
        check_equal(allocator.allocation_count, 1U);
        check_equal(allocator.deallocation_count, 0U);
        check_equal(tr_raft_service_get_peer_delivery_status(
                        service, 3U, &delivery), SALTS_OK);
        check(delivery.paused);
        check_equal(delivery.staged_message_count, 1U);

        check_equal(tr_raft_service_prepare_backup(service), SALTS_EBUSY);
        check_equal(allocator.deallocation_count, 0U);
        check_equal(tr_raft_service_status(service, &status), SALTS_OK);
        check_false(status.backup_prepared);

        state.blocked_peer_id = 0U;
        check_equal(tr_raft_service_prepare_backup(service), SALTS_OK);
        check_equal(allocator.deallocation_count, 1U);
        check_equal(tr_raft_service_get_peer_delivery_status(
                        service, 3U, &delivery), SALTS_OK);
        check_false(delivery.paused);
        check_equal(delivery.staged_message_count, 0U);
        check_equal(delivery.last_error, SALTS_OK);
        check_equal(tr_raft_service_status(service, &status), SALTS_OK);
        check(status.backup_prepared);
        check_equal(tr_raft_service_poll(service), SALTS_EBUSY);

        check_equal(tr_raft_service_resume_backup(service, &config.storage),
                    SALTS_OK);
        check_equal(tr_raft_service_status(service, &status), SALTS_OK);
        check_false(status.backup_prepared);

        tr_raft_service_destroy(service);
        check_equal(allocator.deallocation_count, 1U);
    }

    it("bounds staged allocations across all remote voters")
    {
        tr_raft_node_id_t voters[TR_RAFT_MAX_MEMBERS];
        tr_raft_service_config_t config;
        tr_raft_service_t *service = NULL;
        tr_raft_service_peer_delivery_status_t delivery;
        tr_raft_tick_t tick = {3U, 4U};
        service_test_state_t state;
        service_stage_allocator_test_t allocator;
        size_t index;

        memset(&state, 0, sizeof(state));
        memset(&allocator, 0, sizeof(allocator));
        for (index = 0U; index < TR_RAFT_MAX_MEMBERS; ++index) {
            voters[index] = (tr_raft_node_id_t)(index + 1U);
        }
        state.enqueue_result = SALTS_ENOSPC;
        service_configure(&config, &state, voters, TR_RAFT_MAX_MEMBERS);
        check_equal(tr_raft_service_create(&config, &service), SALTS_OK);
        check_equal(tr_raft_service_set_stage_allocator_for_test(
                        service, service_stage_test_allocate,
                        service_stage_test_deallocate, &allocator),
                    SALTS_OK);

        check_equal(tr_raft_service_tick(service, &tick), SALTS_OK);
        check_equal(allocator.allocation_count, TR_RAFT_MAX_MEMBERS - 1U);
        check_equal(allocator.deallocation_count, 0U);
        for (index = 2U; index <= TR_RAFT_MAX_MEMBERS; ++index) {
            check_equal(tr_raft_service_get_peer_delivery_status(
                            service, (tr_raft_node_id_t)index, &delivery),
                        SALTS_OK);
            check(delivery.paused);
            check_equal(delivery.staged_message_count, 1U);
            check_equal(delivery.staged_message_bytes,
                        sizeof(tr_raft_message_t));
        }

        check_equal(tr_raft_service_tick(service, &tick), SALTS_OK);
        check_equal(allocator.allocation_count, TR_RAFT_MAX_MEMBERS - 1U);
        check_equal(tr_raft_service_set_stage_allocator_for_test(
                        service, NULL, NULL, NULL),
                    SALTS_EBUSY);

        tr_raft_service_destroy(service);
        check_equal(allocator.deallocation_count, TR_RAFT_MAX_MEMBERS - 1U);
    }

    it("releases staged peer delivery once when membership removes the peer")
    {
        const tr_raft_node_id_t voters[] = {1U, 2U, 3U};
        const tr_raft_node_id_t target_voters[] = {1U, 2U};
        tr_raft_service_config_t config;
        tr_raft_service_t *service = NULL;
        tr_raft_service_status_t status;
        tr_raft_service_peer_delivery_status_t delivery;
        tr_raft_membership_change_t change;
        tr_raft_message_t response;
        tr_raft_tick_t election_tick = {3U, 4U};
        tr_raft_tick_t heartbeat_tick = {1U, 4U};
        service_test_state_t state;
        service_stage_allocator_test_t allocator;
        tr_raft_index_t joint_index;
        tr_raft_index_t final_index;
        size_t attempted_removed_peer;

        memset(&state, 0, sizeof(state));
        memset(&allocator, 0, sizeof(allocator));
        state.blocked_peer_id = 3U;
        service_configure(&config, &state, voters, 3U);
        check_equal(tr_raft_service_create(&config, &service), SALTS_OK);
        check_equal(tr_raft_service_set_stage_allocator_for_test(
                        service, service_stage_test_allocate,
                        service_stage_test_deallocate, &allocator),
                    SALTS_OK);

        check_equal(tr_raft_service_tick(service, &election_tick), SALTS_OK);
        check_equal(allocator.allocation_count, 1U);
        check_equal(allocator.deallocation_count, 0U);
        check_equal(tr_raft_service_get_peer_delivery_status(
                        service, 3U, &delivery), SALTS_OK);
        check(delivery.paused);
        check_equal(delivery.staged_message_count, 1U);

        memset(&response, 0, sizeof(response));
        response.type = TR_RAFT_MSG_PRE_VOTE_RESPONSE;
        response.from = 2U;
        response.to = 1U;
        response.campaign_term = 1U;
        response.granted = true;
        check_equal(tr_raft_service_step(service, &response), SALTS_OK);

        response.type = TR_RAFT_MSG_VOTE_RESPONSE;
        response.term = 1U;
        check_equal(tr_raft_service_step(service, &response), SALTS_OK);
        check_equal(tr_raft_service_status(service, &status), SALTS_OK);
        check_equal(status.core.role, TR_RAFT_LEADER);

        memset(&change, 0, sizeof(change));
        change.transition_id = 901U;
        change.voters = target_voters;
        change.voter_count = 2U;
        check_equal(tr_raft_service_change_membership(service, &change),
                    SALTS_OK);
        check_equal(tr_raft_service_status(service, &status), SALTS_OK);
        joint_index = status.core.last_log_index;
        check(joint_index != 0U);

        memset(&response, 0, sizeof(response));
        response.type = TR_RAFT_MSG_APPEND_RESPONSE;
        response.from = 2U;
        response.to = 1U;
        response.term = status.core.term;
        response.granted = true;
        response.previous_log_index = joint_index - 1U;
        response.match_index = joint_index;
        check_equal(tr_raft_service_step(service, &response), SALTS_OK);
        check_equal(tr_raft_service_status(service, &status), SALTS_OK);
        check_equal(status.core.commit_index, joint_index);
        check(status.core.joint_configuration);

        check_equal(tr_raft_service_tick(service, &heartbeat_tick), SALTS_OK);
        check_equal(tr_raft_service_status(service, &status), SALTS_OK);
        final_index = status.core.last_log_index;
        check(final_index > joint_index);

        response.term = status.core.term;
        response.previous_log_index = final_index - 1U;
        response.match_index = final_index;
        check_equal(tr_raft_service_step(service, &response), SALTS_OK);
        check_equal(tr_raft_service_status(service, &status), SALTS_OK);
        check_equal(status.core.commit_index, final_index);
        check_false(status.core.joint_configuration);
        check_equal(status.core.voter_count, 2U);
        check_equal(allocator.deallocation_count, 0U);

        attempted_removed_peer = state.attempted_by_peer[3U];
        check_equal(tr_raft_service_poll(service), SALTS_OK);
        check_equal(allocator.deallocation_count, 1U);
        check_equal(state.attempted_by_peer[3U], attempted_removed_peer);
        check_equal(tr_raft_service_get_peer_delivery_status(
                        service, 3U, &delivery), SALTS_ENOENT);

        tr_raft_service_destroy(service);
        check_equal(allocator.deallocation_count, 1U);
    }

    it("owns one staging allocation per peer pause generation")
    {
        const tr_raft_node_id_t voters[] = {1U, 2U, 3U};
        tr_raft_service_config_t config;
        tr_raft_service_t *service = NULL;
        tr_raft_service_peer_delivery_status_t delivery;
        tr_raft_tick_t election_tick = {3U, 4U};
        tr_raft_tick_t heartbeat_tick = {1U, 4U};
        tr_raft_message_t response;
        service_test_state_t state;
        service_stage_allocator_test_t allocator;

        memset(&state, 0, sizeof(state));
        memset(&allocator, 0, sizeof(allocator));
        state.blocked_peer_id = 3U;
        service_configure(&config, &state, voters, 3U);
        check_equal(tr_raft_service_create(&config, &service), SALTS_OK);
        check_equal(tr_raft_service_set_stage_allocator_for_test(
                        service, service_stage_test_allocate,
                        service_stage_test_deallocate, &allocator),
                    SALTS_OK);

        check_equal(tr_raft_service_tick(service, &election_tick), SALTS_OK);
        check_equal(allocator.allocation_count, 1U);
        check_equal(allocator.deallocation_count, 0U);

        state.blocked_peer_id = 0U;
        check_equal(tr_raft_service_poll(service), SALTS_OK);
        check_equal(allocator.allocation_count, 1U);
        check_equal(allocator.deallocation_count, 1U);
        check_equal(tr_raft_service_get_peer_delivery_status(
                        service, 3U, &delivery), SALTS_OK);
        check_false(delivery.paused);
        check_equal(delivery.staged_message_count, 0U);

        memset(&response, 0, sizeof(response));
        response.type = TR_RAFT_MSG_PRE_VOTE_RESPONSE;
        response.from = 2U;
        response.to = 1U;
        response.campaign_term = 1U;
        response.granted = true;
        check_equal(tr_raft_service_step(service, &response), SALTS_OK);

        response.type = TR_RAFT_MSG_VOTE_RESPONSE;
        response.term = 1U;
        check_equal(tr_raft_service_step(service, &response), SALTS_OK);

        state.blocked_peer_id = 3U;
        check_equal(tr_raft_service_tick(service, &heartbeat_tick), SALTS_OK);
        check_equal(allocator.allocation_count, 2U);
        check_equal(allocator.deallocation_count, 1U);
        check_equal(tr_raft_service_get_peer_delivery_status(
                        service, 3U, &delivery), SALTS_OK);
        check(delivery.paused);
        check_equal(delivery.staged_message_count, 1U);

        state.blocked_peer_id = 0U;
        check_equal(tr_raft_service_poll(service), SALTS_OK);
        check_equal(allocator.allocation_count, 2U);
        check_equal(allocator.deallocation_count, 2U);
        check_equal(tr_raft_service_get_peer_delivery_status(
                        service, 3U, &delivery), SALTS_OK);
        check_false(delivery.paused);

        state.blocked_peer_id = 3U;
        check_equal(tr_raft_service_tick(service, &heartbeat_tick), SALTS_OK);
        check_equal(allocator.allocation_count, 3U);
        check_equal(allocator.deallocation_count, 2U);

        state.blocked_peer_id = 0U;
        check_equal(tr_raft_service_poll(service), SALTS_OK);
        check_equal(allocator.allocation_count, 3U);
        check_equal(allocator.deallocation_count, 3U);

        tr_raft_service_destroy(service);
        check_equal(allocator.deallocation_count, 3U);
    }

    it("retains deterministic seed 0x51A7E5 across repeated peer saturation recovery")
    {
        const tr_raft_node_id_t voters[] = {1U, 2U, 3U};
        const char command[] = "x";
        tr_raft_service_config_t config;
        tr_raft_service_t *service = NULL;
        tr_raft_service_status_t status;
        tr_raft_service_peer_delivery_status_t delivery;
        tr_raft_read_state_t read_state;
        tr_raft_tick_t election_tick = {3U, 4U};
        tr_raft_tick_t heartbeat_tick = {1U, 4U};
        tr_raft_proposal_t proposal;
        tr_raft_message_t response;
        service_test_state_t state;
        service_stage_allocator_test_t allocator;
        uint32_t chaos_state = UINT32_C(0x51A7E5);
        size_t round;

        memset(&state, 0, sizeof(state));
        memset(&allocator, 0, sizeof(allocator));
        service_configure(&config, &state, voters, 3U);
        check_equal(tr_raft_service_create(&config, &service), SALTS_OK);
        check_equal(tr_raft_service_set_stage_allocator_for_test(
                        service, service_stage_test_allocate,
                        service_stage_test_deallocate, &allocator),
                    SALTS_OK);

        check_equal(tr_raft_service_tick(service, &election_tick), SALTS_OK);
        memset(&response, 0, sizeof(response));
        response.type = TR_RAFT_MSG_PRE_VOTE_RESPONSE;
        response.from = 2U;
        response.to = 1U;
        response.campaign_term = 1U;
        response.granted = true;
        check_equal(tr_raft_service_step(service, &response), SALTS_OK);
        response.type = TR_RAFT_MSG_VOTE_RESPONSE;
        response.term = 1U;
        check_equal(tr_raft_service_step(service, &response), SALTS_OK);
        check_equal(tr_raft_service_status(service, &status), SALTS_OK);
        check_equal(status.core.role, TR_RAFT_LEADER);
        check_equal(allocator.allocation_count, 0U);
        check_equal(allocator.deallocation_count, 0U);

        memset(&proposal, 0, sizeof(proposal));
        proposal.data = command;
        proposal.data_length = sizeof(command) - 1U;

        for (round = 0U; round < 8U; ++round) {
            size_t blocked_ticks;
            size_t tick_index;
            size_t generation_allocations = allocator.allocation_count;
            size_t peer2_attempts_before_recovery;
            size_t peer3_success_before_recovery;
            size_t staged_count;
            tr_raft_index_t proposal_index;

            chaos_state = chaos_state * UINT32_C(1664525) +
                          UINT32_C(1013904223);
            blocked_ticks = 1U + (size_t)(chaos_state % UINT32_C(3));

            state.blocked_peer_id = 3U;
            for (tick_index = 0U; tick_index < blocked_ticks; ++tick_index) {
                check_equal(tr_raft_service_tick(service, &heartbeat_tick),
                            SALTS_OK);
                check_equal(tr_raft_service_get_peer_delivery_status(
                                service, 3U, &delivery),
                            SALTS_OK);
                check(delivery.paused);
                check_equal(delivery.staged_message_count, 1U);
                check_equal(allocator.allocation_count,
                            generation_allocations + 1U);
                check_equal(allocator.deallocation_count,
                            generation_allocations);
            }

            proposal.command_id = (uint64_t)(1000U + round);
            check_equal(tr_raft_service_propose(service, &proposal), SALTS_OK);
            check_equal(tr_raft_service_status(service, &status), SALTS_OK);
            proposal_index = status.core.last_log_index;
            check_equal(proposal_index, (tr_raft_index_t)(round + 1U));

            memset(&response, 0, sizeof(response));
            response.type = TR_RAFT_MSG_APPEND_RESPONSE;
            response.from = 2U;
            response.to = 1U;
            response.term = status.core.term;
            response.granted = true;
            response.previous_log_index = proposal_index - 1U;
            response.match_index = proposal_index;
            check_equal(tr_raft_service_step(service, &response), SALTS_OK);
            check_equal(tr_raft_service_status(service, &status), SALTS_OK);
            check_equal(status.core.commit_index, proposal_index);
            check_equal(status.core.applied_index, proposal_index);
            check_equal(state.apply_count, round + 1U);

            check_equal(tr_raft_service_get_peer_delivery_status(
                            service, 3U, &delivery),
                        SALTS_OK);
            check(delivery.paused);
            check_equal(delivery.staged_message_count, 1U);
            staged_count = delivery.staged_message_count;
            peer2_attempts_before_recovery = state.attempted_by_peer[2U];
            peer3_success_before_recovery = state.successful_by_peer[3U];

            state.blocked_peer_id = 0U;
            check_equal(tr_raft_service_take_read_state(service, &read_state),
                        SALTS_ENOENT);
            check_equal(state.attempted_by_peer[2U],
                        peer2_attempts_before_recovery);
            check_equal(state.successful_by_peer[3U],
                        peer3_success_before_recovery + staged_count);
            check_equal(allocator.allocation_count,
                        generation_allocations + 1U);
            check_equal(allocator.deallocation_count,
                        generation_allocations + 1U);

            check_equal(tr_raft_service_get_peer_delivery_status(
                            service, 3U, &delivery),
                        SALTS_OK);
            check_false(delivery.paused);
            check_equal(delivery.staged_message_count, 0U);
            check_equal(delivery.staged_message_bytes, 0U);
            check_equal(delivery.last_error, SALTS_OK);
            check_equal(tr_raft_service_status(service, &status), SALTS_OK);
            check_false(status.faulted);
        }

        check_equal(allocator.allocation_count, 8U);
        check_equal(allocator.deallocation_count, 8U);
        check_equal(state.apply_count, 8U);
        tr_raft_service_destroy(service);
        check_equal(allocator.deallocation_count, 8U);
    }

    it("keeps transport queue saturation retryable without faulting Service")
    {
        const tr_raft_node_id_t voters[] = {1U, 2U, 3U};
        tr_raft_service_config_t config;
        tr_raft_service_t *service = NULL;
        tr_raft_service_status_t status;
        tr_raft_tick_t tick = {3U, 4U};
        service_test_state_t state;

        memset(&state, 0, sizeof(state));
        state.enqueue_result = SALTS_ENOSPC;
        service_configure(&config, &state, voters, 3U);
        check_equal(tr_raft_service_create(&config, &service), SALTS_OK);

        check_equal(tr_raft_service_tick(service, &tick), SALTS_OK);
        check_equal(state.message_count, 2U);
        check_equal(state.successful_message_count, 0U);
        check_equal(tr_raft_service_status(service, &status), SALTS_OK);
        check(!status.faulted);

        check_equal(tr_raft_service_tick(service, &tick), SALTS_OK);
        check_equal(state.message_count, 4U);
        check_equal(state.successful_message_count, 0U);
        check_equal(tr_raft_service_status(service, &status), SALTS_OK);
        check(!status.faulted);

        state.enqueue_result = SALTS_OK;
        check_equal(tr_raft_service_poll(service), SALTS_OK);
        check_equal(state.message_count, 6U);
        check_equal(state.successful_message_count, 2U);
        check_equal(tr_raft_service_status(service, &status), SALTS_OK);
        check(!status.faulted);
        check(!status.core.ready_outstanding);

        tr_raft_service_destroy(service);
    }

    it("keeps a healthy quorum live while one follower is capacity blocked")
    {
        const tr_raft_node_id_t voters[] = {1U, 2U, 3U};
        const char command[] = "set";
        tr_raft_service_config_t config;
        tr_raft_service_t *service = NULL;
        tr_raft_service_peer_delivery_status_t delivery;
        tr_raft_service_status_t status;
        tr_raft_read_state_t read_state;
        tr_raft_tick_t tick = {3U, 4U};
        tr_raft_proposal_t proposal;
        tr_raft_message_t response;
        service_test_state_t state;

        memset(&state, 0, sizeof(state));
        state.blocked_peer_id = 3U;
        service_configure(&config, &state, voters, 3U);
        check_equal(tr_raft_service_create(&config, &service), SALTS_OK);

        check_equal(tr_raft_service_tick(service, &tick), SALTS_OK);
        check(state.successful_by_peer[2U] >= 1U);
        check_equal(state.successful_by_peer[3U], 0U);
        check_equal(tr_raft_service_get_peer_delivery_status(
                        service, 3U, &delivery), SALTS_OK);
        check(delivery.paused);
        check_equal(delivery.staged_message_count, 1U);
        check(delivery.capacity_rejection_count >= 1U);

        memset(&response, 0, sizeof(response));
        response.type = TR_RAFT_MSG_PRE_VOTE_RESPONSE;
        response.from = 2U;
        response.to = 1U;
        response.campaign_term = 1U;
        response.granted = true;
        check_equal(tr_raft_service_step(service, &response), SALTS_OK);

        response.type = TR_RAFT_MSG_VOTE_RESPONSE;
        response.term = 1U;
        check_equal(tr_raft_service_step(service, &response), SALTS_OK);
        check_equal(tr_raft_service_status(service, &status), SALTS_OK);
        check_equal(status.core.role, TR_RAFT_LEADER);

        proposal.command_id = 77U;
        proposal.data = command;
        proposal.data_length = sizeof(command) - 1U;
        check_equal(tr_raft_service_propose(service, &proposal), SALTS_OK);

        memset(&response, 0, sizeof(response));
        response.type = TR_RAFT_MSG_APPEND_RESPONSE;
        response.from = 2U;
        response.to = 1U;
        response.term = 1U;
        response.granted = true;
        response.match_index = 1U;
        check_equal(tr_raft_service_step(service, &response), SALTS_OK);
        check_equal(tr_raft_service_status(service, &status), SALTS_OK);
        check_equal(status.core.commit_index, 1U);
        check_equal(status.core.applied_index, 1U);
        check_equal(state.apply_count, 1U);

        check_equal(tr_raft_service_read_index(service, 501U), SALTS_OK);
        memset(&response, 0, sizeof(response));
        response.type = TR_RAFT_MSG_READ_INDEX_RESPONSE;
        response.from = 2U;
        response.to = 1U;
        response.term = 1U;
        response.context_id = 501U;
        check_equal(tr_raft_service_step(service, &response), SALTS_OK);
        check_equal(tr_raft_service_take_read_state(service, &read_state),
                    SALTS_OK);
        check_equal(read_state.context_id, 501U);
        check_equal(read_state.index, 1U);

        check_equal(tr_raft_service_get_peer_delivery_status(
                        service, 3U, &delivery), SALTS_OK);
        check(delivery.paused);
        check_equal(delivery.staged_message_count, 1U);
        check(delivery.capacity_rejection_count >= 5U);

        state.blocked_peer_id = 0U;
        check_equal(tr_raft_service_poll(service), SALTS_OK);
        check_equal(tr_raft_service_get_peer_delivery_status(
                        service, 3U, &delivery), SALTS_OK);
        check_false(delivery.paused);
        check_equal(delivery.staged_message_count, 0U);
        check_equal(delivery.last_error, SALTS_OK);
        check(state.successful_by_peer[3U] >= 2U);

        tr_raft_service_destroy(service);
    }

    it("retries only the unsent suffix after partial transport admission")
    {
        const tr_raft_node_id_t voters[] = {1U, 2U, 3U};
        tr_raft_service_config_t config;
        tr_raft_service_t *service = NULL;
        tr_raft_service_status_t status;
        tr_raft_tick_t tick = {3U, 4U};
        service_test_state_t state;

        memset(&state, 0, sizeof(state));
        state.limit_successful_messages = true;
        state.max_successful_messages = 1U;
        service_configure(&config, &state, voters, 3U);
        check_equal(tr_raft_service_create(&config, &service), SALTS_OK);

        check_equal(tr_raft_service_tick(service, &tick), SALTS_OK);
        check_equal(state.message_count, 2U);
        check_equal(state.successful_message_count, 1U);
        check_equal(tr_raft_service_status(service, &status), SALTS_OK);
        check(!status.faulted);

        state.limit_successful_messages = false;
        check_equal(tr_raft_service_poll(service), SALTS_OK);
        check_equal(state.message_count, 3U);
        check_equal(state.successful_message_count, 2U);
        check_equal(tr_raft_service_status(service, &status), SALTS_OK);
        check(!status.faulted);

        tr_raft_service_destroy(service);
    }

    it("faults when a pending transport retry becomes a permanent error")
    {
        const tr_raft_node_id_t voters[] = {1U, 2U, 3U};
        tr_raft_service_config_t config;
        tr_raft_service_t *service = NULL;
        tr_raft_service_status_t status;
        tr_raft_tick_t tick = {3U, 4U};
        service_test_state_t state;

        memset(&state, 0, sizeof(state));
        state.enqueue_result = SALTS_ENOSPC;
        service_configure(&config, &state, voters, 3U);
        check_equal(tr_raft_service_create(&config, &service), SALTS_OK);
        check_equal(tr_raft_service_tick(service, &tick), SALTS_OK);

        state.enqueue_result = SALTS_EPIPE;
        check_equal(tr_raft_service_poll(service), SALTS_EPIPE);
        check_equal(tr_raft_service_status(service, &status), SALTS_OK);
        check(status.faulted);
        check_equal(status.cause, SALTS_EPIPE);

        tr_raft_service_destroy(service);
    }

    it("keeps snapshot transport saturation retryable")
    {
        const tr_raft_node_id_t voters[] = {1U, 2U};
        tr_raft_entry_t suffix;
        tr_raft_service_config_t config;
        tr_raft_service_t *service = NULL;
        tr_raft_service_status_t status;
        tr_raft_read_state_t read_state;
        tr_raft_message_t response;
        tr_raft_tick_t tick = {2U, 2U};
        service_test_state_t state;

        memset(&state, 0, sizeof(state));
        memset(&suffix, 0, sizeof(suffix));
        suffix.index = 3U;
        suffix.term = 2U;
        suffix.command_id = 1U;
        service_configure(&config, &state, voters, 2U);
        config.core.election_min_ticks = 2U;
        config.core.election_max_ticks = 3U;
        config.core.initial_election_timeout_ticks = 2U;
        config.core.initial_term = 2U;
        config.core.initial_last_log_index = 2U;
        config.core.initial_last_log_term = 1U;
        config.core.initial_log_entries = &suffix;
        config.core.initial_log_entry_count = 1U;
        config.core.initial_commit_index = 2U;
        config.core.initial_applied_index = 2U;
        check_equal(tr_raft_service_create(&config, &service), SALTS_OK);
        check_equal(tr_raft_service_tick(service, &tick), SALTS_OK);

        memset(&response, 0, sizeof(response));
        response.type = TR_RAFT_MSG_PRE_VOTE_RESPONSE;
        response.from = 2U;
        response.to = 1U;
        response.term = 2U;
        response.campaign_term = 3U;
        response.granted = true;
        check_equal(tr_raft_service_step(service, &response), SALTS_OK);
        response.type = TR_RAFT_MSG_VOTE_RESPONSE;
        response.term = 3U;
        check_equal(tr_raft_service_step(service, &response), SALTS_OK);

        state.snapshot_enqueue_result = SALTS_ENOSPC;
        memset(&response, 0, sizeof(response));
        response.type = TR_RAFT_MSG_HEARTBEAT_RESPONSE;
        response.from = 2U;
        response.to = 1U;
        response.term = 3U;
        response.reject_hint = 1U;
        check_equal(tr_raft_service_step(service, &response), SALTS_OK);
        check_equal(state.snapshot_request_count, 1U);
        check_equal(state.successful_snapshot_request_count, 0U);
        check_equal(tr_raft_service_status(service, &status), SALTS_OK);
        check(!status.faulted);

        check_equal(tr_raft_service_take_read_state(service, &read_state),
                    SALTS_ENOENT);
        state.snapshot_enqueue_result = SALTS_OK;
        check_equal(tr_raft_service_take_read_state(service, &read_state),
                    SALTS_ENOENT);
        check_equal(state.snapshot_request_count, 3U);
        check_equal(state.successful_snapshot_request_count, 1U);
        check_equal(tr_raft_service_status(service, &status), SALTS_OK);
        check(!status.faulted);

        tr_raft_service_destroy(service);
    }

    it("quiesces mutation until a reopened WAL storage is rebound")
    {
        const tr_raft_node_id_t voters[] = {1U};
        tr_raft_service_config_t config;
        tr_raft_service_t *service = NULL;
        tr_raft_service_status_t status;
        tr_raft_storage_t incomplete_storage;
        tr_raft_tick_t tick = {3U, 4U};
        tr_raft_proposal_t proposal;
        service_test_state_t state;

        memset(&state, 0, sizeof(state));
        memset(&incomplete_storage, 0, sizeof(incomplete_storage));
        memset(&proposal, 0, sizeof(proposal));
        service_configure(&config, &state, voters, 1U);
        check_equal(tr_raft_service_create(&config, &service), SALTS_OK);
        check_equal(tr_raft_service_prepare_backup(service), SALTS_OK);
        check_equal(tr_raft_service_status(service, &status), SALTS_OK);
        check(status.backup_prepared);
        check_equal(tr_raft_service_prepare_backup(service), SALTS_EBUSY);
        check_equal(tr_raft_service_tick(service, &tick), SALTS_EBUSY);
        check_equal(tr_raft_service_propose(service, &proposal), SALTS_EBUSY);
        check_equal(tr_raft_service_read_index(service, 1U), SALTS_EBUSY);
        check_equal(tr_raft_service_poll(service), SALTS_EBUSY);
        check_equal(tr_raft_service_trigger_snapshot(service), SALTS_EBUSY);
        check_equal(tr_raft_service_reload(service, &config.core), SALTS_EBUSY);
        check_equal(tr_raft_service_resume_backup(service, &incomplete_storage),
                    SALTS_EINVAL);
        check_equal(tr_raft_service_tick(service, &tick), SALTS_EBUSY);
        check_equal(tr_raft_service_resume_backup(service, &config.storage),
                    SALTS_OK);
        check_equal(tr_raft_service_status(service, &status), SALTS_OK);
        check(!status.backup_prepared);
        check_equal(tr_raft_service_tick(service, &tick), SALTS_OK);

        tr_raft_service_destroy(service);
    }

}
