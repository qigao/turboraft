#include <turboraft/raft_service.h>

#include <tinytest.h>
#include <salts_error.h>

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
    bool block_peer;
    tr_raft_node_id_t blocked_peer_id;
    size_t peer_message_count[4];
    size_t peer_successful_message_count[4];
    size_t snapshot_request_count;
    size_t successful_snapshot_request_count;
    int snapshot_enqueue_result;
} service_test_state_t;

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
    if (message->to < 4U) {
        ++state->peer_message_count[message->to];
    }
    if (state->block_peer && message->to == state->blocked_peer_id) {
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
    if (message->to < 4U) {
        ++state->peer_successful_message_count[message->to];
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

    it("isolates one saturated follower while healthy quorum commits and reads")
    {
        const tr_raft_node_id_t voters[] = {1U, 2U, 3U};
        static const char first_payload[] = "one";
        static const char second_payload[] = "two";
        tr_raft_service_config_t config;
        tr_raft_service_t *service = NULL;
        tr_raft_service_transport_status_t transport_status;
        tr_raft_service_status_t status;
        tr_raft_read_state_t read_state;
        tr_raft_tick_t tick = {3U, 4U};
        tr_raft_message_t response;
        tr_raft_proposal_t proposal;
        service_test_state_t state;
        size_t index;
        bool saw_paused_three = false;

        memset(&state, 0, sizeof(state));
        service_configure(&config, &state, voters, 3U);
        check_equal(tr_raft_service_create(&config, &service), SALTS_OK);

        check_equal(tr_raft_service_tick(service, &tick), SALTS_OK);
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

        state.block_peer = true;
        state.blocked_peer_id = 3U;
        memset(&proposal, 0, sizeof(proposal));
        proposal.command_id = 1U;
        proposal.data = first_payload;
        proposal.data_length = sizeof(first_payload) - 1U;
        check_equal(tr_raft_service_propose(service, &proposal), SALTS_OK);

        check_equal(tr_raft_service_transport_status(
                        service, &transport_status),
                    SALTS_OK);
        check_equal(transport_status.paused_peer_count, 1U);
        check_equal(transport_status.total_staged_message_count, 1U);
        for (index = 0U; index < transport_status.peer_count; ++index) {
            if (transport_status.peers[index].node_id == 3U) {
                check(transport_status.peers[index].paused);
                check_equal(
                    transport_status.peers[index].staged_message_count, 1U);
                saw_paused_three = true;
            }
        }
        check(saw_paused_three);

        memset(&response, 0, sizeof(response));
        response.type = TR_RAFT_MSG_APPEND_RESPONSE;
        response.from = 2U;
        response.to = 1U;
        response.term = 1U;
        response.granted = true;
        response.previous_log_index = 0U;
        response.match_index = 1U;
        check_equal(tr_raft_service_step(service, &response), SALTS_OK);
        check_equal(state.apply_count, 1U);

        proposal.command_id = 2U;
        proposal.data = second_payload;
        proposal.data_length = sizeof(second_payload) - 1U;
        check_equal(tr_raft_service_propose(service, &proposal), SALTS_OK);
        check_equal(tr_raft_service_transport_status(
                        service, &transport_status),
                    SALTS_OK);
        check_equal(transport_status.paused_peer_count, 1U);
        check_equal(transport_status.total_staged_message_count, 1U);

        response.previous_log_index = 1U;
        response.match_index = 2U;
        check_equal(tr_raft_service_step(service, &response), SALTS_OK);
        check_equal(state.apply_count, 2U);

        check_equal(tr_raft_service_read_index(service, 77U), SALTS_OK);
        memset(&response, 0, sizeof(response));
        response.type = TR_RAFT_MSG_READ_INDEX_RESPONSE;
        response.from = 2U;
        response.to = 1U;
        response.term = 1U;
        response.context_id = 77U;
        check_equal(tr_raft_service_step(service, &response), SALTS_OK);
        check_equal(tr_raft_service_take_read_state(service, &read_state),
                    SALTS_OK);
        check_equal(read_state.context_id, 77U);
        check_equal(read_state.index, 2U);
        check_equal(tr_raft_service_status(service, &status), SALTS_OK);
        check(!status.faulted);

        state.block_peer = false;
        check_equal(tr_raft_service_poll(service), SALTS_OK);
        check_equal(state.peer_successful_message_count[3U], 1U);
        check_equal(tr_raft_service_transport_status(
                        service, &transport_status),
                    SALTS_OK);
        check_equal(transport_status.paused_peer_count, 0U);
        check_equal(transport_status.total_staged_message_count, 0U);

        memset(&response, 0, sizeof(response));
        response.type = TR_RAFT_MSG_APPEND_RESPONSE;
        response.from = 3U;
        response.to = 1U;
        response.term = 1U;
        response.granted = true;
        response.previous_log_index = 0U;
        response.match_index = 1U;
        check_equal(tr_raft_service_step(service, &response), SALTS_OK);
        check_equal(state.peer_successful_message_count[3U], 2U);

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
