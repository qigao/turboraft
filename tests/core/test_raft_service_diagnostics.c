#include <turboraft/raft_service.h>

#include <tinytest.h>
#include <salts_error.h>

#include <string.h>

typedef struct diagnostic_state {
    size_t snapshot_create_count;
    size_t snapshot_store_count;
    tr_raft_index_t stored_index;
} diagnostic_state_t;

static int diagnostic_ok(void *context)
{
    (void) context;
    return SALTS_OK;
}

static int diagnostic_hard_state(void *context,
                                 tr_raft_term_t term,
                                 tr_raft_node_id_t vote)
{
    (void) context;
    (void) term;
    (void) vote;
    return SALTS_OK;
}

static int diagnostic_index(void *context, tr_raft_index_t index)
{
    (void) context;
    (void) index;
    return SALTS_OK;
}

static int diagnostic_append(void *context,
                             const tr_raft_entry_t *entries,
                             size_t count)
{
    (void) context;
    return entries != NULL && count != 0U ? SALTS_OK : SALTS_EINVAL;
}

static int diagnostic_apply(void *context,
                            const tr_raft_entry_t *entries,
                            size_t count)
{
    (void) context;
    return entries != NULL && count != 0U ? SALTS_OK : SALTS_EINVAL;
}

static int diagnostic_enqueue(void *context,
                              const tr_raft_message_t *message)
{
    (void) context;
    return message != NULL ? SALTS_OK : SALTS_EINVAL;
}

static int diagnostic_snapshot_create(void *context,
                                      tr_raft_index_t index,
                                      uint8_t *output,
                                      size_t capacity,
                                      size_t *out_size)
{
    diagnostic_state_t *state = (diagnostic_state_t *) context;

    if (state == NULL || index == 0U || output == NULL || capacity < 1U ||
        out_size == NULL) {
        return SALTS_EINVAL;
    }
    state->snapshot_create_count++;
    output[0] = 0x5aU;
    *out_size = 1U;
    return SALTS_OK;
}

static int diagnostic_snapshot_store(void *context,
                                     tr_raft_index_t index,
                                     tr_raft_term_t term,
                                     const tr_raft_conf_t *configuration,
                                     const uint8_t *data,
                                     size_t size)
{
    diagnostic_state_t *state = (diagnostic_state_t *) context;

    if (state == NULL || index == 0U || term == 0U ||
        configuration == NULL || data == NULL || size != 1U ||
        data[0] != 0x5aU) {
        return SALTS_EINVAL;
    }
    state->snapshot_store_count++;
    state->stored_index = index;
    return SALTS_OK;
}

static void diagnostic_configure(tr_raft_service_config_t *config,
                                 diagnostic_state_t *state)
{
    static const tr_raft_node_id_t voters[] = {1U};

    memset(config, 0, sizeof(*config));
    config->core.self_id = 1U;
    config->core.voters = voters;
    config->core.voter_count = 1U;
    config->core.heartbeat_ticks = 1U;
    config->core.election_min_ticks = 3U;
    config->core.election_max_ticks = 5U;
    config->core.initial_election_timeout_ticks = 3U;
    config->core.max_log_entries = 16U;
    config->storage.begin = diagnostic_ok;
    config->storage.write_hard_state = diagnostic_hard_state;
    config->storage.truncate_log = diagnostic_index;
    config->storage.append_log = diagnostic_append;
    config->storage.write_commit_index = diagnostic_index;
    config->storage.commit = diagnostic_ok;
    config->storage.rollback = diagnostic_ok;
    config->transport.enqueue = diagnostic_enqueue;
    config->state_machine.apply_batch = diagnostic_apply;
    config->snapshot_policy.applied_entry_threshold = 16U;
    config->snapshot_policy.max_snapshot_bytes = 64U;
    config->snapshot_policy.create = diagnostic_snapshot_create;
    config->snapshot_policy.create_context = state;
    config->snapshot_policy.store = diagnostic_snapshot_store;
    config->snapshot_policy.store_context = state;
}

spec("raft service diagnostics and manual snapshots")
{
    it("copies membership and progress and forces one durable snapshot")
    {
        diagnostic_state_t state;
        tr_raft_service_config_t config;
        tr_raft_service_t *service = NULL;
        tr_raft_conf_t configuration;
        tr_raft_progress_view_t progress;
        tr_raft_service_status_t status;
        tr_raft_tick_t tick = {3U, 4U};
        tr_raft_proposal_t proposal;
        const uint8_t command[] = {0x31U};

        memset(&state, 0, sizeof(state));
        diagnostic_configure(&config, &state);
        check_equal(tr_raft_service_create(&config, &service), SALTS_OK);
        check_equal(tr_raft_service_configuration(service, &configuration),
                     SALTS_OK);
        check_equal(configuration.member_count, 1U);
        check_equal(configuration.members[0].node_id, 1U);
        check_equal(configuration.members[0].roles,
                     TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER);
        check_equal(tr_raft_service_progress(service, &progress), SALTS_OK);
        check_equal(progress.peer_count, 1U);
        check_equal(progress.peers[0].node_id, 1U);
        check(progress.peers[0].recent_active);
        check_equal(tr_raft_service_trigger_snapshot(service),
                     SALTS_ENOENT);

        check_equal(tr_raft_service_tick(service, &tick), SALTS_OK);
        proposal.command_id = 11U;
        proposal.data = command;
        proposal.data_length = sizeof(command);
        check_equal(tr_raft_service_propose(service, &proposal), SALTS_OK);
        check_equal(tr_raft_service_trigger_snapshot(service), SALTS_OK);
        check_equal(state.snapshot_create_count, 1U);
        check_equal(state.snapshot_store_count, 1U);
        check_equal(state.stored_index, 1U);
        check_equal(tr_raft_service_status(service, &status), SALTS_OK);
        check_equal(status.core.log_base_index, 1U);
        check_equal(tr_raft_service_trigger_snapshot(service),
                     SALTS_ENOENT);

        tr_raft_service_destroy(service);
    }

    it("rejects manual snapshots when policy is disabled")
    {
        diagnostic_state_t state;
        tr_raft_service_config_t config;
        tr_raft_service_t *service = NULL;

        memset(&state, 0, sizeof(state));
        diagnostic_configure(&config, &state);
        memset(&config.snapshot_policy, 0, sizeof(config.snapshot_policy));
        check_equal(tr_raft_service_create(&config, &service), SALTS_OK);
        check_equal(tr_raft_service_trigger_snapshot(service),
                     SALTS_EPROTONOSUPPORT);
        tr_raft_service_destroy(service);
    }
}
