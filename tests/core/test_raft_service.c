#include <turboraft/raft_service.h>

#include <tinytest.h>
#include <salts_error.h>

#include <string.h>

typedef struct service_test_state {
    size_t transaction_count;
    size_t commit_count;
    size_t apply_count;
    size_t message_count;
    int enqueue_result;
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
    return state->enqueue_result;
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
            check_equal(tr_raft_service_prepare_backup(service), SALTS_EBUSY);
            check_equal(tr_raft_service_take_read_state(service, &read_state),
                         SALTS_OK);
            check_equal(read_state.context_id, 21U);
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
