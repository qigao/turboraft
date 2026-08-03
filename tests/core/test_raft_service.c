#include <turboraft/raft_service.h>

#ifdef TURBORAFT_HAS_SERVICE_OWNER
#include <turboraft/raft_service_owner.h>
#include <turbo_coro_context.h>
#include <turbo_coro_object_pool.h>
#endif

#include <tinytest.h>
#include <turbo_error.h>

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
    return TURBO_OK;
}

static int service_storage_hard_state(
    void *context,
    tr_raft_term_t term,
    tr_raft_node_id_t voted_for)
{
    (void) context;
    (void) term;
    (void) voted_for;
    return TURBO_OK;
}

static int service_storage_truncate(void *context, tr_raft_index_t index)
{
    (void) context;
    (void) index;
    return TURBO_OK;
}

static int service_storage_append(
    void *context,
    const tr_raft_entry_t *entries,
    size_t entry_count)
{
    (void) context;
    return entries != NULL && entry_count != 0U ? TURBO_OK : TURBO_EINVAL;
}

static int service_storage_commit_index(
    void *context,
    tr_raft_index_t commit_index)
{
    (void) context;
    (void) commit_index;
    return TURBO_OK;
}

static int service_storage_commit(void *context)
{
    ++((service_test_state_t *) context)->commit_count;
    return TURBO_OK;
}

static int service_storage_rollback(void *context)
{
    (void) context;
    return TURBO_OK;
}

static int service_transport_enqueue(
    void *context,
    const tr_raft_message_t *message)
{
    service_test_state_t *state = (service_test_state_t *) context;

    if (message == NULL) {
        return TURBO_EINVAL;
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
        return TURBO_EINVAL;
    }
    state->apply_count += entry_count;
    return TURBO_OK;
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

#ifdef TURBORAFT_HAS_SERVICE_OWNER
typedef struct service_owner_test_state {
    tr_raft_service_owner_t *owner;
    coro_context_t *context;
    int submit_result;
    int overflow_result;
    int completion_result;
    bool completion_on_owner;
} service_owner_test_state_t;

static int service_owner_timeout(void *context, uint32_t *election_timeout)
{
    (void)context;
    *election_timeout = 3U;
    return TURBO_OK;
}

static int service_owner_command(tr_raft_service_t *service,
                                 const void *payload,
                                 size_t payload_size)
{
    static const char expected[] = "owner-command";
    tr_raft_service_status_t status;

    if (payload_size != sizeof(expected) ||
        memcmp(payload, expected, sizeof(expected)) != 0) {
        return TURBO_EINVAL;
    }
    if (tr_raft_service_status(service, &status) != TURBO_OK) {
        return TURBO_EPROTO;
    }
    return status.core.role == TR_RAFT_LEADER ? TURBO_OK : TURBO_EPROTO;
}

static void service_owner_complete(void *context, int result)
{
    service_owner_test_state_t *state =
        (service_owner_test_state_t *)context;

    state->completion_result = result;
    state->completion_on_owner = coro_context_current() == state->context;
    (void)tr_raft_service_owner_stop(state->owner);
}

static void service_owner_driver(coro_t *coroutine, void *context)
{
    service_owner_test_state_t *state =
        (service_owner_test_state_t *)context;
    char payload[] = "owner-command";

    (void)coroutine;
    (void)coro_sleep(state->context, 64U);
    state->submit_result = tr_raft_service_owner_submit(
        state->owner, service_owner_command, payload, sizeof(payload),
        service_owner_complete, state);
    payload[0] = 'X';
    state->overflow_result = tr_raft_service_owner_submit(
        state->owner, service_owner_command, payload, sizeof(payload), NULL,
        NULL);
}
#endif

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
        check_int_eq(tr_raft_service_create(&config, &service), TURBO_OK);
        check_int_eq(tr_raft_service_tick(service, &tick), TURBO_OK);
        check_int_eq(tr_raft_service_status(service, &status), TURBO_OK);
        check_int_eq(status.core.role, TR_RAFT_LEADER);

        proposal.command_id = 1U;
        proposal.data = command;
        proposal.data_length = sizeof(command) - 1U;
        check_int_eq(tr_raft_service_propose(service, &proposal), TURBO_OK);
        check_size_eq(state.apply_count, 1U);
        check(state.commit_count >= 2U);
        check_int_eq(tr_raft_service_status(service, &status), TURBO_OK);
        check_long_eq(status.core.commit_index, 1U);
        check_long_eq(status.core.applied_index, 1U);
        {
            tr_raft_read_state_t read_state;

            check_int_eq(tr_raft_service_read_index(service, 21U), TURBO_OK);
            check_int_eq(tr_raft_service_take_read_state(service, &read_state),
                         TURBO_OK);
            check_long_eq(read_state.context_id, 21U);
            check_long_eq(read_state.index, 1U);
            check_int_eq(tr_raft_service_take_read_state(service, &read_state),
                         TURBO_ENOENT);
        }

        config.core.initial_term = 2U;
        config.core.initial_last_log_index = 5U;
        config.core.initial_last_log_term = 2U;
        config.core.initial_commit_index = 5U;
        config.core.initial_applied_index = 5U;
        check_int_eq(tr_raft_service_reload(service, &config.core), TURBO_OK);
        check_int_eq(tr_raft_service_status(service, &status), TURBO_OK);
        check(!status.faulted);
        check_long_eq(status.core.last_log_index, 5U);
        check_long_eq(status.core.applied_index, 5U);

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
        state.enqueue_result = TURBO_EPIPE;
        service_configure(&config, &state, voters, 3U);
        check_int_eq(tr_raft_service_create(&config, &service), TURBO_OK);
        check_int_eq(tr_raft_service_tick(service, &tick), TURBO_EPIPE);
        check_int_eq(tr_raft_service_status(service, &status), TURBO_OK);
        check(status.faulted);
        check_int_eq(status.cause, TURBO_EPIPE);
        check_int_eq(tr_raft_service_tick(service, &tick), TURBO_EPROTO);

        tr_raft_service_destroy(service);
    }

#ifdef TURBORAFT_HAS_SERVICE_OWNER
    it("serializes bounded commands with automatic ticks on the owner context")
    {
        const tr_raft_node_id_t voters[] = {1U};
        tr_raft_service_config_t config;
        tr_raft_service_t *service = NULL;
        tr_raft_service_owner_config_t owner_config;
        tr_raft_service_owner_t *owner = NULL;
        tr_raft_service_owner_status_t owner_status;
        service_test_state_t service_state;
        service_owner_test_state_t owner_state;
        coro_object_pool_config_t coro_config;
        coro_context_t *context;

        memset(&service_state, 0, sizeof(service_state));
        memset(&owner_state, 0, sizeof(owner_state));
        memset(&owner_config, 0, sizeof(owner_config));
        memset(&coro_config, 0, sizeof(coro_config));
        coro_config.initial_capacity = 4U;
        context = coro_context_create_ex(NULL, &coro_config);
        check_not_null(context);
        service_configure(&config, &service_state, voters, 1U);
        check_int_eq(tr_raft_service_create(&config, &service), TURBO_OK);
        owner_config.service = service;
        owner_config.context = context;
        owner_config.tick_interval_ms = 1U;
        owner_config.elapsed_ticks = 1U;
        owner_config.max_pending_commands = 1U;
        owner_config.max_command_bytes = 64U;
        owner_config.next_election_timeout = service_owner_timeout;
        check_int_eq(tr_raft_service_owner_create(&owner_config, &owner),
                     TURBO_OK);
        owner_state.owner = owner;
        owner_state.context = context;
        owner_state.submit_result = TURBO_EPROTO;
        owner_state.overflow_result = TURBO_EPROTO;
        owner_state.completion_result = TURBO_EPROTO;
        check_int_eq(tr_raft_service_owner_start(owner), TURBO_OK);
        check_int_eq(tr_raft_service_owner_execute_current(
                         owner, service_owner_command, "owner-command",
                         sizeof("owner-command")),
                     TURBO_EPROTO);
        check_int_eq(coro_context_spawn(context, service_owner_driver,
                                        &owner_state),
                     TURBO_OK);
        (void)coro_context_run(context, TURBO_RUN_DEFAULT);

        check_int_eq(owner_state.submit_result, TURBO_OK);
        check_int_eq(owner_state.overflow_result, TURBO_EBUSY);
        check_int_eq(owner_state.completion_result, TURBO_OK);
        check(owner_state.completion_on_owner);
        check_int_eq(tr_raft_service_owner_status(owner, &owner_status),
                     TURBO_OK);
        check(!owner_status.running);
        check(owner_status.stopping);
        check(!owner_status.faulted);
        check_size_eq(owner_status.pending_commands, 0U);
        check_long_eq(owner_status.completed_commands, 1U);
        check(owner_status.tick_count >= 3U);
        check_int_eq(tr_raft_service_owner_close(owner), TURBO_OK);
        tr_raft_service_destroy(service);
        coro_context_destroy(context);
    }
#endif
}
