#include "../../src/control/raft_control_owner_bridge.h"

#include <turboraft/raft_service.h>

#include <CoroNet/turbo_coro_context.h>
#include <tinytest.h>
#include <turbo_error.h>
#include <turbo_thread.h>

#include <stdbool.h>
#include <string.h>

typedef struct bridge_test_state {
    coro_context_t *owner_context;
    coro_context_t *caller_context;
    tr_raft_service_t *service;
    tr_raft_service_owner_t *owner;
    turbo_thread_t owner_thread;
    int result;
    int overflow_result;
    int value;
    bool executed_on_owner;
} bridge_test_state_t;

typedef struct bridge_test_payload {
    bridge_test_state_t *state;
    int value;
} bridge_test_payload_t;

static int bridge_timeout(void *context, uint32_t *timeout)
{
    (void)context;
    *timeout = 100U;
    return TURBO_OK;
}

static int bridge_command(tr_raft_service_t *service,
                          const void *payload,
                          size_t payload_size)
{
    const bridge_test_payload_t *command =
        (const bridge_test_payload_t *)payload;

    if (service == NULL || command == NULL ||
        payload_size != sizeof(*command)) {
        return TURBO_EINVAL;
    }
    command->state->value = command->value;
    command->state->executed_on_owner =
        coro_context_current() == command->state->owner_context;
    return TURBO_OK;
}

static int bridge_create_service(bridge_test_state_t *state)
{
    static const tr_raft_node_id_t voters[] = {1U};
    tr_raft_service_config_t config;

    memset(&config, 0, sizeof(config));
    config.core.self_id = 1U;
    config.core.voters = voters;
    config.core.voter_count = 1U;
    config.core.heartbeat_ticks = 1U;
    config.core.election_min_ticks = 3U;
    config.core.election_max_ticks = 5U;
    config.core.initial_election_timeout_ticks = 3U;
    config.core.max_log_entries = 16U;
    return tr_raft_service_create(&config, &state->service);
}

static int bridge_create_owner(bridge_test_state_t *state)
{
    tr_raft_service_owner_config_t config;

    memset(&config, 0, sizeof(config));
    config.service = state->service;
    config.context = state->owner_context;
    config.tick_interval_ms = 10U;
    config.elapsed_ticks = 1U;
    config.max_pending_commands = 1U;
    config.max_command_bytes = 64U;
    config.next_election_timeout = bridge_timeout;
    return tr_raft_service_owner_create(&config, &state->owner);
}

static void bridge_owner_thread(void *context)
{
    bridge_test_state_t *state = (bridge_test_state_t *)context;

    (void)coro_context_run(state->owner_context, TURBO_RUN_DEFAULT);
}

static void bridge_caller(coro_t *coroutine, void *context)
{
    bridge_test_state_t *state = (bridge_test_state_t *)context;
    bridge_test_payload_t payload = {state, 42};

    (void)coroutine;
    state->result = tr_control_owner_bridge_execute(
        state->owner, bridge_command, &payload, sizeof(payload), 50U);
    (void)tr_raft_service_owner_stop(state->owner);
}

static void bridge_timeout_caller(coro_t *coroutine, void *context)
{
    bridge_test_state_t *state = (bridge_test_state_t *)context;
    bridge_test_payload_t payload = {state, 99};

    (void)coroutine;
    state->result = tr_control_owner_bridge_execute(
        state->owner, bridge_command, &payload, sizeof(payload), 5U);
    (void)tr_raft_service_owner_stop(state->owner);
}

static void bridge_shutdown_caller(coro_t *coroutine, void *context)
{
    bridge_test_state_t *state = (bridge_test_state_t *)context;
    bridge_test_payload_t payload = {state, 77};

    (void)coroutine;
    state->result = tr_control_owner_bridge_execute(
        state->owner, bridge_command, &payload, sizeof(payload), 1000U);
}

static void bridge_overflow_caller(coro_t *coroutine, void *context)
{
    bridge_test_state_t *state = (bridge_test_state_t *)context;
    bridge_test_payload_t payload = {state, 99};

    (void)coroutine;
    state->overflow_result = tr_control_owner_bridge_execute(
        state->owner, bridge_command, &payload, sizeof(payload), 100U);
}

static void bridge_caller_thread(void *context)
{
    bridge_test_state_t *state = (bridge_test_state_t *)context;

    (void)coro_context_run(state->caller_context, TURBO_RUN_DEFAULT);
}

static void bridge_cleanup(bridge_test_state_t *state)
{
    if (state->owner != NULL) {
        check_equal(tr_raft_service_owner_close(state->owner), TURBO_OK);
        state->owner = NULL;
    }
    coro_context_destroy(state->caller_context);
    coro_context_destroy(state->owner_context);
    tr_raft_service_destroy(state->service);
}

spec("control_owner_bridge") {
    it("executes a borrowed payload on a different owner context") {
        bridge_test_state_t state = {0};

        state.owner_context = coro_context_create(NULL);
        state.caller_context = coro_context_create(NULL);
        check_not_null(state.owner_context);
        check_not_null(state.caller_context);
        check_equal(bridge_create_service(&state), TURBO_OK);
        check_equal(bridge_create_owner(&state), TURBO_OK);
        check_equal(tr_raft_service_owner_start(state.owner), TURBO_OK);
        check_equal(turbo_thread_create(&state.owner_thread,
                                         bridge_owner_thread, &state), 0);
        check_equal(coro_context_spawn(state.caller_context,
                                        bridge_caller, &state), TURBO_OK);
        (void)coro_context_run(state.caller_context, TURBO_RUN_DEFAULT);
        check_equal(turbo_thread_join(&state.owner_thread), 0);
        turbo_thread_destroy(&state.owner_thread);
        check_equal(state.result, TURBO_OK);
        check_equal(state.value, 42);
        check_true(state.executed_on_owner);
        bridge_cleanup(&state);
    }

    it("cancels a queued command before returning timeout") {
        bridge_test_state_t state = {0};

        state.owner_context = coro_context_create(NULL);
        state.caller_context = coro_context_create(NULL);
        check_not_null(state.owner_context);
        check_not_null(state.caller_context);
        check_equal(bridge_create_service(&state), TURBO_OK);
        check_equal(bridge_create_owner(&state), TURBO_OK);
        check_equal(tr_raft_service_owner_start(state.owner), TURBO_OK);
        check_equal(coro_context_spawn(state.caller_context,
                                        bridge_timeout_caller, &state),
                     TURBO_OK);
        (void)coro_context_run(state.caller_context, TURBO_RUN_DEFAULT);
        check_equal(state.result, TURBO_ETIMEDOUT);
        check_equal(turbo_thread_create(&state.owner_thread,
                                         bridge_owner_thread, &state), 0);
        check_equal(turbo_thread_join(&state.owner_thread), 0);
        turbo_thread_destroy(&state.owner_thread);
        check_equal(state.value, 0);
        check_false(state.executed_on_owner);
        bridge_cleanup(&state);
    }

    it("drains an accepted request while shutdown rejects new work") {
        enum { BRIDGE_PENDING_WAIT_SPINS = 100000 };
        bridge_test_state_t state = {0};
        tr_raft_service_owner_status_t status;
        bridge_test_payload_t rejected_payload = {&state, 88};
        turbo_thread_t drain_thread;
        size_t spin;

        state.owner_context = coro_context_create(NULL);
        state.caller_context = coro_context_create(NULL);
        check_not_null(state.owner_context);
        check_not_null(state.caller_context);
        check_equal(bridge_create_service(&state), TURBO_OK);
        check_equal(bridge_create_owner(&state), TURBO_OK);
        check_equal(tr_raft_service_owner_start(state.owner), TURBO_OK);
        check_equal(coro_context_spawn(state.caller_context,
                                        bridge_shutdown_caller, &state),
                     TURBO_OK);
        check_equal(turbo_thread_create(&state.owner_thread,
                                         bridge_caller_thread, &state), 0);
        for (spin = 0U; spin < BRIDGE_PENDING_WAIT_SPINS; ++spin) {
            check_equal(tr_raft_service_owner_status(state.owner, &status),
                         TURBO_OK);
            if (status.pending_commands == 1U) {
                break;
            }
            turbo_thread_yield();
        }
        check(spin < BRIDGE_PENDING_WAIT_SPINS);
        check_equal(tr_raft_service_owner_stop(state.owner), TURBO_OK);
        check_equal(tr_raft_service_owner_submit(
                         state.owner, bridge_command, &rejected_payload,
                         sizeof(rejected_payload), NULL, NULL),
                     TURBO_ECANCELED);
        check_equal(turbo_thread_create(&drain_thread, bridge_owner_thread,
                                         &state), 0);
        check_equal(turbo_thread_join(&drain_thread), 0);
        turbo_thread_destroy(&drain_thread);
        check_equal(turbo_thread_join(&state.owner_thread), 0);
        turbo_thread_destroy(&state.owner_thread);
        check_equal(state.result, TURBO_OK);
        check_equal(state.value, 77);
        check_true(state.executed_on_owner);
        check_equal(tr_raft_service_owner_status(state.owner, &status),
                     TURBO_OK);
        check_false(status.running);
        check_equal(status.pending_commands, 0U);
        bridge_cleanup(&state);
    }

    it("rejects a bridge request when the owner queue is full") {
        enum { BRIDGE_PENDING_WAIT_SPINS = 100000 };
        bridge_test_state_t state = {0};
        tr_raft_service_owner_status_t status;
        coro_context_t *overflow_context;
        turbo_thread_t drain_thread;
        size_t spin;

        state.owner_context = coro_context_create(NULL);
        state.caller_context = coro_context_create(NULL);
        overflow_context = coro_context_create(NULL);
        check_not_null(state.owner_context);
        check_not_null(state.caller_context);
        check_not_null(overflow_context);
        check_equal(bridge_create_service(&state), TURBO_OK);
        check_equal(bridge_create_owner(&state), TURBO_OK);
        check_equal(tr_raft_service_owner_start(state.owner), TURBO_OK);
        check_equal(coro_context_spawn(state.caller_context,
                                        bridge_caller, &state), TURBO_OK);
        check_equal(turbo_thread_create(&state.owner_thread,
                                         bridge_caller_thread, &state), 0);
        for (spin = 0U; spin < BRIDGE_PENDING_WAIT_SPINS; ++spin) {
            check_equal(tr_raft_service_owner_status(state.owner, &status),
                         TURBO_OK);
            if (status.pending_commands == 1U) {
                break;
            }
            turbo_thread_yield();
        }
        check(spin < BRIDGE_PENDING_WAIT_SPINS);
        check_equal(coro_context_spawn(overflow_context,
                                        bridge_overflow_caller, &state),
                     TURBO_OK);
        (void)coro_context_run(overflow_context, TURBO_RUN_DEFAULT);
        check_equal(state.overflow_result, TURBO_EBUSY);
        check_equal(turbo_thread_create(&drain_thread, bridge_owner_thread,
                                         &state), 0);
        check_equal(turbo_thread_join(&state.owner_thread), 0);
        turbo_thread_destroy(&state.owner_thread);
        check_equal(turbo_thread_join(&drain_thread), 0);
        turbo_thread_destroy(&drain_thread);
        check_equal(state.result, TURBO_OK);
        check_equal(state.value, 42);
        check_equal(tr_raft_service_owner_status(state.owner, &status),
                     TURBO_OK);
        check_equal(status.pending_commands, 0U);
        coro_context_destroy(overflow_context);
        bridge_cleanup(&state);
    }
}
