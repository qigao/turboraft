#include <turboraft/raft_cflow_state_machine.h>

#include <cflow/cflow.h>
#include <salts/thread.h>
#include <salts_error.h>
#include <tinytest.h>

#include <stdatomic.h>
#include <string.h>

enum matrix_id {
    MATRIX_ROOT = 1,
    MATRIX_INITIAL = 2,
    MATRIX_ACTIVE = 3,
    MATRIX_EVENT = 10,
    MATRIX_GUARD = 20,
    MATRIX_ACTION = 30
};

typedef struct matrix_probe {
    bool fail_guard;
    bool fail_action;
    bool wrong_type;
    size_t guard_calls;
    size_t action_calls;
} matrix_probe_t;

typedef struct matrix_blocker {
    atomic_bool entered;
    atomic_bool release;
} matrix_blocker_t;

typedef struct matrix_fixture {
    matrix_probe_t probe;
    tr_raft_cflow_state_machine_t *adapter;
    cflow_statechart statechart;
    cflow_executor executor;
    cflow_statechart_instance instance;
    tr_raft_entry_state_machine_v1_t state_machine;
    bool statechart_built;
    bool executor_initialized;
    bool instance_initialized;
} matrix_fixture_t;

static const cflow_statechart_state matrix_states[] = {
    {MATRIX_ROOT, 0U, CFLOW_STATECHART_COMPOUND, 0U},
    {MATRIX_INITIAL, MATRIX_ROOT, CFLOW_STATECHART_INITIAL, 1U},
    {MATRIX_ACTIVE, MATRIX_ROOT, CFLOW_STATECHART_ATOMIC, 2U}
};

static const cflow_event_type matrix_events[] = {
    {MATRIX_EVENT, &cmeta_type_int}
};

static const cflow_statechart_guard matrix_guards[] = {{
    MATRIX_GUARD, &cmeta_type_int, CMETA_EFFECT_MAY_FAIL,
    CMETA_PROP_DETERMINISTIC | CMETA_PROP_NO_ALIAS
}};

static const cflow_statechart_executable matrix_actions_decl[] = {{
    MATRIX_ACTION, &cmeta_type_int, CMETA_EFFECT_MAY_FAIL,
    CMETA_PROP_DETERMINISTIC | CMETA_PROP_NO_ALIAS
}};

static const cflow_statechart_transition matrix_transitions[] = {
    {1U, MATRIX_INITIAL, CFLOW_STATECHART_TRIGGER_EVENTLESS, 0U,
     0U, 0U, MATRIX_ACTIVE, CFLOW_STATECHART_TRANSITION_EXTERNAL,
     0U, 0U},
    {2U, MATRIX_ACTIVE, CFLOW_STATECHART_TRIGGER_EVENT, MATRIX_EVENT,
     0U, MATRIX_GUARD, MATRIX_ACTIVE,
     CFLOW_STATECHART_TRANSITION_EXTERNAL, 0U, 1U}
};

static const cflow_statechart_transition_action matrix_transition_actions[] = {
    {2U, MATRIX_ACTION, 0U}
};

static const cflow_statechart_definition matrix_definition = {
    &cmeta_type_int,
    matrix_states, 3U,
    matrix_events, 1U,
    matrix_guards, 1U,
    matrix_actions_decl, 1U,
    matrix_transitions, 2U,
    NULL, 0U,
    matrix_transition_actions, 1U
};

static bool matrix_guard(void *user,
                         const void *state,
                         const cflow_event_view *event,
                         bool *out_enabled,
                         const char **out_error)
{
    matrix_probe_t *probe = (matrix_probe_t *) user;

    if (probe == NULL || state == NULL || event == NULL ||
        out_enabled == NULL || out_error == NULL) {
        return false;
    }
    ++probe->guard_calls;
    if (probe->fail_guard) {
        *out_error = "deliberate Raft CFlow guard failure";
        return false;
    }
    *out_enabled = true;
    *out_error = NULL;
    return true;
}

static bool matrix_action(void *user,
                          cflow_statechart_action_phase phase,
                          cflow_machine_state_id owner,
                          const void *state,
                          const cflow_event_view *event,
                          void *out_state,
                          cflow_statechart_raise_fn raise_internal,
                          void *raise_user,
                          const char **out_error)
{
    matrix_probe_t *probe = (matrix_probe_t *) user;

    (void) owner;
    (void) raise_internal;
    (void) raise_user;
    if (probe == NULL ||
        phase != CFLOW_STATECHART_ACTION_TRANSITION ||
        state == NULL || event == NULL || event->payload == NULL ||
        out_state == NULL || out_error == NULL) {
        return false;
    }
    ++probe->action_calls;
    if (probe->fail_action) {
        *out_error = "deliberate Raft CFlow action failure";
        return false;
    }
    *(int *) out_state =
        *(const int *) state + *(const int *) event->payload;
    *out_error = NULL;
    return true;
}

static int matrix_decode(void *context,
                         const tr_raft_entry_t *entry,
                         cflow_event_view *out_event)
{
    matrix_probe_t *probe = (matrix_probe_t *) context;

    if (probe == NULL || entry == NULL || out_event == NULL ||
        entry->data_length != sizeof(int)) {
        return SALTS_EINVAL;
    }
    memset(out_event, 0, sizeof(*out_event));
    out_event->id = MATRIX_EVENT;
    out_event->payload_type =
        probe->wrong_type ? &cmeta_type_bool : &cmeta_type_int;
    out_event->payload = entry->data;
    return SALTS_OK;
}

static void matrix_block_executor(void *user)
{
    matrix_blocker_t *blocker = (matrix_blocker_t *) user;

    atomic_store_explicit(&blocker->entered, true, memory_order_release);
    while (!atomic_load_explicit(
        &blocker->release, memory_order_acquire)) {
        salts_thread_yield();
    }
}

static tr_raft_entry_t matrix_entry(int value)
{
    tr_raft_entry_t entry;

    memset(&entry, 0, sizeof(entry));
    entry.index = 1U;
    entry.term = 1U;
    entry.command_id = 101U;
    entry.data_length = sizeof(value);
    memcpy(entry.data, &value, sizeof(value));
    return entry;
}

static int matrix_create_core(const tr_raft_entry_t *entry,
                              tr_raft_core_t **out_core)
{
    static const tr_raft_node_id_t voters[] = {1U};
    tr_raft_core_config_t config;

    memset(&config, 0, sizeof(config));
    config.self_id = 1U;
    config.voters = voters;
    config.voter_count = 1U;
    config.heartbeat_ticks = 2U;
    config.election_min_ticks = 5U;
    config.election_max_ticks = 10U;
    config.initial_election_timeout_ticks = 5U;
    config.initial_term = 1U;
    config.initial_log_entries = entry;
    config.initial_log_entry_count = 1U;
    config.initial_commit_index = 1U;
    config.max_log_entries = 4U;
    return tr_raft_core_create(&config, out_core);
}

static bool matrix_fixture_init(matrix_fixture_t *fixture)
{
    tr_raft_cflow_state_machine_config_v1_t adapter_config;
    cflow_statechart_instance_hooks hooks;
    cflow_statechart_guard_binding guard_binding;
    cflow_statechart_executable_binding action_binding;
    cflow_statechart_instance_config instance_config;
    void *hook_user = NULL;
    int initial_state = 0;

    if (fixture == NULL) {
        return false;
    }
    memset(fixture, 0, sizeof(*fixture));

    memset(&adapter_config, 0, sizeof(adapter_config));
    adapter_config.abi_version =
        TR_RAFT_CFLOW_STATE_MACHINE_CONFIG_ABI_V1;
    adapter_config.struct_size = sizeof(adapter_config);
    adapter_config.decode_entry = matrix_decode;
    adapter_config.decode_context = &fixture->probe;
    if (tr_raft_cflow_state_machine_create(
            &adapter_config, &fixture->adapter) != SALTS_OK ||
        tr_raft_cflow_state_machine_hooks(
            fixture->adapter, &hooks, &hook_user) != SALTS_OK) {
        return false;
    }

    if (cflow_statechart_build(
            &fixture->statechart, &matrix_definition) !=
        CFLOW_STATECHART_OK) {
        return false;
    }
    fixture->statechart_built = true;
    if (!cflow_executor_serial_init_with_capacity(
            &fixture->executor, 4U)) {
        return false;
    }
    fixture->executor_initialized = true;

    memset(&guard_binding, 0, sizeof(guard_binding));
    guard_binding.id = MATRIX_GUARD;
    guard_binding.fn = matrix_guard;
    guard_binding.user = &fixture->probe;
    memset(&action_binding, 0, sizeof(action_binding));
    action_binding.id = MATRIX_ACTION;
    action_binding.fn = matrix_action;
    action_binding.user = &fixture->probe;

    memset(&instance_config, 0, sizeof(instance_config));
    instance_config.statechart = &fixture->statechart;
    instance_config.initial_state = &initial_state;
    instance_config.guards = &guard_binding;
    instance_config.guard_count = 1U;
    instance_config.executables = &action_binding;
    instance_config.executable_count = 1U;
    instance_config.external_event_capacity = 1U;
    instance_config.internal_event_capacity = 1U;
    instance_config.completion_capacity = 1U;
    instance_config.microstep_limit = 8U;
    instance_config.executor = &fixture->executor;
    instance_config.hooks = &hooks;
    instance_config.hook_user = hook_user;
    if (cflow_statechart_instance_init(
            &fixture->instance, &instance_config) !=
        CFLOW_STATECHART_INSTANCE_OK) {
        return false;
    }
    fixture->instance_initialized = true;
    if (tr_raft_cflow_state_machine_bind(
            fixture->adapter, &fixture->instance) != SALTS_OK ||
        tr_raft_cflow_state_machine_get_spi(
            fixture->adapter, &fixture->state_machine) != SALTS_OK ||
        !cflow_executor_wait_idle(&fixture->executor)) {
        return false;
    }
    return true;
}

static bool matrix_fixture_cleanup(matrix_fixture_t *fixture)
{
    bool ok = true;

    if (fixture == NULL) {
        return false;
    }
    if (fixture->instance_initialized) {
        cflow_statechart_instance_close(&fixture->instance);
        ok = cflow_executor_wait_idle(&fixture->executor) && ok;
        ok = cflow_statechart_instance_destroy(&fixture->instance) ==
                 CFLOW_STATECHART_INSTANCE_OK && ok;
        ok = tr_raft_cflow_state_machine_unbind(
                 fixture->adapter, &fixture->instance) == SALTS_OK && ok;
        fixture->instance_initialized = false;
    }
    if (fixture->adapter != NULL) {
        ok = tr_raft_cflow_state_machine_destroy(
                 fixture->adapter) == SALTS_OK && ok;
        fixture->adapter = NULL;
    }
    if (fixture->executor_initialized) {
        ok = cflow_executor_shutdown(&fixture->executor) && ok;
        cflow_executor_destroy(&fixture->executor);
        fixture->executor_initialized = false;
    }
    if (fixture->statechart_built) {
        cflow_statechart_destroy(&fixture->statechart);
        fixture->statechart_built = false;
    }
    return ok;
}

static int matrix_runtime_start(
    matrix_fixture_t *fixture,
    tr_raft_core_t **out_core,
    tr_raft_apply_runtime_t **out_runtime,
    tr_raft_apply_runtime_result_t *out_result)
{
    tr_raft_entry_t entry = matrix_entry(7);
    tr_raft_ready_t ready;
    tr_raft_apply_runtime_config_v1_t config;
    int result;

    if (fixture == NULL || out_core == NULL || out_runtime == NULL ||
        out_result == NULL) {
        return SALTS_EINVAL;
    }
    *out_core = NULL;
    *out_runtime = NULL;
    result = matrix_create_core(&entry, out_core);
    if (result != SALTS_OK) {
        return result;
    }
    memset(&ready, 0, sizeof(ready));
    result = tr_raft_core_poll(*out_core, &ready);
    if (result != SALTS_OK) {
        tr_raft_core_destroy(*out_core);
        *out_core = NULL;
        return result;
    }
    memset(&config, 0, sizeof(config));
    config.abi_version = TR_RAFT_APPLY_RUNTIME_CONFIG_ABI_V1;
    config.struct_size = sizeof(config);
    config.core = *out_core;
    config.state_machine = fixture->state_machine;
    config.max_pending_entries = 1U;
    result = tr_raft_apply_runtime_create(&config, out_runtime);
    if (result != SALTS_OK) {
        tr_raft_core_destroy(*out_core);
        *out_core = NULL;
        return result;
    }
    return tr_raft_apply_runtime_start(
        *out_runtime, &ready, out_result);
}

static void matrix_assert_unapplied(tr_raft_core_t *core)
{
    tr_raft_status_t status;

    check_equal(tr_raft_core_status(core, &status), SALTS_OK);
    check_equal(status.applied_index, 0U);
    check_false(status.ready_outstanding);
}

static void matrix_assert_settled(
    const matrix_fixture_t *fixture,
    uint64_t accepted,
    uint64_t failed,
    uint64_t cancelled)
{
    cflow_statechart_instance_stats stats;

    memset(&stats, 0, sizeof(stats));
    check_true(cflow_statechart_instance_get_stats(
        &fixture->instance, &stats));
    check_equal(stats.external_accepted, accepted);
    check_equal(stats.external_failed, failed);
    check_equal(stats.external_cancelled, cancelled);
    check_equal(stats.external_pending, 0U);
    check_equal(stats.external_in_flight, 0U);
}

spec("CFlow Raft failure and shutdown matrix")
{
    it("rejects CLOSED admission without creating an external settlement")
    {
        matrix_fixture_t fixture;
        tr_raft_core_t *core = NULL;
        tr_raft_apply_runtime_t *runtime = NULL;
        tr_raft_apply_runtime_result_t result;

        check_true(matrix_fixture_init(&fixture));
        cflow_statechart_instance_close(&fixture.instance);
        check_true(cflow_executor_wait_idle(&fixture.executor));

        check_equal(matrix_runtime_start(
                        &fixture, &core, &runtime, &result),
                    SALTS_ESHUTDOWN);
        check_true(tr_raft_apply_runtime_is_faulted(runtime));
        matrix_assert_unapplied(core);
        matrix_assert_settled(&fixture, 0U, 0U, 0U);

        check_equal(tr_raft_apply_runtime_destroy(runtime), SALTS_OK);
        tr_raft_core_destroy(core);
        check_true(matrix_fixture_cleanup(&fixture));
    }

    it("rejects Event type mismatch before mailbox ownership transfer")
    {
        matrix_fixture_t fixture;
        tr_raft_core_t *core = NULL;
        tr_raft_apply_runtime_t *runtime = NULL;
        tr_raft_apply_runtime_result_t result;

        check_true(matrix_fixture_init(&fixture));
        fixture.probe.wrong_type = true;
        check_equal(matrix_runtime_start(
                        &fixture, &core, &runtime, &result),
                    SALTS_EINVAL);
        check_true(tr_raft_apply_runtime_is_faulted(runtime));
        matrix_assert_unapplied(core);
        matrix_assert_settled(&fixture, 0U, 0U, 0U);
        check_equal(fixture.probe.guard_calls, 0U);
        check_equal(fixture.probe.action_calls, 0U);

        check_equal(tr_raft_apply_runtime_destroy(runtime), SALTS_OK);
        tr_raft_core_destroy(core);
        check_true(matrix_fixture_cleanup(&fixture));
    }

    it("maps guard callback failure to one exact failed settlement")
    {
        matrix_fixture_t fixture;
        tr_raft_core_t *core = NULL;
        tr_raft_apply_runtime_t *runtime = NULL;
        tr_raft_apply_runtime_result_t result;

        check_true(matrix_fixture_init(&fixture));
        fixture.probe.fail_guard = true;
        check_equal(matrix_runtime_start(
                        &fixture, &core, &runtime, &result),
                    SALTS_OK);
        check_equal(result.state,
                    TR_RAFT_APPLY_RUNTIME_WAITING_SETTLEMENT);
        check_true(cflow_executor_wait_idle(&fixture.executor));
        check_equal(tr_raft_apply_runtime_poll(runtime, &result),
                    SALTS_EIO);
        check_equal(result.state, TR_RAFT_APPLY_RUNTIME_FAULTED);
        matrix_assert_unapplied(core);
        matrix_assert_settled(&fixture, 1U, 1U, 0U);
        check_equal(fixture.probe.guard_calls, 1U);
        check_equal(fixture.probe.action_calls, 0U);

        check_equal(tr_raft_apply_runtime_destroy(runtime), SALTS_OK);
        tr_raft_core_destroy(core);
        check_true(matrix_fixture_cleanup(&fixture));
    }

    it("maps action callback failure to one exact failed settlement")
    {
        matrix_fixture_t fixture;
        tr_raft_core_t *core = NULL;
        tr_raft_apply_runtime_t *runtime = NULL;
        tr_raft_apply_runtime_result_t result;

        check_true(matrix_fixture_init(&fixture));
        fixture.probe.fail_action = true;
        check_equal(matrix_runtime_start(
                        &fixture, &core, &runtime, &result),
                    SALTS_OK);
        check_equal(result.state,
                    TR_RAFT_APPLY_RUNTIME_WAITING_SETTLEMENT);
        check_true(cflow_executor_wait_idle(&fixture.executor));
        check_equal(tr_raft_apply_runtime_poll(runtime, &result),
                    SALTS_EIO);
        check_equal(result.state, TR_RAFT_APPLY_RUNTIME_FAULTED);
        matrix_assert_unapplied(core);
        matrix_assert_settled(&fixture, 1U, 1U, 0U);
        check_equal(fixture.probe.guard_calls, 1U);
        check_equal(fixture.probe.action_calls, 1U);

        check_equal(tr_raft_apply_runtime_destroy(runtime), SALTS_OK);
        tr_raft_core_destroy(core);
        check_true(matrix_fixture_cleanup(&fixture));
    }

    it("settles an accepted token exactly once when close wins before execution")
    {
        matrix_fixture_t fixture;
        matrix_blocker_t blocker;
        tr_raft_core_t *core = NULL;
        tr_raft_apply_runtime_t *runtime = NULL;
        tr_raft_apply_runtime_result_t result;

        check_true(matrix_fixture_init(&fixture));
        atomic_init(&blocker.entered, false);
        atomic_init(&blocker.release, false);
        check_equal(cflow_executor_try_post(
                        &fixture.executor,
                        matrix_block_executor, &blocker),
                    CFLOW_ADMISSION_ACCEPTED);
        while (!atomic_load_explicit(
            &blocker.entered, memory_order_acquire)) {
            salts_thread_yield();
        }

        check_equal(matrix_runtime_start(
                        &fixture, &core, &runtime, &result),
                    SALTS_OK);
        check_equal(result.state,
                    TR_RAFT_APPLY_RUNTIME_WAITING_SETTLEMENT);
        check_equal(result.in_flight_token, 1U);
        check_equal(tr_raft_cflow_state_machine_destroy(
                        fixture.adapter),
                    SALTS_EBUSY);

        cflow_statechart_instance_close(&fixture.instance);
        atomic_store_explicit(
            &blocker.release, true, memory_order_release);
        check_true(cflow_executor_wait_idle(&fixture.executor));
        check_equal(tr_raft_apply_runtime_poll(runtime, &result),
                    SALTS_ECANCELED);
        check_equal(result.state, TR_RAFT_APPLY_RUNTIME_FAULTED);
        matrix_assert_unapplied(core);
        matrix_assert_settled(&fixture, 1U, 0U, 1U);
        check_equal(fixture.probe.guard_calls, 0U);
        check_equal(fixture.probe.action_calls, 0U);

        check_equal(tr_raft_apply_runtime_destroy(runtime), SALTS_OK);
        tr_raft_core_destroy(core);
        check_true(matrix_fixture_cleanup(&fixture));
    }
}
