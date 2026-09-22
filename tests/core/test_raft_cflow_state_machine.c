#include <turboraft/raft_cflow_state_machine.h>

#include <cflow/cflow.h>
#include <salts/thread.h>
#include <salts_error.h>
#include <tinytest.h>

#include <stdatomic.h>
#include <string.h>

enum cflow_fixture_id {
    FIXTURE_ROOT = 1,
    FIXTURE_INITIAL = 2,
    FIXTURE_ACTIVE = 3,
    FIXTURE_EVENT = 10,
    FIXTURE_EXECUTABLE = 20
};

typedef struct host_probe {
    size_t calls;
    uint64_t drop_token;
    uint64_t fatal_token;
} host_probe_t;

static cflow_statechart_host_result observe_host_transaction(
    void *user,
    cflow_statechart_host_context *context,
    const char **out_error)
{
    host_probe_t *probe = (host_probe_t *) user;
    const cflow_statechart_observed_event *trigger;

    if (probe == NULL || context == NULL || out_error == NULL) {
        return CFLOW_STATECHART_HOST_FATAL;
    }
    ++probe->calls;
    *out_error = NULL;
    trigger = cflow_statechart_host_context_trigger(context);
    if (trigger != NULL && trigger->origin_token == probe->drop_token) {
        return CFLOW_STATECHART_HOST_DROP;
    }
    if (trigger != NULL && trigger->origin_token == probe->fatal_token) {
        *out_error = "deliberate Raft host transaction failure";
        return CFLOW_STATECHART_HOST_FATAL;
    }
    return CFLOW_STATECHART_HOST_CONTINUE;
}

typedef struct executor_blocker {
    atomic_bool entered;
    atomic_bool release;
} executor_blocker_t;

static void block_executor(void *user)
{
    executor_blocker_t *blocker = (executor_blocker_t *) user;

    atomic_store(&blocker->entered, true);
    while (!atomic_load(&blocker->release)) {
        salts_thread_yield();
    }
}

static void noop_executor(void *user)
{
    (void) user;
}

static bool add_payload(void *user,
                        cflow_statechart_action_phase phase,
                        cflow_machine_state_id owner,
                        const void *state,
                        const cflow_event_view *event,
                        void *out_state,
                        cflow_statechart_raise_fn raise_internal,
                        void *raise_user,
                        const char **out_error)
{
    (void) user;
    (void) owner;
    (void) raise_internal;
    (void) raise_user;
    if (phase != CFLOW_STATECHART_ACTION_TRANSITION || state == NULL ||
        event == NULL || event->payload == NULL || out_state == NULL ||
        out_error == NULL) {
        return false;
    }
    *(int *) out_state = *(const int *) state +
                         *(const int *) event->payload;
    *out_error = NULL;
    return true;
}

static int decode_entry(void *context,
                        const tr_raft_entry_t *entry,
                        cflow_event_view *out_event)
{
    (void) context;
    if (entry == NULL || out_event == NULL ||
        entry->data_length != sizeof(int)) {
        return SALTS_EINVAL;
    }
    out_event->id = FIXTURE_EVENT;
    out_event->payload_type = &cmeta_type_int;
    out_event->payload = entry->data;
    return SALTS_OK;
}

static tr_raft_entry_t make_int_entry(tr_raft_index_t index, int value)
{
    tr_raft_entry_t entry;

    memset(&entry, 0, sizeof(entry));
    entry.index = index;
    entry.term = 1U;
    entry.command_id = 101U;
    entry.data_length = sizeof(value);
    memcpy(entry.data, &value, sizeof(value));
    return entry;
}

static int create_core(const tr_raft_entry_t *entries,
                       size_t entry_count,
                       tr_raft_index_t applied_index,
                       tr_raft_core_t **out_core)
{
    static const tr_raft_node_id_t voter[] = {1U};
    tr_raft_core_config_t config;

    memset(&config, 0, sizeof(config));
    config.self_id = 1U;
    config.voters = voter;
    config.voter_count = 1U;
    config.heartbeat_ticks = 2U;
    config.election_min_ticks = 5U;
    config.election_max_ticks = 10U;
    config.initial_election_timeout_ticks = 5U;
    config.initial_term = 1U;
    config.initial_log_entries = entries;
    config.initial_log_entry_count = entry_count;
    config.initial_commit_index = (tr_raft_index_t) entry_count;
    config.initial_applied_index = applied_index;
    config.max_log_entries = 4U;
    return tr_raft_core_create(&config, out_core);
}

spec("CFlow Statechart Raft state machine")
{
    it("advances Core after the matching macrostep settlement")
    {
        const cflow_statechart_state states[] = {
            {FIXTURE_ROOT, 0U, CFLOW_STATECHART_COMPOUND, 0U},
            {FIXTURE_INITIAL, FIXTURE_ROOT, CFLOW_STATECHART_INITIAL, 1U},
            {FIXTURE_ACTIVE, FIXTURE_ROOT, CFLOW_STATECHART_ATOMIC, 2U}};
        const cflow_event_type events[] = {
            {FIXTURE_EVENT, &cmeta_type_int}};
        const cflow_statechart_executable executables[] = {{
            FIXTURE_EXECUTABLE, &cmeta_type_int, CMETA_EFFECT_MAY_FAIL,
            CMETA_PROP_DETERMINISTIC | CMETA_PROP_NO_ALIAS}};
        const cflow_statechart_transition transitions[] = {
            {1U, FIXTURE_INITIAL, CFLOW_STATECHART_TRIGGER_EVENTLESS, 0U,
             0U, 0U, FIXTURE_ACTIVE, CFLOW_STATECHART_TRANSITION_EXTERNAL,
             0U, 0U},
            {2U, FIXTURE_ACTIVE, CFLOW_STATECHART_TRIGGER_EVENT,
             FIXTURE_EVENT, 0U, 0U, FIXTURE_ACTIVE,
             CFLOW_STATECHART_TRANSITION_EXTERNAL, 0U, 1U}};
        const cflow_statechart_transition_action actions[] = {
            {2U, FIXTURE_EXECUTABLE, 0U}};
        const cflow_statechart_definition definition = {
            &cmeta_type_int, states, 3U, events, 1U, NULL, 0U,
            executables, 1U, transitions, 2U, NULL, 0U, actions, 1U};
        const cflow_statechart_executable_binding binding = {
            FIXTURE_EXECUTABLE, add_payload, NULL, NULL};
        tr_raft_cflow_state_machine_config_v1_t adapter_config;
        tr_raft_cflow_state_machine_t *adapter = NULL;
        cflow_statechart_instance_hooks hooks;
        void *hook_user = NULL;
        cflow_statechart statechart = {0};
        cflow_executor executor = {0};
        cflow_statechart_instance instance = {0};
        cflow_statechart_instance_config instance_config;
        tr_raft_entry_state_machine_v1_t state_machine;
        tr_raft_entry_t entries[3] = {make_int_entry(1U, 7),
                                      make_int_entry(2U, 11),
                                      make_int_entry(3U, 13)};
        tr_raft_core_t *core = NULL;
        tr_raft_ready_t ready;
        tr_raft_apply_runtime_config_v1_t runtime_config;
        tr_raft_apply_runtime_t *runtime = NULL;
        tr_raft_apply_runtime_result_t result;
        tr_raft_status_t status;
        const cmeta_type_desc *state_type = NULL;
        int state = 0;
        host_probe_t host = {0};

        memset(&adapter_config, 0, sizeof(adapter_config));
        adapter_config.abi_version =
            TR_RAFT_CFLOW_STATE_MACHINE_CONFIG_ABI_V1;
        adapter_config.struct_size = sizeof(adapter_config);
        adapter_config.decode_entry = decode_entry;
        adapter_config.host_transaction = observe_host_transaction;
        host.drop_token = 2U;
        adapter_config.host_context = &host;
        check_equal(tr_raft_cflow_state_machine_create(&adapter_config,
                                                       &adapter),
                    SALTS_OK);
        check_equal(tr_raft_cflow_state_machine_hooks(adapter, &hooks,
                                                      &hook_user),
                    SALTS_OK);

        check_equal(cflow_statechart_build(&statechart, &definition),
                    CFLOW_STATECHART_OK);
        check_true(cflow_executor_serial_init_with_capacity(&executor, 4U));
        memset(&instance_config, 0, sizeof(instance_config));
        instance_config.statechart = &statechart;
        instance_config.initial_state = &state;
        instance_config.executables = &binding;
        instance_config.executable_count = 1U;
        instance_config.external_event_capacity = 1U;
        instance_config.internal_event_capacity = 1U;
        instance_config.completion_capacity = 1U;
        instance_config.microstep_limit = 8U;
        instance_config.executor = &executor;
        instance_config.hooks = &hooks;
        instance_config.hook_user = hook_user;
        check_equal(cflow_statechart_instance_init(&instance,
                                                   &instance_config),
                    CFLOW_STATECHART_INSTANCE_OK);
        check_equal(tr_raft_cflow_state_machine_bind(adapter, &instance),
                    SALTS_OK);
        check_equal(tr_raft_cflow_state_machine_get_spi(adapter,
                                                        &state_machine),
                    SALTS_OK);

        check_equal(create_core(entries, 2U, 0U, &core), SALTS_OK);
        memset(&ready, 0, sizeof(ready));
        check_equal(tr_raft_core_poll(core, &ready), SALTS_OK);
        memset(&runtime_config, 0, sizeof(runtime_config));
        runtime_config.abi_version = TR_RAFT_APPLY_RUNTIME_CONFIG_ABI_V1;
        runtime_config.struct_size = sizeof(runtime_config);
        runtime_config.core = core;
        runtime_config.state_machine = state_machine;
        runtime_config.max_pending_entries = 2U;
        check_equal(tr_raft_apply_runtime_create(&runtime_config, &runtime),
                    SALTS_OK);

        check_equal(tr_raft_apply_runtime_start(runtime, &ready, &result),
                    SALTS_OK);
        check_equal(result.state,
                    TR_RAFT_APPLY_RUNTIME_WAITING_SETTLEMENT);
        check_equal(result.in_flight_token, 1U);
        check_true(cflow_executor_wait_idle(&executor));
        check_equal(tr_raft_apply_runtime_poll(runtime, &result), SALTS_OK);
        check_equal(result.state,
                    TR_RAFT_APPLY_RUNTIME_WAITING_SETTLEMENT);
        check_equal(result.applied_through, 1U);
        check_equal(result.in_flight_token, 2U);
        check_true(cflow_executor_wait_idle(&executor));
        host.drop_token = 0U;
        check_equal(tr_raft_apply_runtime_poll(runtime, &result), SALTS_OK);
        check_equal(result.state,
                    TR_RAFT_APPLY_RUNTIME_WAITING_SETTLEMENT);
        check_equal(result.applied_through, 1U);
        check_equal(result.in_flight_token, 2U);
        check_true(cflow_executor_wait_idle(&executor));
        check_equal(tr_raft_apply_runtime_poll(runtime, &result), SALTS_OK);
        check_equal(result.state, TR_RAFT_APPLY_RUNTIME_COMPLETE);
        check_equal(result.applied_through, 2U);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.applied_index, 2U);
        check_false(status.ready_outstanding);
        check_true(cflow_statechart_instance_copy_state(
            &instance, &state_type, &state, sizeof(state)));
        check_true(cmeta_type_equal(state_type, &cmeta_type_int));
        check_equal(state, 18);
        check_greater(host.calls, 0U);

        check_equal(tr_raft_apply_runtime_destroy(runtime), SALTS_OK);
        tr_raft_core_destroy(core);

        host.drop_token = 0U;
        host.fatal_token = 3U;
        check_equal(create_core(entries, 3U, 2U, &core), SALTS_OK);
        memset(&ready, 0, sizeof(ready));
        check_equal(tr_raft_core_poll(core, &ready), SALTS_OK);
        runtime_config.core = core;
        runtime_config.max_pending_entries = 1U;
        check_equal(tr_raft_apply_runtime_create(&runtime_config, &runtime),
                    SALTS_OK);
        check_equal(tr_raft_apply_runtime_start(runtime, &ready, &result),
                    SALTS_OK);
        check_true(cflow_executor_wait_idle(&executor));
        check_equal(tr_raft_apply_runtime_poll(runtime, &result), SALTS_EIO);
        check_equal(result.state, TR_RAFT_APPLY_RUNTIME_FAULTED);
        check_equal(result.in_flight_token, 3U);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.applied_index, 2U);

        cflow_statechart_instance_close(&instance);
        check_true(cflow_executor_wait_idle(&executor));
        check_equal(cflow_statechart_instance_destroy(&instance),
                    CFLOW_STATECHART_INSTANCE_OK);
        check_equal(tr_raft_cflow_state_machine_unbind(adapter, &instance),
                    SALTS_OK);

        state = 31;
        host.fatal_token = 0U;
        check_equal(cflow_statechart_instance_init(&instance,
                                                   &instance_config),
                    CFLOW_STATECHART_INSTANCE_OK);
        check_equal(tr_raft_cflow_state_machine_bind(adapter, &instance),
                    SALTS_OK);
        check_equal(tr_raft_apply_runtime_reconcile(
                        runtime, &entries[2],
                        TR_RAFT_APPLY_OUTCOME_APPLIED, &result),
                    SALTS_OK);
        check_equal(result.state, TR_RAFT_APPLY_RUNTIME_COMPLETE);
        check_equal(result.applied_through, 3U);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.applied_index, 3U);
        check_true(cflow_statechart_instance_copy_state(
            &instance, &state_type, &state, sizeof(state)));
        check_true(cmeta_type_equal(state_type, &cmeta_type_int));
        check_equal(state, 31);

        check_equal(tr_raft_apply_runtime_destroy(runtime), SALTS_OK);
        tr_raft_core_destroy(core);

        check_equal(tr_raft_cflow_state_machine_unbind(adapter, &instance),
                    SALTS_EBUSY);
        check_equal(tr_raft_cflow_state_machine_destroy(adapter),
                    SALTS_EBUSY);
        cflow_statechart_instance_close(&instance);
        check_true(cflow_executor_wait_idle(&executor));
        check_equal(cflow_statechart_instance_destroy(&instance),
                    CFLOW_STATECHART_INSTANCE_OK);
        check_equal(tr_raft_cflow_state_machine_unbind(adapter, &instance),
                    SALTS_OK);
        check_equal(tr_raft_cflow_state_machine_destroy(adapter), SALTS_OK);
        check_true(cflow_executor_shutdown(&executor));
        cflow_executor_destroy(&executor);
        cflow_statechart_destroy(&statechart);
    }

    it("requires a live binding before exposing its apply SPI")
    {
        tr_raft_cflow_state_machine_config_v1_t adapter_config;
        tr_raft_cflow_state_machine_t *adapter = NULL;
        tr_raft_entry_state_machine_v1_t state_machine;

        memset(&adapter_config, 0, sizeof(adapter_config));
        adapter_config.abi_version =
            TR_RAFT_CFLOW_STATE_MACHINE_CONFIG_ABI_V1;
        adapter_config.struct_size = sizeof(adapter_config);
        adapter_config.decode_entry = decode_entry;
        check_equal(tr_raft_cflow_state_machine_create(&adapter_config,
                                                       &adapter),
                    SALTS_OK);
        check_equal(tr_raft_cflow_state_machine_get_spi(adapter,
                                                        &state_machine),
                    SALTS_EPROTO);
        check_equal(tr_raft_cflow_state_machine_destroy(adapter), SALTS_OK);
    }

    it("preserves executor-full as an exact settlement failure")
    {
        const cflow_statechart_state states[] = {
            {FIXTURE_ROOT, 0U, CFLOW_STATECHART_COMPOUND, 0U},
            {FIXTURE_INITIAL, FIXTURE_ROOT, CFLOW_STATECHART_INITIAL, 1U},
            {FIXTURE_ACTIVE, FIXTURE_ROOT, CFLOW_STATECHART_ATOMIC, 2U}};
        const cflow_event_type events[] = {
            {FIXTURE_EVENT, &cmeta_type_int}};
        const cflow_statechart_executable executables[] = {{
            FIXTURE_EXECUTABLE, &cmeta_type_int, CMETA_EFFECT_MAY_FAIL,
            CMETA_PROP_DETERMINISTIC | CMETA_PROP_NO_ALIAS}};
        const cflow_statechart_transition transitions[] = {
            {1U, FIXTURE_INITIAL, CFLOW_STATECHART_TRIGGER_EVENTLESS, 0U,
             0U, 0U, FIXTURE_ACTIVE, CFLOW_STATECHART_TRANSITION_EXTERNAL,
             0U, 0U},
            {2U, FIXTURE_ACTIVE, CFLOW_STATECHART_TRIGGER_EVENT,
             FIXTURE_EVENT, 0U, 0U, FIXTURE_ACTIVE,
             CFLOW_STATECHART_TRANSITION_EXTERNAL, 0U, 1U}};
        const cflow_statechart_transition_action actions[] = {
            {2U, FIXTURE_EXECUTABLE, 0U}};
        const cflow_statechart_definition definition = {
            &cmeta_type_int, states, 3U, events, 1U, NULL, 0U,
            executables, 1U, transitions, 2U, NULL, 0U, actions, 1U};
        const cflow_statechart_executable_binding binding = {
            FIXTURE_EXECUTABLE, add_payload, NULL, NULL};
        tr_raft_cflow_state_machine_config_v1_t adapter_config;
        tr_raft_cflow_state_machine_t *adapter = NULL;
        cflow_statechart_instance_hooks hooks;
        void *hook_user = NULL;
        cflow_statechart statechart = {0};
        cflow_executor executor = {0};
        cflow_statechart_instance instance = {0};
        cflow_statechart_instance_config instance_config;
        tr_raft_entry_state_machine_v1_t state_machine;
        tr_raft_entry_t entry = make_int_entry(1U, 7);
        tr_raft_core_t *core = NULL;
        tr_raft_ready_t ready;
        tr_raft_apply_runtime_config_v1_t runtime_config;
        tr_raft_apply_runtime_t *runtime = NULL;
        tr_raft_apply_runtime_result_t result;
        tr_raft_status_t status;
        executor_blocker_t blocker;
        int state = 0;

        memset(&adapter_config, 0, sizeof(adapter_config));
        adapter_config.abi_version =
            TR_RAFT_CFLOW_STATE_MACHINE_CONFIG_ABI_V1;
        adapter_config.struct_size = sizeof(adapter_config);
        adapter_config.decode_entry = decode_entry;
        check_equal(tr_raft_cflow_state_machine_create(&adapter_config,
                                                       &adapter),
                    SALTS_OK);
        check_equal(tr_raft_cflow_state_machine_hooks(adapter, &hooks,
                                                      &hook_user),
                    SALTS_OK);
        check_equal(cflow_statechart_build(&statechart, &definition),
                    CFLOW_STATECHART_OK);
        check_true(cflow_executor_serial_init_with_capacity(&executor, 1U));
        memset(&instance_config, 0, sizeof(instance_config));
        instance_config.statechart = &statechart;
        instance_config.initial_state = &state;
        instance_config.executables = &binding;
        instance_config.executable_count = 1U;
        instance_config.external_event_capacity = 1U;
        instance_config.internal_event_capacity = 1U;
        instance_config.completion_capacity = 1U;
        instance_config.microstep_limit = 8U;
        instance_config.executor = &executor;
        instance_config.hooks = &hooks;
        instance_config.hook_user = hook_user;
        check_equal(cflow_statechart_instance_init(&instance,
                                                   &instance_config),
                    CFLOW_STATECHART_INSTANCE_OK);
        check_true(cflow_executor_wait_idle(&executor));
        check_equal(tr_raft_cflow_state_machine_bind(adapter, &instance),
                    SALTS_OK);
        check_equal(tr_raft_cflow_state_machine_get_spi(adapter,
                                                        &state_machine),
                    SALTS_OK);

        check_equal(create_core(&entry, 1U, 0U, &core), SALTS_OK);
        memset(&ready, 0, sizeof(ready));
        check_equal(tr_raft_core_poll(core, &ready), SALTS_OK);
        memset(&runtime_config, 0, sizeof(runtime_config));
        runtime_config.abi_version = TR_RAFT_APPLY_RUNTIME_CONFIG_ABI_V1;
        runtime_config.struct_size = sizeof(runtime_config);
        runtime_config.core = core;
        runtime_config.state_machine = state_machine;
        runtime_config.max_pending_entries = 1U;
        check_equal(tr_raft_apply_runtime_create(&runtime_config, &runtime),
                    SALTS_OK);

        atomic_init(&blocker.entered, false);
        atomic_init(&blocker.release, false);
        check_equal(cflow_executor_try_post(&executor, block_executor,
                                            &blocker),
                    CFLOW_ADMISSION_ACCEPTED);
        while (!atomic_load(&blocker.entered)) {
            salts_thread_yield();
        }
        check_equal(cflow_executor_try_post(&executor, noop_executor, NULL),
                    CFLOW_ADMISSION_ACCEPTED);

        check_equal(tr_raft_apply_runtime_start(runtime, &ready, &result),
                    SALTS_OK);
        check_equal(result.state,
                    TR_RAFT_APPLY_RUNTIME_WAITING_SETTLEMENT);
        check_equal(result.in_flight_token, 1U);
        check_equal(tr_raft_apply_runtime_poll(runtime, &result),
                    SALTS_ENOBUFS);
        check_equal(result.state, TR_RAFT_APPLY_RUNTIME_FAULTED);
        check_equal(result.in_flight_token, 1U);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.applied_index, 0U);
        check_false(status.ready_outstanding);

        atomic_store(&blocker.release, true);
        check_true(cflow_executor_wait_idle(&executor));
        check_equal(tr_raft_apply_runtime_destroy(runtime), SALTS_OK);
        tr_raft_core_destroy(core);

        cflow_statechart_instance_cancel(&instance);
        check_true(cflow_executor_wait_idle(&executor));
        check_equal(cflow_statechart_instance_destroy(&instance),
                    CFLOW_STATECHART_INSTANCE_OK);
        check_equal(tr_raft_cflow_state_machine_unbind(adapter, &instance),
                    SALTS_OK);
        check_equal(tr_raft_cflow_state_machine_destroy(adapter), SALTS_OK);
        check_true(cflow_executor_shutdown(&executor));
        cflow_executor_destroy(&executor);
        cflow_statechart_destroy(&statechart);
    }

    it("keeps Core unapplied when cancellation wins before the macrostep")
    {
        const cflow_statechart_state states[] = {
            {FIXTURE_ROOT, 0U, CFLOW_STATECHART_COMPOUND, 0U},
            {FIXTURE_INITIAL, FIXTURE_ROOT, CFLOW_STATECHART_INITIAL, 1U},
            {FIXTURE_ACTIVE, FIXTURE_ROOT, CFLOW_STATECHART_ATOMIC, 2U}};
        const cflow_event_type events[] = {
            {FIXTURE_EVENT, &cmeta_type_int}};
        const cflow_statechart_executable executables[] = {{
            FIXTURE_EXECUTABLE, &cmeta_type_int, CMETA_EFFECT_MAY_FAIL,
            CMETA_PROP_DETERMINISTIC | CMETA_PROP_NO_ALIAS}};
        const cflow_statechart_transition transitions[] = {
            {1U, FIXTURE_INITIAL, CFLOW_STATECHART_TRIGGER_EVENTLESS, 0U,
             0U, 0U, FIXTURE_ACTIVE, CFLOW_STATECHART_TRANSITION_EXTERNAL,
             0U, 0U},
            {2U, FIXTURE_ACTIVE, CFLOW_STATECHART_TRIGGER_EVENT,
             FIXTURE_EVENT, 0U, 0U, FIXTURE_ACTIVE,
             CFLOW_STATECHART_TRANSITION_EXTERNAL, 0U, 1U}};
        const cflow_statechart_transition_action actions[] = {
            {2U, FIXTURE_EXECUTABLE, 0U}};
        const cflow_statechart_definition definition = {
            &cmeta_type_int, states, 3U, events, 1U, NULL, 0U,
            executables, 1U, transitions, 2U, NULL, 0U, actions, 1U};
        const cflow_statechart_executable_binding binding = {
            FIXTURE_EXECUTABLE, add_payload, NULL, NULL};
        tr_raft_cflow_state_machine_config_v1_t adapter_config;
        tr_raft_cflow_state_machine_t *adapter = NULL;
        cflow_statechart_instance_hooks hooks;
        void *hook_user = NULL;
        cflow_statechart statechart = {0};
        cflow_executor executor = {0};
        cflow_statechart_instance instance = {0};
        cflow_statechart_instance_config instance_config;
        tr_raft_entry_state_machine_v1_t state_machine;
        tr_raft_entry_t entry = make_int_entry(1U, 7);
        tr_raft_core_t *core = NULL;
        tr_raft_ready_t ready;
        tr_raft_apply_runtime_config_v1_t runtime_config;
        tr_raft_apply_runtime_t *runtime = NULL;
        tr_raft_apply_runtime_result_t result;
        tr_raft_status_t status;
        executor_blocker_t blocker;
        int state = 0;

        memset(&adapter_config, 0, sizeof(adapter_config));
        adapter_config.abi_version =
            TR_RAFT_CFLOW_STATE_MACHINE_CONFIG_ABI_V1;
        adapter_config.struct_size = sizeof(adapter_config);
        adapter_config.decode_entry = decode_entry;
        check_equal(tr_raft_cflow_state_machine_create(&adapter_config,
                                                       &adapter),
                    SALTS_OK);
        check_equal(tr_raft_cflow_state_machine_hooks(adapter, &hooks,
                                                      &hook_user),
                    SALTS_OK);
        check_equal(cflow_statechart_build(&statechart, &definition),
                    CFLOW_STATECHART_OK);
        check_true(cflow_executor_serial_init_with_capacity(&executor, 4U));
        memset(&instance_config, 0, sizeof(instance_config));
        instance_config.statechart = &statechart;
        instance_config.initial_state = &state;
        instance_config.executables = &binding;
        instance_config.executable_count = 1U;
        instance_config.external_event_capacity = 1U;
        instance_config.internal_event_capacity = 1U;
        instance_config.completion_capacity = 1U;
        instance_config.microstep_limit = 8U;
        instance_config.executor = &executor;
        instance_config.hooks = &hooks;
        instance_config.hook_user = hook_user;
        check_equal(cflow_statechart_instance_init(&instance,
                                                   &instance_config),
                    CFLOW_STATECHART_INSTANCE_OK);
        check_equal(tr_raft_cflow_state_machine_bind(adapter, &instance),
                    SALTS_OK);
        check_equal(tr_raft_cflow_state_machine_get_spi(adapter,
                                                        &state_machine),
                    SALTS_OK);
        check_equal(create_core(&entry, 1U, 0U, &core), SALTS_OK);
        memset(&ready, 0, sizeof(ready));
        check_equal(tr_raft_core_poll(core, &ready), SALTS_OK);
        memset(&runtime_config, 0, sizeof(runtime_config));
        runtime_config.abi_version = TR_RAFT_APPLY_RUNTIME_CONFIG_ABI_V1;
        runtime_config.struct_size = sizeof(runtime_config);
        runtime_config.core = core;
        runtime_config.state_machine = state_machine;
        runtime_config.max_pending_entries = 1U;
        check_equal(tr_raft_apply_runtime_create(&runtime_config, &runtime),
                    SALTS_OK);

        atomic_init(&blocker.entered, false);
        atomic_init(&blocker.release, false);
        check_equal(cflow_executor_try_post(&executor, block_executor,
                                            &blocker),
                    CFLOW_ADMISSION_ACCEPTED);
        while (!atomic_load(&blocker.entered)) {
            salts_thread_yield();
        }
        {
            const cflow_event_view untagged = {
                FIXTURE_EVENT, &cmeta_type_int, entry.data};
            check_equal(cflow_statechart_instance_try_send(&instance,
                                                            &untagged),
                        CFLOW_MAILBOX_OK);
        }
        check_equal(tr_raft_apply_runtime_start(runtime, &ready, &result),
                    SALTS_OK);
        check_equal(result.state, TR_RAFT_APPLY_RUNTIME_WAITING_ADMISSION);
        check_equal(result.cause, SALTS_ENOBUFS);
        atomic_store(&blocker.release, true);
        check_true(cflow_executor_wait_idle(&executor));

        atomic_store(&blocker.entered, false);
        atomic_store(&blocker.release, false);
        check_equal(cflow_executor_try_post(&executor, block_executor,
                                            &blocker),
                    CFLOW_ADMISSION_ACCEPTED);
        while (!atomic_load(&blocker.entered)) {
            salts_thread_yield();
        }
        check_equal(tr_raft_apply_runtime_poll(runtime, &result), SALTS_OK);
        check_equal(result.state,
                    TR_RAFT_APPLY_RUNTIME_WAITING_SETTLEMENT);
        cflow_statechart_instance_cancel(&instance);
        atomic_store(&blocker.release, true);
        check_true(cflow_executor_wait_idle(&executor));

        check_equal(tr_raft_apply_runtime_poll(runtime, &result),
                    SALTS_ECANCELED);
        check_equal(result.state, TR_RAFT_APPLY_RUNTIME_FAULTED);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.applied_index, 0U);
        check_false(status.ready_outstanding);

        check_equal(tr_raft_apply_runtime_destroy(runtime), SALTS_OK);
        tr_raft_core_destroy(core);
        check_equal(cflow_statechart_instance_destroy(&instance),
                    CFLOW_STATECHART_INSTANCE_OK);
        check_equal(tr_raft_cflow_state_machine_unbind(adapter, &instance),
                    SALTS_OK);
        check_equal(tr_raft_cflow_state_machine_destroy(adapter), SALTS_OK);
        check_true(cflow_executor_shutdown(&executor));
        cflow_executor_destroy(&executor);
        cflow_statechart_destroy(&statechart);
    }
}
