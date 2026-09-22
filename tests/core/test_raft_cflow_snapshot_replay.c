#include <turboraft/raft_cflow_state_machine.h>

#include <cflow/cflow.h>
#include <salts_error.h>
#include <tinytest.h>

#include <string.h>

enum snapshot_fixture_id {
    SNAPSHOT_ROOT = 1,
    SNAPSHOT_INITIAL = 2,
    SNAPSHOT_ACTIVE = 3,
    SNAPSHOT_EVENT = 10,
    SNAPSHOT_ACTION = 20
};

typedef struct snapshot_fixture {
    tr_raft_cflow_state_machine_t *adapter;
    cflow_statechart statechart;
    cflow_executor executor;
    cflow_statechart_instance instance;
    tr_raft_entry_state_machine_v1_t state_machine;
    int initial_state;
} snapshot_fixture_t;

static bool snapshot_add(void *user,
                         cflow_statechart_action_phase phase,
                         cflow_machine_state_id owner,
                         const void *state,
                         const cflow_event_view *event,
                         void *out_state,
                         cflow_statechart_raise_fn raise_internal,
                         void *raise_user,
                         const char **out_error)
{
    int delta;

    (void) user;
    (void) owner;
    (void) raise_internal;
    (void) raise_user;
    if (phase != CFLOW_STATECHART_ACTION_TRANSITION || state == NULL ||
        event == NULL || event->payload == NULL || out_state == NULL ||
        out_error == NULL) {
        return false;
    }
    memcpy(&delta, event->payload, sizeof(delta));
    *(int *) out_state = *(const int *) state + delta;
    *out_error = NULL;
    return true;
}

static int snapshot_decode(void *context,
                           const tr_raft_entry_t *entry,
                           cflow_event_view *out_event)
{
    (void) context;
    if (entry == NULL || out_event == NULL ||
        entry->data_length != sizeof(int)) {
        return SALTS_EINVAL;
    }
    out_event->id = SNAPSHOT_EVENT;
    out_event->payload_type = &cmeta_type_int;
    out_event->payload = entry->data;
    return SALTS_OK;
}

static tr_raft_entry_t snapshot_entry(tr_raft_index_t index, int delta)
{
    tr_raft_entry_t entry;

    memset(&entry, 0, sizeof(entry));
    entry.index = index;
    entry.term = 1U;
    entry.command_id = 100U + index;
    entry.data_length = sizeof(delta);
    memcpy(entry.data, &delta, sizeof(delta));
    return entry;
}

static int snapshot_fixture_open(snapshot_fixture_t *fixture,
                                 int initial_state)
{
    static const cflow_statechart_state states[] = {
        {SNAPSHOT_ROOT, 0U, CFLOW_STATECHART_COMPOUND, 0U},
        {SNAPSHOT_INITIAL, SNAPSHOT_ROOT, CFLOW_STATECHART_INITIAL, 1U},
        {SNAPSHOT_ACTIVE, SNAPSHOT_ROOT, CFLOW_STATECHART_ATOMIC, 2U}};
    static const cflow_event_type events[] = {
        {SNAPSHOT_EVENT, &cmeta_type_int}};
    static const cflow_statechart_executable executables[] = {{
        SNAPSHOT_ACTION, &cmeta_type_int, CMETA_EFFECT_MAY_FAIL,
        CMETA_PROP_DETERMINISTIC | CMETA_PROP_NO_ALIAS}};
    static const cflow_statechart_transition transitions[] = {
        {1U, SNAPSHOT_INITIAL, CFLOW_STATECHART_TRIGGER_EVENTLESS, 0U,
         0U, 0U, SNAPSHOT_ACTIVE, CFLOW_STATECHART_TRANSITION_EXTERNAL,
         0U, 0U},
        {2U, SNAPSHOT_ACTIVE, CFLOW_STATECHART_TRIGGER_EVENT,
         SNAPSHOT_EVENT, 0U, 0U, SNAPSHOT_ACTIVE,
         CFLOW_STATECHART_TRANSITION_EXTERNAL, 0U, 1U}};
    static const cflow_statechart_transition_action actions[] = {
        {2U, SNAPSHOT_ACTION, 0U}};
    static const cflow_statechart_definition definition = {
        &cmeta_type_int, states, 3U, events, 1U, NULL, 0U,
        executables, 1U, transitions, 2U, NULL, 0U, actions, 1U};
    static const cflow_statechart_executable_binding binding = {
        SNAPSHOT_ACTION, snapshot_add, NULL, NULL};
    tr_raft_cflow_state_machine_config_v1_t adapter_config;
    cflow_statechart_instance_config instance_config;
    cflow_statechart_instance_hooks hooks;
    void *hook_user = NULL;

    memset(fixture, 0, sizeof(*fixture));
    fixture->initial_state = initial_state;
    memset(&adapter_config, 0, sizeof(adapter_config));
    adapter_config.abi_version =
        TR_RAFT_CFLOW_STATE_MACHINE_CONFIG_ABI_V1;
    adapter_config.struct_size = sizeof(adapter_config);
    adapter_config.decode_entry = snapshot_decode;
    if (tr_raft_cflow_state_machine_create(&adapter_config,
                                            &fixture->adapter) != SALTS_OK) {
        return SALTS_EIO;
    }
    if (tr_raft_cflow_state_machine_hooks(fixture->adapter, &hooks,
                                           &hook_user) != SALTS_OK ||
        cflow_statechart_build(&fixture->statechart, &definition) !=
            CFLOW_STATECHART_OK ||
        !cflow_executor_serial_init_with_capacity(&fixture->executor, 4U)) {
        return SALTS_EIO;
    }
    memset(&instance_config, 0, sizeof(instance_config));
    instance_config.statechart = &fixture->statechart;
    instance_config.initial_state = &fixture->initial_state;
    instance_config.executables = &binding;
    instance_config.executable_count = 1U;
    instance_config.external_event_capacity = 1U;
    instance_config.internal_event_capacity = 1U;
    instance_config.completion_capacity = 1U;
    instance_config.microstep_limit = 8U;
    instance_config.executor = &fixture->executor;
    instance_config.hooks = &hooks;
    instance_config.hook_user = hook_user;
    if (cflow_statechart_instance_init(&fixture->instance,
                                        &instance_config) !=
            CFLOW_STATECHART_INSTANCE_OK ||
        tr_raft_cflow_state_machine_bind(fixture->adapter,
                                          &fixture->instance) != SALTS_OK ||
        tr_raft_cflow_state_machine_get_spi(
            fixture->adapter, &fixture->state_machine) != SALTS_OK ||
        !cflow_executor_wait_idle(&fixture->executor)) {
        return SALTS_EIO;
    }
    return SALTS_OK;
}

static int snapshot_fixture_close(snapshot_fixture_t *fixture)
{
    cflow_statechart_instance_close(&fixture->instance);
    if (!cflow_executor_wait_idle(&fixture->executor) ||
        cflow_statechart_instance_destroy(&fixture->instance) !=
            CFLOW_STATECHART_INSTANCE_OK ||
        tr_raft_cflow_state_machine_unbind(fixture->adapter,
                                            &fixture->instance) != SALTS_OK ||
        tr_raft_cflow_state_machine_destroy(fixture->adapter) != SALTS_OK ||
        !cflow_executor_shutdown(&fixture->executor)) {
        return SALTS_EIO;
    }
    cflow_executor_destroy(&fixture->executor);
    cflow_statechart_destroy(&fixture->statechart);
    memset(fixture, 0, sizeof(*fixture));
    return SALTS_OK;
}

static int snapshot_core_create(const tr_raft_entry_t *entries,
                                size_t entry_count,
                                const tr_raft_snapshot_point_t *point,
                                tr_raft_index_t commit_index,
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
    config.initial_commit_index = commit_index;
    config.initial_applied_index = applied_index;
    config.max_log_entries = 8U;
    if (point != NULL) {
        config.initial_configuration = &point->configuration;
        config.initial_last_log_index = point->index;
        config.initial_last_log_term = point->term;
    }
    return tr_raft_core_create(&config, out_core);
}

static int snapshot_drive(snapshot_fixture_t *fixture,
                          tr_raft_core_t *core,
                          size_t max_entries)
{
    tr_raft_apply_runtime_config_v1_t config;
    tr_raft_apply_runtime_t *runtime = NULL;
    tr_raft_apply_runtime_result_t result;
    tr_raft_ready_t ready;
    size_t step;
    int status;

    memset(&ready, 0, sizeof(ready));
    status = tr_raft_core_poll(core, &ready);
    if (status != SALTS_OK) {
        return status;
    }
    memset(&config, 0, sizeof(config));
    config.abi_version = TR_RAFT_APPLY_RUNTIME_CONFIG_ABI_V1;
    config.struct_size = sizeof(config);
    config.core = core;
    config.state_machine = fixture->state_machine;
    config.max_pending_entries = max_entries;
    status = tr_raft_apply_runtime_create(&config, &runtime);
    if (status != SALTS_OK) {
        return status;
    }
    status = tr_raft_apply_runtime_start(runtime, &ready, &result);
    for (step = 0U;
         status == SALTS_OK &&
         result.state != TR_RAFT_APPLY_RUNTIME_COMPLETE && step < 16U;
         ++step) {
        if (result.state == TR_RAFT_APPLY_RUNTIME_WAITING_SETTLEMENT &&
            !cflow_executor_wait_idle(&fixture->executor)) {
            status = SALTS_EIO;
            break;
        }
        status = tr_raft_apply_runtime_poll(runtime, &result);
    }
    if (status == SALTS_OK &&
        result.state != TR_RAFT_APPLY_RUNTIME_COMPLETE) {
        status = SALTS_EPROTO;
    }
    if (tr_raft_apply_runtime_destroy(runtime) != SALTS_OK &&
        status == SALTS_OK) {
        status = SALTS_EBUSY;
    }
    return status;
}

static int snapshot_copy_state(snapshot_fixture_t *fixture, int *out_state)
{
    const cmeta_type_desc *state_type = NULL;

    if (!cflow_statechart_instance_copy_state(
            &fixture->instance, &state_type, out_state,
            sizeof(*out_state)) ||
        !cmeta_type_equal(state_type, &cmeta_type_int)) {
        return SALTS_EPROTO;
    }
    return SALTS_OK;
}

spec("CFlow in-memory snapshot and suffix replay")
{
    it("matches uninterrupted execution after restoring the applied boundary")
    {
        tr_raft_entry_t entries[3] = {
            snapshot_entry(1U, 7), snapshot_entry(2U, 11),
            snapshot_entry(3U, 13)};
        snapshot_fixture_t checkpoint_fixture;
        snapshot_fixture_t restored_fixture;
        snapshot_fixture_t uninterrupted_fixture;
        tr_raft_core_t *checkpoint_core = NULL;
        tr_raft_core_t *restored_core = NULL;
        tr_raft_core_t *uninterrupted_core = NULL;
        tr_raft_snapshot_point_t point;
        tr_raft_status_t status;
        int checkpoint_state = 0;
        int restored_state = 0;
        int uninterrupted_state = 0;

        check_equal(snapshot_fixture_open(&checkpoint_fixture, 0), SALTS_OK);
        check_equal(snapshot_core_create(entries, 2U, NULL, 2U, 0U,
                                         &checkpoint_core),
                    SALTS_OK);
        check_equal(snapshot_drive(&checkpoint_fixture, checkpoint_core, 2U),
                    SALTS_OK);
        check_equal(snapshot_copy_state(&checkpoint_fixture,
                                        &checkpoint_state),
                    SALTS_OK);
        check_equal(checkpoint_state, 18);
        check_equal(tr_raft_core_snapshot_point(checkpoint_core, &point),
                    SALTS_OK);
        check_equal(point.index, 2U);
        check_equal(point.term, 1U);
        check_equal(snapshot_fixture_close(&checkpoint_fixture), SALTS_OK);
        tr_raft_core_destroy(checkpoint_core);

        check_equal(snapshot_fixture_open(&restored_fixture,
                                          checkpoint_state),
                    SALTS_OK);
        check_equal(snapshot_core_create(&entries[2], 1U, &point, 3U, 2U,
                                         &restored_core),
                    SALTS_OK);
        check_equal(snapshot_drive(&restored_fixture, restored_core, 1U),
                    SALTS_OK);
        check_equal(snapshot_copy_state(&restored_fixture, &restored_state),
                    SALTS_OK);
        check_equal(restored_state, 31);
        check_equal(tr_raft_core_status(restored_core, &status), SALTS_OK);
        check_equal(status.applied_index, 3U);
        check_equal(snapshot_fixture_close(&restored_fixture), SALTS_OK);
        tr_raft_core_destroy(restored_core);

        check_equal(snapshot_fixture_open(&uninterrupted_fixture, 0),
                    SALTS_OK);
        check_equal(snapshot_core_create(entries, 3U, NULL, 3U, 0U,
                                         &uninterrupted_core),
                    SALTS_OK);
        check_equal(snapshot_drive(&uninterrupted_fixture,
                                   uninterrupted_core, 3U),
                    SALTS_OK);
        check_equal(snapshot_copy_state(&uninterrupted_fixture,
                                        &uninterrupted_state),
                    SALTS_OK);
        check_equal(uninterrupted_state, restored_state);
        check_equal(snapshot_fixture_close(&uninterrupted_fixture), SALTS_OK);
        tr_raft_core_destroy(uninterrupted_core);
    }
}
