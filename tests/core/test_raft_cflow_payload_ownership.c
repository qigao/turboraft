#include <turboraft/raft_cflow_state_machine.h>

#include <cflow/cflow.h>
#include <cmeta/cmeta.h>
#include <salts/thread.h>
#include <salts_error.h>
#include <tinytest.h>

#include <stdatomic.h>
#include <stdint.h>
#include <string.h>

enum {
    PAYLOAD_ROOT = 1,
    PAYLOAD_INITIAL = 2,
    PAYLOAD_ACTIVE = 3,
    PAYLOAD_EVENT = 10,
    PAYLOAD_EXECUTABLE = 20,
    PAYLOAD_CAPACITY = 64
};

typedef struct bounded_payload_event {
    uint32_t length;
    uint32_t generation;
    unsigned char bytes[PAYLOAD_CAPACITY];
} bounded_payload_event_t;

static const cmeta_type_traits bounded_payload_traits = {
    .flags = CMETA_TRAIT_TRIVIAL_COPY | CMETA_TRAIT_TRIVIAL_DESTROY
};

static const cmeta_type_identity bounded_payload_identity =
    CMETA_TYPE_ID_ATOM_INIT("turboraft.test.cflow.bounded-payload");

static const cmeta_type_desc bounded_payload_type = {
    .name = "bounded_payload_event",
    .size = sizeof(bounded_payload_event_t),
    .align = _Alignof(bounded_payload_event_t),
    .kind = CMETA_T_OBJECT,
    .pointee = NULL,
    .traits = &bounded_payload_traits,
    .identity = &bounded_payload_identity
};

typedef struct payload_decoder {
    bounded_payload_event_t scratch;
    uint32_t generation;
} payload_decoder_t;

typedef struct payload_action_probe {
    uint32_t expected_generation;
    uint32_t expected_length;
    int expected_sum;
    size_t calls;
    bool exact;
} payload_action_probe_t;

typedef struct executor_blocker {
    atomic_bool entered;
    atomic_bool release;
} executor_blocker_t;

typedef struct payload_fixture {
    payload_decoder_t decoder;
    payload_action_probe_t action;
    tr_raft_cflow_state_machine_t *adapter;
    cflow_statechart statechart;
    cflow_executor executor;
    cflow_statechart_instance instance;
    tr_raft_entry_state_machine_v1_t state_machine;
    bool instance_initialized;
} payload_fixture_t;

static const cflow_statechart_state payload_states[] = {
    {PAYLOAD_ROOT, 0U, CFLOW_STATECHART_COMPOUND, 0U},
    {PAYLOAD_INITIAL, PAYLOAD_ROOT, CFLOW_STATECHART_INITIAL, 1U},
    {PAYLOAD_ACTIVE, PAYLOAD_ROOT, CFLOW_STATECHART_ATOMIC, 2U}
};

static const cflow_event_type payload_events[] = {
    {PAYLOAD_EVENT, &bounded_payload_type}
};

static const cflow_statechart_executable payload_executables[] = {{
    PAYLOAD_EXECUTABLE, &cmeta_type_int, CMETA_EFFECT_MAY_FAIL,
    CMETA_PROP_DETERMINISTIC | CMETA_PROP_NO_ALIAS
}};

static const cflow_statechart_transition payload_transitions[] = {
    {1U, PAYLOAD_INITIAL, CFLOW_STATECHART_TRIGGER_EVENTLESS, 0U,
     0U, 0U, PAYLOAD_ACTIVE, CFLOW_STATECHART_TRANSITION_EXTERNAL,
     0U, 0U},
    {2U, PAYLOAD_ACTIVE, CFLOW_STATECHART_TRIGGER_EVENT, PAYLOAD_EVENT,
     0U, 0U, PAYLOAD_ACTIVE, CFLOW_STATECHART_TRANSITION_EXTERNAL,
     0U, 1U}
};

static const cflow_statechart_transition_action payload_actions[] = {
    {2U, PAYLOAD_EXECUTABLE, 0U}
};

static const cflow_statechart_definition payload_definition = {
    &cmeta_type_int,
    payload_states, 3U,
    payload_events, 1U,
    NULL, 0U,
    payload_executables, 1U,
    payload_transitions, 2U,
    NULL, 0U,
    payload_actions, 1U
};

static void block_executor(void *user)
{
    executor_blocker_t *blocker = (executor_blocker_t *) user;

    atomic_store_explicit(&blocker->entered, true, memory_order_release);
    while (!atomic_load_explicit(&blocker->release, memory_order_acquire)) {
        salts_thread_yield();
    }
}

static bool apply_bounded_payload(
    void *user,
    cflow_statechart_action_phase phase,
    cflow_machine_state_id owner,
    const void *state,
    const cflow_event_view *event,
    void *out_state,
    cflow_statechart_raise_fn raise_internal,
    void *raise_user,
    const char **out_error)
{
    payload_action_probe_t *probe = (payload_action_probe_t *) user;
    const bounded_payload_event_t *payload;
    size_t index;
    int sum = 0;

    (void) owner;
    (void) raise_internal;
    (void) raise_user;
    if (probe == NULL ||
        phase != CFLOW_STATECHART_ACTION_TRANSITION ||
        state == NULL || event == NULL || out_state == NULL ||
        out_error == NULL || event->payload == NULL ||
        !cmeta_type_equal(event->payload_type, &bounded_payload_type)) {
        return false;
    }
    payload = (const bounded_payload_event_t *) event->payload;
    if (payload->length > PAYLOAD_CAPACITY) {
        return false;
    }
    for (index = 0U; index < payload->length; ++index) {
        sum += payload->bytes[index];
    }
    ++probe->calls;
    probe->exact =
        payload->generation == probe->expected_generation &&
        payload->length == probe->expected_length &&
        sum == probe->expected_sum;
    *(int *) out_state = sum;
    *out_error = NULL;
    return probe->exact;
}

static int decode_bounded_payload(
    void *context,
    const tr_raft_entry_t *entry,
    cflow_event_view *out_event)
{
    payload_decoder_t *decoder = (payload_decoder_t *) context;

    if (decoder == NULL || entry == NULL || out_event == NULL ||
        entry->data_length == 0U) {
        return SALTS_EINVAL;
    }
    if (entry->data_length > PAYLOAD_CAPACITY) {
        return SALTS_ENOBUFS;
    }
    memset(&decoder->scratch, 0, sizeof(decoder->scratch));
    ++decoder->generation;
    decoder->scratch.length = (uint32_t) entry->data_length;
    decoder->scratch.generation = decoder->generation;
    memcpy(decoder->scratch.bytes, entry->data, entry->data_length);

    memset(out_event, 0, sizeof(*out_event));
    out_event->id = PAYLOAD_EVENT;
    out_event->payload_type = &bounded_payload_type;
    out_event->payload = &decoder->scratch;
    return SALTS_OK;
}

static tr_raft_entry_t make_payload_entry(
    tr_raft_index_t index,
    size_t length,
    unsigned char seed,
    int *out_sum)
{
    tr_raft_entry_t entry;
    size_t offset;
    int sum = 0;

    memset(&entry, 0, sizeof(entry));
    entry.index = index;
    entry.term = 1U;
    entry.command_id = 900U + index;
    entry.data_length = length;
    for (offset = 0U; offset < length; ++offset) {
        entry.data[offset] = (unsigned char) (seed + (unsigned char) offset);
        sum += entry.data[offset];
    }
    if (out_sum != NULL) {
        *out_sum = sum;
    }
    return entry;
}

static int create_payload_core(
    const tr_raft_entry_t *entries,
    size_t entry_count,
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
    config.initial_log_entries = entries;
    config.initial_log_entry_count = entry_count;
    config.initial_commit_index = (tr_raft_index_t) entry_count;
    config.max_log_entries = 4U;
    return tr_raft_core_create(&config, out_core);
}

static bool payload_fixture_init(payload_fixture_t *fixture)
{
    tr_raft_cflow_state_machine_config_v1_t adapter_config;
    cflow_statechart_instance_hooks hooks;
    cflow_statechart_executable_binding binding;
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
    adapter_config.decode_entry = decode_bounded_payload;
    adapter_config.decode_context = &fixture->decoder;
    if (tr_raft_cflow_state_machine_create(
            &adapter_config, &fixture->adapter) != SALTS_OK ||
        tr_raft_cflow_state_machine_hooks(
            fixture->adapter, &hooks, &hook_user) != SALTS_OK ||
        cflow_statechart_build(&fixture->statechart, &payload_definition) !=
            CFLOW_STATECHART_OK ||
        !cflow_executor_serial_init_with_capacity(&fixture->executor, 4U)) {
        return false;
    }

    memset(&binding, 0, sizeof(binding));
    binding.id = PAYLOAD_EXECUTABLE;
    binding.fn = apply_bounded_payload;
    binding.user = &fixture->action;

    memset(&instance_config, 0, sizeof(instance_config));
    instance_config.statechart = &fixture->statechart;
    instance_config.initial_state = &initial_state;
    instance_config.executables = &binding;
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

static bool payload_fixture_destroy(payload_fixture_t *fixture)
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
        ok = tr_raft_cflow_state_machine_destroy(fixture->adapter) ==
                 SALTS_OK && ok;
        fixture->adapter = NULL;
    }
    ok = cflow_executor_shutdown(&fixture->executor) && ok;
    cflow_executor_destroy(&fixture->executor);
    cflow_statechart_destroy(&fixture->statechart);
    return ok;
}

spec("CFlow Raft payload ownership")
{
    it("copies bounded variable payload before decoder scratch generation reuse")
    {
        payload_fixture_t fixture;
        executor_blocker_t blocker;
        tr_raft_entry_t entry;
        tr_raft_core_t *core = NULL;
        tr_raft_ready_t ready;
        tr_raft_apply_runtime_config_v1_t runtime_config;
        tr_raft_apply_runtime_t *runtime = NULL;
        tr_raft_apply_runtime_result_t result;
        tr_raft_status_t status;
        const cmeta_type_desc *state_type = NULL;
        int expected_sum = 0;
        int state = 0;

        check_true(payload_fixture_init(&fixture));
        entry = make_payload_entry(1U, 37U, 3U, &expected_sum);
        check_equal(create_payload_core(&entry, 1U, &core), SALTS_OK);
        memset(&ready, 0, sizeof(ready));
        check_equal(tr_raft_core_poll(core, &ready), SALTS_OK);
        memset(&runtime_config, 0, sizeof(runtime_config));
        runtime_config.abi_version = TR_RAFT_APPLY_RUNTIME_CONFIG_ABI_V1;
        runtime_config.struct_size = sizeof(runtime_config);
        runtime_config.core = core;
        runtime_config.state_machine = fixture.state_machine;
        runtime_config.max_pending_entries = 1U;
        check_equal(tr_raft_apply_runtime_create(
                        &runtime_config, &runtime), SALTS_OK);

        atomic_init(&blocker.entered, false);
        atomic_init(&blocker.release, false);
        check_equal(cflow_executor_try_post(
                        &fixture.executor, block_executor, &blocker),
                    CFLOW_ADMISSION_ACCEPTED);
        while (!atomic_load_explicit(
            &blocker.entered, memory_order_acquire)) {
            salts_thread_yield();
        }

        check_equal(tr_raft_apply_runtime_start(runtime, &ready, &result),
                    SALTS_OK);
        check_equal(result.state,
                    TR_RAFT_APPLY_RUNTIME_WAITING_SETTLEMENT);
        fixture.action.expected_generation = fixture.decoder.generation;
        fixture.action.expected_length = 37U;
        fixture.action.expected_sum = expected_sum;

        /*
         * Simulate immediate decoder generation reuse after admission. The
         * delayed Statechart macrostep must observe the mailbox-owned copy,
         * not this overwritten scratch generation.
         */
        memset(&fixture.decoder.scratch, 0xA5,
               sizeof(fixture.decoder.scratch));
        fixture.decoder.generation += 17U;

        atomic_store_explicit(
            &blocker.release, true, memory_order_release);
        check_true(cflow_executor_wait_idle(&fixture.executor));
        check_equal(tr_raft_apply_runtime_poll(runtime, &result), SALTS_OK);
        check_equal(result.state, TR_RAFT_APPLY_RUNTIME_COMPLETE);
        check_equal(result.applied_through, 1U);
        check_equal(fixture.action.calls, 1U);
        check_true(fixture.action.exact);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.applied_index, 1U);
        check_true(cflow_statechart_instance_copy_state(
            &fixture.instance, &state_type, &state, sizeof(state)));
        check_true(cmeta_type_equal(state_type, &cmeta_type_int));
        check_equal(state, expected_sum);

        check_equal(tr_raft_apply_runtime_destroy(runtime), SALTS_OK);
        tr_raft_core_destroy(core);
        check_true(payload_fixture_destroy(&fixture));
    }

    it("rejects a variable command larger than the declared inline bound")
    {
        payload_fixture_t fixture;
        tr_raft_entry_t entry;
        tr_raft_core_t *core = NULL;
        tr_raft_ready_t ready;
        tr_raft_apply_runtime_config_v1_t runtime_config;
        tr_raft_apply_runtime_t *runtime = NULL;
        tr_raft_apply_runtime_result_t result;
        tr_raft_status_t status;

        check_true(payload_fixture_init(&fixture));
        entry = make_payload_entry(
            1U, PAYLOAD_CAPACITY + 1U, 7U, NULL);
        check_equal(create_payload_core(&entry, 1U, &core), SALTS_OK);
        memset(&ready, 0, sizeof(ready));
        check_equal(tr_raft_core_poll(core, &ready), SALTS_OK);
        memset(&runtime_config, 0, sizeof(runtime_config));
        runtime_config.abi_version = TR_RAFT_APPLY_RUNTIME_CONFIG_ABI_V1;
        runtime_config.struct_size = sizeof(runtime_config);
        runtime_config.core = core;
        runtime_config.state_machine = fixture.state_machine;
        runtime_config.max_pending_entries = 1U;
        check_equal(tr_raft_apply_runtime_create(
                        &runtime_config, &runtime), SALTS_OK);

        check_equal(tr_raft_apply_runtime_start(runtime, &ready, &result),
                    SALTS_ENOBUFS);
        check_true(tr_raft_apply_runtime_is_faulted(runtime));
        check_equal(fixture.action.calls, 0U);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.applied_index, 0U);
        check_false(status.ready_outstanding);

        check_equal(tr_raft_apply_runtime_destroy(runtime), SALTS_OK);
        tr_raft_core_destroy(core);
        check_true(payload_fixture_destroy(&fixture));
    }
}
