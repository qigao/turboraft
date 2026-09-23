#include <turboraft/raft_cflow_state_machine.h>
#include <turboraft/turbodb_redis_state_machine.h>

#include <cflow/cflow.h>
#include <salts_coro_executor.h>
#include <salts_error.h>
#include <tinytest.h>

#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define REDIS_ASYNC_WAIT_TIMEOUT_NS UINT64_C(5000000000)
#define REDIS_ASYNC_MAX_STEPS 16U
#define REDIS_ASYNC_MAX_COMMAND_BYTES 8192U

enum redis_async_id {
    REDIS_ASYNC_ROOT = 1,
    REDIS_ASYNC_INITIAL = 2,
    REDIS_ASYNC_ACTIVE = 3,
    REDIS_ASYNC_EVENT = 10,
    REDIS_ASYNC_ACTION = 20
};

typedef struct redis_async_persistence {
    tr_raft_entry_state_machine_v1_t cflow;
    salts_coro_executor_t *executor;
    redis_io_runtime io_runtime;
    redis_cflow_connection connection;
    tr_turbodb_redis_state_machine_t *adapter;
    tr_raft_state_machine_t legacy;
    char port_text[16];
    tr_raft_entry_t entry;
    uint64_t token;
    int task_result;
    atomic_bool task_ready;
    bool in_flight;
    bool cflow_settled;
    bool redis_ready;
} redis_async_persistence_t;

typedef struct redis_async_app {
    tr_raft_cflow_state_machine_t *adapter;
    cflow_statechart statechart;
    cflow_executor executor;
    cflow_statechart_instance instance;
    tr_raft_entry_state_machine_v1_t cflow_spi;
    redis_async_persistence_t persistence;
    tr_raft_entry_state_machine_v1_t async_spi;
    bool instance_initialized;
} redis_async_app_t;

static cflow_io_native_backend_kind redis_async_backend(void)
{
#if defined(_WIN32)
    return CFLOW_IO_NATIVE_IOCP;
#elif defined(__linux__)
    return CFLOW_IO_NATIVE_EPOLL;
#elif defined(__APPLE__)
    return CFLOW_IO_NATIVE_KQUEUE;
#else
    return CFLOW_IO_NATIVE_POLL;
#endif
}

static int redis_async_open_connection(
    redis_io_runtime *runtime,
    redis_cflow_connection *connection,
    const char *port_text)
{
    redis_io_runtime_config runtime_config = {
        redis_async_backend(), 1U, 1U};
    redis_cflow_open_config connection_config;
    redis_cflow_connect_step connect_step;
    char *port_end = NULL;
    unsigned long port;

    if (runtime == NULL || connection == NULL ||
        port_text == NULL || port_text[0] == '\0') {
        return SALTS_EINVAL;
    }
    port = strtoul(port_text, &port_end, 10);
    if (port_end == port_text || *port_end != '\0' ||
        port == 0U || port > UINT16_MAX) {
        return SALTS_EINVAL;
    }
    if (redis_io_runtime_init(runtime, &runtime_config) != SALTS_OK) {
        return SALTS_EIO;
    }
    connection_config = (redis_cflow_open_config){
        runtime, "127.0.0.1", (uint16_t) port, 1U,
        REDIS_ASYNC_MAX_COMMAND_BYTES, 64U, 4096U, 64U,
        REDIS_ASYNC_WAIT_TIMEOUT_NS};
    if (redis_cflow_connection_open(connection, &connection_config) !=
        SALTS_OK) {
        (void) redis_io_runtime_close(runtime);
        (void) redis_io_runtime_destroy(runtime);
        return SALTS_EIO;
    }
    connect_step = redis_cflow_connection_connect_next(connection);
    if (connect_step.kind != REDIS_CFLOW_CONNECT_WAIT ||
        redis_io_runtime_wait_idle(
            runtime, REDIS_ASYNC_WAIT_TIMEOUT_NS) != SALTS_OK) {
        (void) redis_cflow_connection_destroy(connection);
        (void) redis_io_runtime_close(runtime);
        (void) redis_io_runtime_destroy(runtime);
        return SALTS_EIO;
    }
    connect_step = redis_cflow_connection_connect_next(connection);
    if (connect_step.kind != REDIS_CFLOW_CONNECT_DONE) {
        (void) redis_cflow_connection_destroy(connection);
        (void) redis_io_runtime_close(runtime);
        (void) redis_io_runtime_destroy(runtime);
        return SALTS_EIO;
    }
    return SALTS_OK;
}

static redis_reply_t *redis_async_command(
    redis_cflow_connection *connection,
    redis_io_runtime *runtime,
    int argc,
    const char **argv)
{
    redis_cflow_stream stream = {0};
    redis_cflow_stream_step step;
    size_t attempt;

    if (redis_cflow_command_open(
            connection, argc, argv, NULL, 4096U, &stream) != SALTS_OK) {
        return NULL;
    }
    for (attempt = 0U; attempt < REDIS_ASYNC_MAX_STEPS; ++attempt) {
        step = redis_cflow_stream_next(&stream);
        if (step.kind == REDIS_CFLOW_STREAM_WAIT) {
            if (redis_io_runtime_wait_idle(
                    runtime, REDIS_ASYNC_WAIT_TIMEOUT_NS) != SALTS_OK) {
                break;
            }
            continue;
        }
        if (step.kind == REDIS_CFLOW_STREAM_ITEM) {
            redis_reply_t *reply = step.item;
            step = redis_cflow_stream_next(&stream);
            if (step.kind == REDIS_CFLOW_STREAM_DONE) {
                (void) redis_cflow_stream_destroy(&stream);
                return reply;
            }
            redis_reply_free(reply);
        }
        redis_reply_free(step.item);
        break;
    }
    (void) redis_cflow_stream_destroy(&stream);
    return NULL;
}

static tr_turbodb_redis_state_machine_config_t redis_async_config(
    redis_cflow_connection *connection,
    redis_io_runtime *runtime)
{
    return (tr_turbodb_redis_state_machine_config_t){
        connection, runtime, REDIS_ASYNC_WAIT_TIMEOUT_NS, 16U, 1U,
        "raft:{cflow-async}:meta",
        "raft:{cflow-async}:journal",
        "raft:{cflow-async}:identity",
        "raft:{cflow-async}:outbox"};
}

static void redis_async_init_task(coro_t *coroutine, void *user)
{
    redis_async_persistence_t *persistence =
        (redis_async_persistence_t *) user;
    tr_turbodb_redis_state_machine_config_t config;

    (void) coroutine;
    persistence->task_result = redis_async_open_connection(
        &persistence->io_runtime, &persistence->connection,
        persistence->port_text);
    if (persistence->task_result == SALTS_OK) {
        config = redis_async_config(
            &persistence->connection, &persistence->io_runtime);
        persistence->task_result =
            tr_turbodb_redis_state_machine_open(
                &config, &persistence->adapter);
    }
    if (persistence->task_result == SALTS_OK) {
        persistence->task_result =
            tr_turbodb_redis_state_machine_bind(
                persistence->adapter, &persistence->legacy);
    }
    persistence->redis_ready =
        persistence->task_result == SALTS_OK;
    atomic_store_explicit(
        &persistence->task_ready, true, memory_order_release);
}

static void redis_async_apply_task(coro_t *coroutine, void *user)
{
    redis_async_persistence_t *persistence =
        (redis_async_persistence_t *) user;
    tr_turbodb_redis_reconcile_result_t reconciliation;
    int reconcile_result;

    (void) coroutine;
    persistence->task_result = persistence->legacy.apply_batch(
        persistence->legacy.context, &persistence->entry, 1U);
    if (persistence->task_result != SALTS_OK) {
        reconcile_result =
            tr_turbodb_redis_state_machine_reconcile_batch(
                persistence->adapter, &persistence->entry, 1U,
                &reconciliation);
        if (reconcile_result == SALTS_OK &&
            reconciliation == TR_TURBODB_REDIS_RECONCILE_REPLAYED) {
            persistence->task_result = SALTS_OK;
        }
    }
    atomic_store_explicit(
        &persistence->task_ready, true, memory_order_release);
}

static void redis_async_close_task(coro_t *coroutine, void *user)
{
    redis_async_persistence_t *persistence =
        (redis_async_persistence_t *) user;
    int result = SALTS_OK;
    int close_result;

    (void) coroutine;
    if (persistence->adapter != NULL) {
        close_result = tr_turbodb_redis_state_machine_close(
            persistence->adapter);
        if (result == SALTS_OK && close_result != SALTS_OK) {
            result = close_result;
        }
        persistence->adapter = NULL;
    }
    close_result = redis_cflow_connection_destroy(
        &persistence->connection);
    if (result == SALTS_OK && close_result != SALTS_OK) {
        result = close_result;
    }
    close_result = redis_io_runtime_close(&persistence->io_runtime);
    if (result == SALTS_OK && close_result != SALTS_OK) {
        result = close_result;
    }
    close_result = redis_io_runtime_destroy(&persistence->io_runtime);
    if (result == SALTS_OK && close_result != SALTS_OK) {
        result = close_result;
    }
    persistence->redis_ready = false;
    persistence->task_result = result;
    atomic_store_explicit(
        &persistence->task_ready, true, memory_order_release);
}

static void redis_async_task_cancel(void *user, int status)
{
    redis_async_persistence_t *persistence =
        (redis_async_persistence_t *) user;

    persistence->task_result =
        status == SALTS_OK ? SALTS_EIO : status;
    atomic_store_explicit(
        &persistence->task_ready, true, memory_order_release);
}

static bool redis_async_submit(
    redis_async_persistence_t *persistence,
    coro_fn run)
{
    salts_coro_executor_task_t task = {
        run, redis_async_task_cancel, NULL, persistence};

    atomic_store_explicit(
        &persistence->task_ready, false, memory_order_relaxed);
    return salts_coro_executor_try_submit(
               persistence->executor, &task) == SALTS_OK;
}

static tr_raft_apply_admission_t redis_async_try_apply(
    void *context,
    const tr_raft_entry_t *entry,
    uint64_t token,
    int *out_cause)
{
    redis_async_persistence_t *persistence =
        (redis_async_persistence_t *) context;
    tr_raft_apply_admission_t admission;

    if (persistence == NULL || entry == NULL || token == 0U ||
        out_cause == NULL || persistence->in_flight ||
        !persistence->redis_ready) {
        if (out_cause != NULL) {
            *out_cause = SALTS_EPROTO;
        }
        return TR_RAFT_APPLY_ADMISSION_FAILED;
    }
    admission = persistence->cflow.try_apply(
        persistence->cflow.context, entry, token, out_cause);
    if (admission == TR_RAFT_APPLY_ADMISSION_ACCEPTED) {
        persistence->entry = *entry;
        persistence->token = token;
        persistence->in_flight = true;
        persistence->cflow_settled = false;
    }
    return admission;
}

static int redis_async_poll_settlement(
    void *context,
    tr_raft_apply_settlement_t *out_settlement,
    bool *out_ready)
{
    redis_async_persistence_t *persistence =
        (redis_async_persistence_t *) context;
    tr_raft_apply_settlement_t cflow_settlement;
    bool cflow_ready = false;
    int result;

    if (persistence == NULL || out_settlement == NULL ||
        out_ready == NULL || !persistence->in_flight) {
        return SALTS_EINVAL;
    }
    *out_ready = false;
    if (!persistence->cflow_settled) {
        memset(&cflow_settlement, 0, sizeof(cflow_settlement));
        result = persistence->cflow.poll_settlement(
            persistence->cflow.context,
            &cflow_settlement, &cflow_ready);
        if (result != SALTS_OK || !cflow_ready) {
            return result;
        }
        if (cflow_settlement.token != persistence->token ||
            cflow_settlement.outcome != TR_RAFT_APPLY_OUTCOME_APPLIED ||
            cflow_settlement.cause != SALTS_OK) {
            *out_settlement = cflow_settlement;
            persistence->in_flight = false;
            *out_ready = true;
            return SALTS_OK;
        }
        if (!redis_async_submit(
                persistence, redis_async_apply_task)) {
            out_settlement->token = persistence->token;
            out_settlement->outcome = TR_RAFT_APPLY_OUTCOME_UNKNOWN;
            out_settlement->cause = SALTS_ENOBUFS;
            persistence->in_flight = false;
            *out_ready = true;
            return SALTS_OK;
        }
        persistence->cflow_settled = true;
    }
    if (!atomic_load_explicit(
            &persistence->task_ready, memory_order_acquire)) {
        return SALTS_OK;
    }
    out_settlement->token = persistence->token;
    out_settlement->outcome =
        persistence->task_result == SALTS_OK
            ? TR_RAFT_APPLY_OUTCOME_APPLIED
            : TR_RAFT_APPLY_OUTCOME_UNKNOWN;
    out_settlement->cause = persistence->task_result;
    persistence->in_flight = false;
    persistence->cflow_settled = false;
    *out_ready = true;
    return SALTS_OK;
}

static bool redis_async_persistence_init(
    redis_async_persistence_t *persistence,
    const tr_raft_entry_state_machine_v1_t *cflow,
    const char *port_text)
{
    salts_coro_executor_config_t config =
        SALTS_CORO_EXECUTOR_CONFIG_DEFAULT;

    if (persistence == NULL || cflow == NULL ||
        port_text == NULL ||
        strlen(port_text) >= sizeof(persistence->port_text)) {
        return false;
    }
    memset(persistence, 0, sizeof(*persistence));
    persistence->cflow = *cflow;
    memcpy(persistence->port_text, port_text, strlen(port_text) + 1U);
    config.worker_count = 1U;
    config.queue_capacity_per_worker = 2U;
    config.coroutine_pool.initial_capacity = 0U;
    config.coroutine_pool.max_capacity = 2U;
    persistence->executor = salts_coro_executor_create(&config);
    if (persistence->executor == NULL) {
        return false;
    }
    atomic_init(&persistence->task_ready, false);
    if (!redis_async_submit(persistence, redis_async_init_task) ||
        salts_coro_executor_wait(persistence->executor) != SALTS_OK ||
        !atomic_load_explicit(
            &persistence->task_ready, memory_order_acquire) ||
        persistence->task_result != SALTS_OK ||
        !persistence->redis_ready) {
        (void) salts_coro_executor_shutdown(persistence->executor);
        (void) salts_coro_executor_wait(persistence->executor);
        (void) salts_coro_executor_destroy(persistence->executor);
        persistence->executor = NULL;
        return false;
    }
    return true;
}

static bool redis_async_persistence_wait(
    redis_async_persistence_t *persistence)
{
    return persistence != NULL &&
           persistence->executor != NULL &&
           salts_coro_executor_wait(persistence->executor) == SALTS_OK;
}

static bool redis_async_persistence_destroy(
    redis_async_persistence_t *persistence)
{
    int shutdown_result;
    int wait_result;
    int destroy_result;

    if (persistence == NULL || persistence->executor == NULL ||
        persistence->in_flight) {
        return false;
    }
    if (!redis_async_submit(persistence, redis_async_close_task) ||
        salts_coro_executor_wait(persistence->executor) != SALTS_OK ||
        persistence->task_result != SALTS_OK) {
        return false;
    }
    shutdown_result =
        salts_coro_executor_shutdown(persistence->executor);
    wait_result = salts_coro_executor_wait(persistence->executor);
    destroy_result =
        salts_coro_executor_destroy(persistence->executor);
    persistence->executor = NULL;
    return shutdown_result == SALTS_OK &&
           wait_result == SALTS_OK &&
           destroy_result == SALTS_OK;
}

static bool redis_async_action(
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
    (void) user;
    (void) owner;
    (void) raise_internal;
    (void) raise_user;
    if (phase != CFLOW_STATECHART_ACTION_TRANSITION ||
        state == NULL || event == NULL || event->payload == NULL ||
        out_state == NULL || out_error == NULL) {
        return false;
    }
    *(int *) out_state =
        *(const int *) state + *(const int *) event->payload;
    *out_error = NULL;
    return true;
}

static int redis_async_decode(
    void *context,
    const tr_raft_entry_t *entry,
    cflow_event_view *out_event)
{
    (void) context;
    if (entry == NULL || out_event == NULL ||
        entry->data_length != sizeof(int)) {
        return SALTS_EINVAL;
    }
    memset(out_event, 0, sizeof(*out_event));
    out_event->id = REDIS_ASYNC_EVENT;
    out_event->payload_type = &cmeta_type_int;
    out_event->payload = entry->data;
    return SALTS_OK;
}

static bool redis_async_app_init(
    redis_async_app_t *app,
    const char *port_text)
{
    static const cflow_statechart_state states[] = {
        {REDIS_ASYNC_ROOT, 0U, CFLOW_STATECHART_COMPOUND, 0U},
        {REDIS_ASYNC_INITIAL, REDIS_ASYNC_ROOT,
         CFLOW_STATECHART_INITIAL, 1U},
        {REDIS_ASYNC_ACTIVE, REDIS_ASYNC_ROOT,
         CFLOW_STATECHART_ATOMIC, 2U}
    };
    static const cflow_event_type events[] = {
        {REDIS_ASYNC_EVENT, &cmeta_type_int}
    };
    static const cflow_statechart_executable executables[] = {{
        REDIS_ASYNC_ACTION, &cmeta_type_int,
        CMETA_EFFECT_MAY_FAIL,
        CMETA_PROP_DETERMINISTIC | CMETA_PROP_NO_ALIAS
    }};
    static const cflow_statechart_transition transitions[] = {
        {1U, REDIS_ASYNC_INITIAL,
         CFLOW_STATECHART_TRIGGER_EVENTLESS, 0U, 0U, 0U,
         REDIS_ASYNC_ACTIVE,
         CFLOW_STATECHART_TRANSITION_EXTERNAL, 0U, 0U},
        {2U, REDIS_ASYNC_ACTIVE,
         CFLOW_STATECHART_TRIGGER_EVENT, REDIS_ASYNC_EVENT,
         0U, 0U, REDIS_ASYNC_ACTIVE,
         CFLOW_STATECHART_TRANSITION_EXTERNAL, 0U, 1U}
    };
    static const cflow_statechart_transition_action actions[] = {
        {2U, REDIS_ASYNC_ACTION, 0U}
    };
    static const cflow_statechart_definition definition = {
        &cmeta_type_int,
        states, 3U,
        events, 1U,
        NULL, 0U,
        executables, 1U,
        transitions, 2U,
        NULL, 0U,
        actions, 1U
    };
    tr_raft_cflow_state_machine_config_v1_t adapter_config;
    cflow_statechart_instance_hooks hooks;
    cflow_statechart_executable_binding binding;
    cflow_statechart_instance_config instance_config;
    void *hook_user = NULL;
    int initial_state = 0;

    if (app == NULL) {
        return false;
    }
    memset(app, 0, sizeof(*app));
    memset(&adapter_config, 0, sizeof(adapter_config));
    adapter_config.abi_version =
        TR_RAFT_CFLOW_STATE_MACHINE_CONFIG_ABI_V1;
    adapter_config.struct_size = sizeof(adapter_config);
    adapter_config.decode_entry = redis_async_decode;
    if (tr_raft_cflow_state_machine_create(
            &adapter_config, &app->adapter) != SALTS_OK ||
        tr_raft_cflow_state_machine_hooks(
            app->adapter, &hooks, &hook_user) != SALTS_OK ||
        cflow_statechart_build(
            &app->statechart, &definition) != CFLOW_STATECHART_OK ||
        !cflow_executor_serial_init_with_capacity(
            &app->executor, 4U)) {
        return false;
    }

    memset(&binding, 0, sizeof(binding));
    binding.id = REDIS_ASYNC_ACTION;
    binding.fn = redis_async_action;

    memset(&instance_config, 0, sizeof(instance_config));
    instance_config.statechart = &app->statechart;
    instance_config.initial_state = &initial_state;
    instance_config.executables = &binding;
    instance_config.executable_count = 1U;
    instance_config.external_event_capacity = 1U;
    instance_config.internal_event_capacity = 1U;
    instance_config.completion_capacity = 1U;
    instance_config.microstep_limit = 8U;
    instance_config.executor = &app->executor;
    instance_config.hooks = &hooks;
    instance_config.hook_user = hook_user;
    if (cflow_statechart_instance_init(
            &app->instance, &instance_config) !=
            CFLOW_STATECHART_INSTANCE_OK) {
        return false;
    }
    app->instance_initialized = true;
    if (tr_raft_cflow_state_machine_bind(
            app->adapter, &app->instance) != SALTS_OK ||
        tr_raft_cflow_state_machine_get_spi(
            app->adapter, &app->cflow_spi) != SALTS_OK ||
        !cflow_executor_wait_idle(&app->executor) ||
        !redis_async_persistence_init(
            &app->persistence, &app->cflow_spi, port_text)) {
        return false;
    }

    memset(&app->async_spi, 0, sizeof(app->async_spi));
    app->async_spi.abi_version =
        TR_RAFT_ENTRY_STATE_MACHINE_ABI_V1;
    app->async_spi.struct_size = sizeof(app->async_spi);
    app->async_spi.context = &app->persistence;
    app->async_spi.try_apply = redis_async_try_apply;
    app->async_spi.poll_settlement =
        redis_async_poll_settlement;
    return true;
}

static bool redis_async_app_destroy(redis_async_app_t *app)
{
    bool ok = true;

    if (app == NULL) {
        return false;
    }
    ok = redis_async_persistence_destroy(
             &app->persistence) && ok;
    if (app->instance_initialized) {
        cflow_statechart_instance_close(&app->instance);
        ok = cflow_executor_wait_idle(&app->executor) && ok;
        ok = cflow_statechart_instance_destroy(&app->instance) ==
                 CFLOW_STATECHART_INSTANCE_OK && ok;
        ok = tr_raft_cflow_state_machine_unbind(
                 app->adapter, &app->instance) == SALTS_OK && ok;
        app->instance_initialized = false;
    }
    if (app->adapter != NULL) {
        ok = tr_raft_cflow_state_machine_destroy(
                 app->adapter) == SALTS_OK && ok;
        app->adapter = NULL;
    }
    ok = cflow_executor_shutdown(&app->executor) && ok;
    cflow_executor_destroy(&app->executor);
    cflow_statechart_destroy(&app->statechart);
    return ok;
}

static tr_raft_entry_t redis_async_entry(int value)
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

static int redis_async_create_core(
    const tr_raft_entry_t *entry,
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

static int redis_async_apply_once(
    redis_async_app_t *app,
    const tr_raft_entry_t *entry,
    tr_raft_core_t **out_core,
    tr_raft_apply_runtime_t **out_runtime,
    tr_raft_apply_runtime_result_t *out_result)
{
    tr_raft_ready_t ready;
    tr_raft_apply_runtime_config_v1_t config;
    int result;

    *out_core = NULL;
    *out_runtime = NULL;
    result = redis_async_create_core(entry, out_core);
    if (result != SALTS_OK) {
        return result;
    }
    memset(&ready, 0, sizeof(ready));
    result = tr_raft_core_poll(*out_core, &ready);
    if (result != SALTS_OK) {
        return result;
    }
    memset(&config, 0, sizeof(config));
    config.abi_version = TR_RAFT_APPLY_RUNTIME_CONFIG_ABI_V1;
    config.struct_size = sizeof(config);
    config.core = *out_core;
    config.state_machine = app->async_spi;
    config.max_pending_entries = 1U;
    result = tr_raft_apply_runtime_create(
        &config, out_runtime);
    if (result != SALTS_OK) {
        return result;
    }
    result = tr_raft_apply_runtime_start(
        *out_runtime, &ready, out_result);
    if (result != SALTS_OK) {
        return result;
    }
    if (!cflow_executor_wait_idle(&app->executor)) {
        return SALTS_EIO;
    }
    result = tr_raft_apply_runtime_poll(
        *out_runtime, out_result);
    if (result != SALTS_OK) {
        return result;
    }
    if (!redis_async_persistence_wait(&app->persistence)) {
        return SALTS_EIO;
    }
    return tr_raft_apply_runtime_poll(
        *out_runtime, out_result);
}

static bool redis_async_read_i64(
    redis_cflow_connection *connection,
    redis_io_runtime *runtime,
    const char *command,
    const char *key,
    const char *field,
    int64_t expected)
{
    const char *argv[3] = {command, key, field};
    redis_reply_t *reply =
        redis_async_command(connection, runtime, 3, argv);
    bool ok = reply != NULL;

    if (ok) {
        if (reply->str != NULL) {
            char *end = NULL;
            long long value = strtoll(reply->str, &end, 10);
            ok = end != reply->str && *end == '\0' &&
                 value == expected;
        } else {
            ok = reply->integer == expected;
        }
    }
    redis_reply_free(reply);
    return ok;
}

spec("CFlow Redis Lua asynchronous persistence")
{
    it("applies, replays after restart, and faults on conflict through ApplyRuntime")
    {
        const char *port_text = getenv("TURBODB_REDIS_TEST_PORT");
        static const char *delete_command[] = {
            "DEL",
            "raft:{cflow-async}:meta",
            "raft:{cflow-async}:journal",
            "raft:{cflow-async}:identity",
            "raft:{cflow-async}:outbox"
        };
        static const char *outbox_command[] = {
            "XLEN", "raft:{cflow-async}:outbox"
        };
        redis_io_runtime verify_runtime = {0};
        redis_cflow_connection verify_connection = {0};
        redis_reply_t *reply = NULL;
        redis_async_app_t app;
        tr_raft_entry_t entry = redis_async_entry(7);
        tr_raft_entry_t conflict = redis_async_entry(9);
        tr_raft_core_t *core = NULL;
        tr_raft_apply_runtime_t *runtime = NULL;
        tr_raft_apply_runtime_result_t result;
        tr_raft_status_t status;
        const cmeta_type_desc *state_type = NULL;
        int state = 0;

        if (port_text == NULL || port_text[0] == '\0') {
            return;
        }
        check_equal(redis_async_open_connection(
                        &verify_runtime, &verify_connection,
                        port_text), SALTS_OK);
        reply = redis_async_command(
            &verify_connection, &verify_runtime, 5,
            delete_command);
        check_not_null(reply);
        redis_reply_free(reply);

        check_true(redis_async_app_init(&app, port_text));
        check_equal(redis_async_apply_once(
                        &app, &entry, &core, &runtime,
                        &result), SALTS_OK);
        check_equal(result.state, TR_RAFT_APPLY_RUNTIME_COMPLETE);
        check_equal(result.applied_through, 1U);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.applied_index, 1U);
        check_true(cflow_statechart_instance_copy_state(
            &app.instance, &state_type, &state, sizeof(state)));
        check_true(cmeta_type_equal(state_type, &cmeta_type_int));
        check_equal(state, 7);
        check_true(redis_async_read_i64(
            &verify_connection, &verify_runtime,
            "HGET", "raft:{cflow-async}:meta",
            "applied_index", 1));
        reply = redis_async_command(
            &verify_connection, &verify_runtime, 2,
            outbox_command);
        check_not_null(reply);
        check_equal(reply->integer, 1);
        redis_reply_free(reply);

        check_equal(tr_raft_apply_runtime_destroy(runtime), SALTS_OK);
        tr_raft_core_destroy(core);
        check_true(redis_async_app_destroy(&app));

        /*
         * Restart from a Core/app checkpoint behind Redis. The CFlow
         * transition is reconstructed and the Lua adapter proves the exact
         * journal identity as REPLAYED without duplicating the outbox.
         */
        check_true(redis_async_app_init(&app, port_text));
        core = NULL;
        runtime = NULL;
        memset(&result, 0, sizeof(result));
        state = 0;
        check_equal(redis_async_apply_once(
                        &app, &entry, &core, &runtime,
                        &result), SALTS_OK);
        check_equal(result.state, TR_RAFT_APPLY_RUNTIME_COMPLETE);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.applied_index, 1U);
        reply = redis_async_command(
            &verify_connection, &verify_runtime, 2,
            outbox_command);
        check_not_null(reply);
        check_equal(reply->integer, 1);
        redis_reply_free(reply);
        check_equal(tr_raft_apply_runtime_destroy(runtime), SALTS_OK);
        tr_raft_core_destroy(core);
        check_true(redis_async_app_destroy(&app));

        /*
         * The same index with a different payload is CONFLICT in Redis.
         * CFlow may have executed its reconstructed in-memory transition, but
         * the durable fact source rejects it and Core remains unapplied.
         */
        check_true(redis_async_app_init(&app, port_text));
        core = NULL;
        runtime = NULL;
        memset(&result, 0, sizeof(result));
        check_equal(redis_async_apply_once(
                        &app, &conflict, &core, &runtime,
                        &result), SALTS_EPROTO);
        check_equal(result.state, TR_RAFT_APPLY_RUNTIME_FAULTED);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.applied_index, 0U);
        reply = redis_async_command(
            &verify_connection, &verify_runtime, 2,
            outbox_command);
        check_not_null(reply);
        check_equal(reply->integer, 1);
        redis_reply_free(reply);

        check_equal(tr_raft_apply_runtime_destroy(runtime), SALTS_OK);
        tr_raft_core_destroy(core);
        check_true(redis_async_app_destroy(&app));

        check_equal(redis_cflow_connection_destroy(
                        &verify_connection), SALTS_OK);
        check_equal(redis_io_runtime_close(&verify_runtime), SALTS_OK);
        check_equal(redis_io_runtime_destroy(&verify_runtime), SALTS_OK);
    }
}
