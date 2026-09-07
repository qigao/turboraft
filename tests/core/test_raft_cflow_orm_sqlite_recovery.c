#include <turboraft/raft_cflow_state_machine.h>

#include <orm.h>
#include <salts_coro_executor.h>
#include <salts_error.h>
#include "tinytest.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

enum {
    APP_ROOT = 1U,
    APP_INITIAL = 2U,
    APP_ACTIVE = 3U,
    APP_EVENT = 10U,
    APP_EXECUTABLE = 20U
};

typedef struct app_command_event {
    uint64_t index;
    uint64_t term;
    uint64_t command_id;
    size_t payload_size;
    int delta;
    uint8_t payload[sizeof(int)];
} app_command_event_t;

static const cmeta_type_identity app_command_event_identity =
    CMETA_TYPE_ID_ATOM_INIT("turboraft.test.AppCommandEvent");
static const cmeta_type_traits app_command_event_traits = {
    .flags = CMETA_TRAIT_TRIVIAL_COPY | CMETA_TRAIT_TRIVIAL_DESTROY};
static const cmeta_type_desc app_command_event_type = {
    .name = "app_command_event_t",
    .size = sizeof(app_command_event_t),
    .align = _Alignof(app_command_event_t),
    .kind = CMETA_T_OBJECT,
    .traits = &app_command_event_traits,
    .identity = &app_command_event_identity};

typedef enum app_store_failure {
    APP_STORE_NO_FAILURE = 0,
    APP_STORE_FAIL_BEFORE_COMMIT,
    APP_STORE_FAIL_AFTER_COMMIT
} app_store_failure_t;

typedef enum app_store_apply_result {
    APP_STORE_APPLIED = 0,
    APP_STORE_ROLLED_BACK,
    APP_STORE_COMMIT_UNKNOWN
} app_store_apply_result_t;

typedef struct app_store_snapshot {
    int value;
    uint64_t applied_index;
    uint64_t journal_rows;
    app_command_event_t identity;
    bool has_identity;
} app_store_snapshot_t;

typedef struct app_store {
    orm_connection_t *connection;
    app_store_failure_t failure;
    uint64_t failure_index;
    salts_coro_executor_t *persistence_executor;
    atomic_bool wrong_executor;
} app_store_t;

typedef struct app_decoder {
    app_command_event_t event;
} app_decoder_t;

typedef struct app_cflow_fixture {
    cflow_statechart_instance instance;
    bool initialized;
} app_cflow_fixture_t;

typedef struct app_persisting_state_machine {
    tr_raft_entry_state_machine_v1_t cflow;
    cflow_statechart_instance *instance;
    app_store_t *store;
    salts_coro_executor_t *executor;
    app_command_event_t entry;
    uint64_t token;
    int candidate_state;
    app_store_apply_result_t store_outcome;
    int task_cause;
    atomic_bool task_ready;
    bool in_flight;
    bool cflow_settled;
} app_persisting_state_machine_t;

static orm_status_t app_execute(
    orm_connection_t *connection,
    orm_transaction_t *transaction,
    const char *sql,
    const orm_value_t *bindings,
    size_t binding_count,
    orm_result_t **out_result,
    orm_error_t *error)
{
    orm_query_t *query = NULL;
    orm_result_t *result = NULL;
    orm_status_t status;
    size_t index;

    status = orm_raw(connection, orm_view(sql), &query, error);
    if (status != ORM_STATUS_OK) {
        return status;
    }
    for (index = 0U; index < binding_count; ++index) {
        status = orm_query_bind(query, bindings[index], error);
        if (status != ORM_STATUS_OK) {
            orm_query_destroy(query);
            return status;
        }
    }
    status = transaction == NULL
                 ? orm_query_execute(query, &result, error)
                 : orm_query_execute_in_transaction(query, transaction,
                                                    &result, error);
    orm_query_destroy(query);
    if (status != ORM_STATUS_OK) {
        orm_result_destroy(result);
        return status;
    }
    if (out_result != NULL) {
        *out_result = result;
    } else {
        orm_result_destroy(result);
    }
    return ORM_STATUS_OK;
}

static bool app_store_open(app_store_t *store, const char *filename)
{
    orm_config_t config;
    orm_option_t option;
    orm_error_t error;

    if (store == NULL || filename == NULL || store->connection != NULL) {
        return false;
    }
    orm_error_init(&error);
    orm_config(&config);
    option.keyword = orm_view("filename");
    option.value = orm_view(filename);
    config.driver = orm_view("sqlite");
    config.options = &option;
    config.option_count = 1U;
    return orm_connect(&config, &store->connection, &error) == ORM_STATUS_OK;
}

static void app_store_close(app_store_t *store)
{
    if (store == NULL) {
        return;
    }
    if (store->connection != NULL) {
        orm_disconnect(store->connection);
    }
    store->connection = NULL;
}

static bool app_store_create_schema(app_store_t *store)
{
    static const char state_schema[] =
        "create table app_state("
        "singleton integer primary key check(singleton=1),"
        "value integer not null,applied_index integer not null)";
    static const char state_seed[] =
        "insert into app_state(singleton,value,applied_index) values(1,0,0)";
    static const char journal_schema[] =
        "create table raft_journal("
        "log_index integer primary key,term integer not null,"
        "command_id integer not null,payload blob not null)";
    orm_error_t error;

    if (store == NULL || store->connection == NULL) {
        return false;
    }
    orm_error_init(&error);
    return app_execute(store->connection, NULL, state_schema, NULL, 0U,
                       NULL, &error) == ORM_STATUS_OK &&
           app_execute(store->connection, NULL, state_seed, NULL, 0U,
                       NULL, &error) == ORM_STATUS_OK &&
           app_execute(store->connection, NULL, journal_schema, NULL, 0U,
                       NULL, &error) == ORM_STATUS_OK;
}

static bool app_result_i64(const orm_result_t *result,
                           uint64_t row,
                           uint64_t column,
                           int64_t *out_value,
                           orm_error_t *error)
{
    return orm_result_get_int64(result, row, column, out_value, error) ==
           ORM_STATUS_OK;
}

static bool app_store_read_snapshot(app_store_t *store,
                                    app_store_snapshot_t *snapshot)
{
    static const char state_sql[] =
        "select value,applied_index from app_state where singleton=1";
    static const char count_sql[] = "select count(*) from raft_journal";
    static const char identity_sql[] =
        "select term,command_id,payload from raft_journal where log_index=?1";
    orm_result_t *result = NULL;
    orm_error_t error;
    orm_blob_t payload = {0};
    orm_value_t binding;
    int64_t value;
    int64_t applied;
    int64_t rows;
    int64_t term;
    int64_t command_id;
    uint64_t row_count = 0U;

    if (store == NULL || store->connection == NULL || snapshot == NULL) {
        return false;
    }
    memset(snapshot, 0, sizeof(*snapshot));
    orm_error_init(&error);
    if (app_execute(store->connection, NULL, state_sql, NULL, 0U,
                    &result, &error) != ORM_STATUS_OK ||
        orm_result_row_count(result, &row_count, &error) != ORM_STATUS_OK ||
        row_count != 1U ||
        !app_result_i64(result, 0U, 0U, &value, &error) ||
        !app_result_i64(result, 0U, 1U, &applied, &error) ||
        applied < 0) {
        orm_result_destroy(result);
        return false;
    }
    snapshot->value = (int) value;
    snapshot->applied_index = (uint64_t) applied;
    orm_result_destroy(result);
    result = NULL;

    if (app_execute(store->connection, NULL, count_sql, NULL, 0U,
                    &result, &error) != ORM_STATUS_OK ||
        !app_result_i64(result, 0U, 0U, &rows, &error) || rows < 0) {
        orm_result_destroy(result);
        return false;
    }
    snapshot->journal_rows = (uint64_t) rows;
    orm_result_destroy(result);
    result = NULL;
    if (snapshot->applied_index == 0U) {
        return true;
    }

    binding = orm_i64((int64_t) snapshot->applied_index);
    if (app_execute(store->connection, NULL, identity_sql, &binding, 1U,
                    &result, &error) != ORM_STATUS_OK ||
        orm_result_row_count(result, &row_count, &error) != ORM_STATUS_OK ||
        row_count != 1U ||
        !app_result_i64(result, 0U, 0U, &term, &error) || term < 0 ||
        !app_result_i64(result, 0U, 1U, &command_id, &error) ||
        command_id < 0 ||
        orm_result_get_blob(result, 0U, 2U, &payload, &error) !=
            ORM_STATUS_OK ||
        payload.size != sizeof(int)) {
        orm_result_destroy(result);
        return false;
    }
    snapshot->identity.index = snapshot->applied_index;
    snapshot->identity.term = (uint64_t) term;
    snapshot->identity.command_id = (uint64_t) command_id;
    snapshot->identity.payload_size = payload.size;
    memcpy(snapshot->identity.payload, payload.data, payload.size);
    memcpy(&snapshot->identity.delta, payload.data, sizeof(int));
    snapshot->has_identity = true;
    orm_result_destroy(result);
    return true;
}

static app_store_apply_result_t app_store_apply(
    app_store_t *store,
    const app_command_event_t *entry,
    int state)
{
    static const char marker_sql[] =
        "select applied_index from app_state where singleton=1";
    static const char journal_sql[] =
        "insert into raft_journal(log_index,term,command_id,payload) "
        "values(?1,?2,?3,?4)";
    static const char state_sql[] =
        "update app_state set value=?1,applied_index=?2 "
        "where singleton=1 and applied_index=?3";
    orm_transaction_t *transaction = NULL;
    orm_result_t *result = NULL;
    orm_error_t error;
    orm_value_t journal_bindings[4];
    orm_value_t state_bindings[3];
    orm_status_t status;
    int64_t applied = -1;
    uint64_t affected = 0U;
    bool committed = false;
    app_store_apply_result_t outcome = APP_STORE_ROLLED_BACK;

    if (store != NULL &&
        salts_coro_executor_current() != store->persistence_executor) {
        atomic_store_explicit(&store->wrong_executor, true,
                              memory_order_release);
        return APP_STORE_ROLLED_BACK;
    }
    if (store == NULL || store->connection == NULL || entry == NULL ||
        entry->index == 0U || entry->index > INT64_MAX ||
        entry->term > INT64_MAX || entry->command_id > INT64_MAX ||
        entry->payload_size != sizeof(int)) {
        return APP_STORE_ROLLED_BACK;
    }
    orm_error_init(&error);
    status = orm_transaction_begin(store->connection,
                                   ORM_ISOLATION_SERIALIZABLE,
                                   &transaction, &error);
    if (status != ORM_STATUS_OK) {
        return APP_STORE_ROLLED_BACK;
    }
    status = app_execute(store->connection, transaction, marker_sql, NULL,
                         0U, &result, &error);
    if (status != ORM_STATUS_OK ||
        !app_result_i64(result, 0U, 0U, &applied, &error) || applied < 0 ||
        entry->index != (uint64_t) applied + 1U) {
        goto cleanup;
    }
    orm_result_destroy(result);
    result = NULL;

    journal_bindings[0] = orm_i64((int64_t) entry->index);
    journal_bindings[1] = orm_i64((int64_t) entry->term);
    journal_bindings[2] = orm_i64((int64_t) entry->command_id);
    journal_bindings[3] = orm_blob(entry->payload, entry->payload_size);
    status = app_execute(store->connection, transaction, journal_sql,
                         journal_bindings, 4U, NULL, &error);
    if (status != ORM_STATUS_OK) {
        goto cleanup;
    }
    if (store->failure == APP_STORE_FAIL_BEFORE_COMMIT &&
        store->failure_index == entry->index) {
        store->failure = APP_STORE_NO_FAILURE;
        goto cleanup;
    }

    state_bindings[0] = orm_i64((int64_t) state);
    state_bindings[1] = orm_i64((int64_t) entry->index);
    state_bindings[2] = orm_i64(applied);
    status = app_execute(store->connection, transaction, state_sql,
                         state_bindings, 3U, &result, &error);
    if (status != ORM_STATUS_OK ||
        orm_result_affected_rows(result, &affected, &error) != ORM_STATUS_OK ||
        affected != 1U) {
        goto cleanup;
    }
    orm_result_destroy(result);
    result = NULL;
    if (orm_transaction_commit(transaction, &error) != ORM_STATUS_OK) {
        goto cleanup;
    }
    committed = true;
    outcome = APP_STORE_APPLIED;
    if (store->failure == APP_STORE_FAIL_AFTER_COMMIT &&
        store->failure_index == entry->index) {
        store->failure = APP_STORE_NO_FAILURE;
        outcome = APP_STORE_COMMIT_UNKNOWN;
    }

cleanup:
    orm_result_destroy(result);
    if (!committed) {
        (void) orm_transaction_rollback(transaction, &error);
    }
    orm_transaction_destroy(transaction);
    return outcome;
}

static void app_persist_task(coro_t *coroutine, void *user)
{
    app_persisting_state_machine_t *state_machine =
        (app_persisting_state_machine_t *) user;

    (void) coroutine;
    state_machine->store_outcome = app_store_apply(
        state_machine->store, &state_machine->entry,
        state_machine->candidate_state);
    state_machine->task_cause =
        state_machine->store_outcome == APP_STORE_APPLIED ? SALTS_OK
                                                          : SALTS_EIO;
    atomic_store_explicit(&state_machine->task_ready, true,
                          memory_order_release);
}

static void app_persist_cancel(void *user, int status)
{
    app_persisting_state_machine_t *state_machine =
        (app_persisting_state_machine_t *) user;

    state_machine->store_outcome = APP_STORE_COMMIT_UNKNOWN;
    state_machine->task_cause = status == SALTS_OK ? SALTS_EIO : status;
    atomic_store_explicit(&state_machine->task_ready, true,
                          memory_order_release);
}

static tr_raft_apply_admission_t app_persisting_try_apply(
    void *context, const tr_raft_entry_t *entry, uint64_t token,
    int *out_cause)
{
    app_persisting_state_machine_t *state_machine =
        (app_persisting_state_machine_t *) context;
    tr_raft_apply_admission_t admission;

    if (state_machine == NULL || entry == NULL || token == 0U ||
        entry->data_length != sizeof(int) ||
        out_cause == NULL || state_machine->in_flight) {
        if (out_cause != NULL) {
            *out_cause = SALTS_EPROTO;
        }
        return TR_RAFT_APPLY_ADMISSION_FAILED;
    }
    admission = state_machine->cflow.try_apply(
        state_machine->cflow.context, entry, token, out_cause);
    if (admission == TR_RAFT_APPLY_ADMISSION_ACCEPTED) {
        memset(&state_machine->entry, 0, sizeof(state_machine->entry));
        state_machine->entry.index = entry->index;
        state_machine->entry.term = entry->term;
        state_machine->entry.command_id = entry->command_id;
        state_machine->entry.payload_size = entry->data_length;
        memcpy(state_machine->entry.payload, entry->data,
               entry->data_length);
        memcpy(&state_machine->entry.delta, entry->data,
               sizeof(state_machine->entry.delta));
        state_machine->token = token;
        state_machine->in_flight = true;
        state_machine->cflow_settled = false;
        atomic_store_explicit(&state_machine->task_ready, false,
                              memory_order_relaxed);
        state_machine->task_cause = SALTS_OK;
    }
    return admission;
}

static int app_persisting_poll_settlement(
    void *context, tr_raft_apply_settlement_t *out_settlement,
    bool *out_ready)
{
    app_persisting_state_machine_t *state_machine =
        (app_persisting_state_machine_t *) context;
    tr_raft_apply_settlement_t cflow_settlement;
    salts_coro_executor_task_t task;
    const cmeta_type_desc *state_type = NULL;
    bool cflow_ready = false;
    int status;

    if (state_machine == NULL || out_settlement == NULL ||
        out_ready == NULL || !state_machine->in_flight) {
        return SALTS_EINVAL;
    }
    *out_ready = false;
    if (!state_machine->cflow_settled) {
        memset(&cflow_settlement, 0, sizeof(cflow_settlement));
        status = state_machine->cflow.poll_settlement(
            state_machine->cflow.context, &cflow_settlement, &cflow_ready);
        if (status != SALTS_OK || !cflow_ready) {
            return status;
        }
        if (cflow_settlement.token != state_machine->token ||
            cflow_settlement.outcome != TR_RAFT_APPLY_OUTCOME_APPLIED ||
            cflow_settlement.cause != SALTS_OK) {
            *out_settlement = cflow_settlement;
            state_machine->in_flight = false;
            *out_ready = true;
            return SALTS_OK;
        }
        if (!cflow_statechart_instance_copy_state(
                state_machine->instance, &state_type,
                &state_machine->candidate_state,
                sizeof(state_machine->candidate_state)) ||
            !cmeta_type_equal(state_type, &cmeta_type_int)) {
            out_settlement->token = state_machine->token;
            out_settlement->outcome = TR_RAFT_APPLY_OUTCOME_UNKNOWN;
            out_settlement->cause = SALTS_EPROTO;
            state_machine->in_flight = false;
            *out_ready = true;
            return SALTS_OK;
        }
        task = (salts_coro_executor_task_t){
            app_persist_task, app_persist_cancel, NULL, state_machine};
        status = salts_coro_executor_try_submit(state_machine->executor,
                                                &task);
        if (status != SALTS_OK) {
            out_settlement->token = state_machine->token;
            out_settlement->outcome = TR_RAFT_APPLY_OUTCOME_UNKNOWN;
            out_settlement->cause = status;
            state_machine->in_flight = false;
            *out_ready = true;
            return SALTS_OK;
        }
        state_machine->cflow_settled = true;
    }
    if (!atomic_load_explicit(&state_machine->task_ready,
                              memory_order_acquire)) {
        return SALTS_OK;
    }
    out_settlement->token = state_machine->token;
    out_settlement->outcome =
        state_machine->store_outcome == APP_STORE_APPLIED
            ? TR_RAFT_APPLY_OUTCOME_APPLIED
            : TR_RAFT_APPLY_OUTCOME_UNKNOWN;
    out_settlement->cause = state_machine->task_cause;
    state_machine->in_flight = false;
    state_machine->cflow_settled = false;
    *out_ready = true;
    return SALTS_OK;
}

static bool app_persisting_state_machine_init(
    app_persisting_state_machine_t *state_machine,
    const tr_raft_entry_state_machine_v1_t *cflow,
    cflow_statechart_instance *instance, app_store_t *store)
{
    salts_coro_executor_config_t config =
        SALTS_CORO_EXECUTOR_CONFIG_DEFAULT;

    if (state_machine == NULL || cflow == NULL || instance == NULL ||
        store == NULL) {
        return false;
    }
    memset(state_machine, 0, sizeof(*state_machine));
    config.worker_count = 1U;
    config.queue_capacity_per_worker = 1U;
    config.coroutine_pool.initial_capacity = 0U;
    config.coroutine_pool.max_capacity = 1U;
    state_machine->executor = salts_coro_executor_create(&config);
    if (state_machine->executor == NULL) {
        return false;
    }
    state_machine->cflow = *cflow;
    state_machine->instance = instance;
    state_machine->store = store;
    store->persistence_executor = state_machine->executor;
    atomic_init(&state_machine->task_ready, false);
    atomic_init(&store->wrong_executor, false);
    return true;
}

static void app_persisting_state_machine_get_spi(
    app_persisting_state_machine_t *state_machine,
    tr_raft_entry_state_machine_v1_t *out_spi)
{
    memset(out_spi, 0, sizeof(*out_spi));
    out_spi->abi_version = TR_RAFT_ENTRY_STATE_MACHINE_ABI_V1;
    out_spi->struct_size = sizeof(*out_spi);
    out_spi->context = state_machine;
    out_spi->try_apply = app_persisting_try_apply;
    out_spi->poll_settlement = app_persisting_poll_settlement;
}

static bool app_persisting_state_machine_wait(
    app_persisting_state_machine_t *state_machine)
{
    return state_machine != NULL && state_machine->executor != NULL &&
           salts_coro_executor_wait(state_machine->executor) == SALTS_OK;
}

static bool app_persisting_state_machine_destroy(
    app_persisting_state_machine_t *state_machine)
{
    int shutdown_status;
    int wait_status;
    int destroy_status;

    if (state_machine == NULL || state_machine->executor == NULL ||
        state_machine->in_flight) {
        return false;
    }
    shutdown_status = salts_coro_executor_shutdown(state_machine->executor);
    wait_status = salts_coro_executor_wait(state_machine->executor);
    destroy_status = salts_coro_executor_destroy(state_machine->executor);
    state_machine->store->persistence_executor = NULL;
    state_machine->executor = NULL;
    return shutdown_status == SALTS_OK && wait_status == SALTS_OK &&
           destroy_status == SALTS_OK;
}

static bool app_add_command(void *user,
                            cflow_statechart_action_phase phase,
                            cflow_machine_state_id owner,
                            const void *state,
                            const cflow_event_view *event,
                            void *out_state,
                            cflow_statechart_raise_fn raise_internal,
                            void *raise_user,
                            const char **out_error)
{
    const app_command_event_t *command;

    (void) user;
    (void) owner;
    (void) raise_internal;
    (void) raise_user;
    if (phase != CFLOW_STATECHART_ACTION_TRANSITION || state == NULL ||
        event == NULL || out_state == NULL ||
        out_error == NULL || event->payload == NULL ||
        !cmeta_type_equal(event->payload_type, &app_command_event_type)) {
        return false;
    }
    command = (const app_command_event_t *) event->payload;
    *(int *) out_state = *(const int *) state + command->delta;
    *out_error = NULL;
    return true;
}

static int app_decode_entry(void *context,
                            const tr_raft_entry_t *entry,
                            cflow_event_view *out_event)
{
    app_decoder_t *decoder = (app_decoder_t *) context;

    if (decoder == NULL || entry == NULL || out_event == NULL ||
        entry->data_length != sizeof(int)) {
        return SALTS_EINVAL;
    }
    memset(&decoder->event, 0, sizeof(decoder->event));
    decoder->event.index = entry->index;
    decoder->event.term = entry->term;
    decoder->event.command_id = entry->command_id;
    decoder->event.payload_size = entry->data_length;
    memcpy(&decoder->event.delta, entry->data, sizeof(int));
    memcpy(decoder->event.payload, entry->data, entry->data_length);
    out_event->id = APP_EVENT;
    out_event->payload_type = &app_command_event_type;
    out_event->payload = &decoder->event;
    return SALTS_OK;
}

static tr_raft_entry_t app_entry(uint64_t index, uint64_t term,
                                 uint64_t command_id, int delta)
{
    tr_raft_entry_t entry;

    memset(&entry, 0, sizeof(entry));
    entry.index = index;
    entry.term = term;
    entry.command_id = command_id;
    entry.data_length = sizeof(delta);
    memcpy(entry.data, &delta, sizeof(delta));
    return entry;
}

static bool app_identity_matches(const app_store_snapshot_t *snapshot,
                                 const tr_raft_entry_t *entry)
{
    return snapshot != NULL && entry != NULL && snapshot->has_identity &&
           snapshot->identity.index == entry->index &&
           snapshot->identity.term == entry->term &&
           snapshot->identity.command_id == entry->command_id &&
           snapshot->identity.payload_size == entry->data_length &&
           memcmp(snapshot->identity.payload, entry->data,
                  entry->data_length) == 0;
}

static int app_create_core(const tr_raft_entry_t *entries,
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

static bool app_instance_init(
    app_cflow_fixture_t *fixture,
    cflow_statechart *statechart,
    cflow_executor *executor,
    tr_raft_cflow_state_machine_t *adapter,
    const cflow_statechart_executable_binding *binding,
    int initial_state)
{
    cflow_statechart_instance_hooks hooks;
    cflow_statechart_instance_config config;
    void *hook_user = NULL;

    memset(fixture, 0, sizeof(*fixture));
    if (tr_raft_cflow_state_machine_hooks(adapter, &hooks, &hook_user) !=
        SALTS_OK) {
        return false;
    }
    memset(&config, 0, sizeof(config));
    config.statechart = statechart;
    config.initial_state = &initial_state;
    config.executables = binding;
    config.executable_count = 1U;
    config.external_event_capacity = 1U;
    config.internal_event_capacity = 1U;
    config.completion_capacity = 1U;
    config.microstep_limit = 8U;
    config.executor = executor;
    config.hooks = &hooks;
    config.hook_user = hook_user;
    if (cflow_statechart_instance_init(&fixture->instance, &config) !=
        CFLOW_STATECHART_INSTANCE_OK) {
        return false;
    }
    fixture->initialized = true;
    if (tr_raft_cflow_state_machine_bind(adapter, &fixture->instance) !=
        SALTS_OK) {
        cflow_statechart_instance_close(&fixture->instance);
        (void) cflow_executor_wait_idle(executor);
        (void) cflow_statechart_instance_destroy(&fixture->instance);
        fixture->initialized = false;
        return false;
    }
    return true;
}

static bool app_instance_destroy(app_cflow_fixture_t *fixture,
                                 cflow_executor *executor,
                                 tr_raft_cflow_state_machine_t *adapter)
{
    if (fixture == NULL || !fixture->initialized) {
        return false;
    }
    cflow_statechart_instance_close(&fixture->instance);
    if (!cflow_executor_wait_idle(executor) ||
        cflow_statechart_instance_destroy(&fixture->instance) !=
            CFLOW_STATECHART_INSTANCE_OK ||
        tr_raft_cflow_state_machine_unbind(adapter, &fixture->instance) !=
            SALTS_OK) {
        return false;
    }
    fixture->initialized = false;
    return true;
}

spec("application-owned CFlow SQLite recovery")
{
    it("atomically rolls back or explicitly reconciles an exact durable entry")
    {
        static const cflow_statechart_state states[] = {
            {APP_ROOT, 0U, CFLOW_STATECHART_COMPOUND, 0U},
            {APP_INITIAL, APP_ROOT, CFLOW_STATECHART_INITIAL, 1U},
            {APP_ACTIVE, APP_ROOT, CFLOW_STATECHART_ATOMIC, 2U}};
        static const cflow_event_type events[] = {
            {APP_EVENT, &app_command_event_type}};
        static const cflow_statechart_executable executables[] = {{
            APP_EXECUTABLE, &cmeta_type_int, CMETA_EFFECT_MAY_FAIL,
            CMETA_PROP_DETERMINISTIC | CMETA_PROP_NO_ALIAS}};
        static const cflow_statechart_transition transitions[] = {
            {1U, APP_INITIAL, CFLOW_STATECHART_TRIGGER_EVENTLESS, 0U,
             0U, 0U, APP_ACTIVE, CFLOW_STATECHART_TRANSITION_EXTERNAL,
             0U, 0U},
            {2U, APP_ACTIVE, CFLOW_STATECHART_TRIGGER_EVENT, APP_EVENT,
             0U, 0U, APP_ACTIVE, CFLOW_STATECHART_TRANSITION_EXTERNAL,
             0U, 1U}};
        static const cflow_statechart_transition_action actions[] = {
            {2U, APP_EXECUTABLE, 0U}};
        const cflow_statechart_definition definition = {
            &cmeta_type_int, states, 3U, events, 1U, NULL, 0U,
            executables, 1U, transitions, 2U, NULL, 0U, actions, 1U};
        const cflow_statechart_executable_binding binding = {
            APP_EXECUTABLE, app_add_command, NULL, NULL};
        tr_raft_entry_t entries[] = {
            app_entry(1U, 1U, 101U, 7),
            app_entry(2U, 1U, 102U, 11)};
        char *database_path = tt_make_temp_file("turboraft-cflow", ".sqlite");
        app_store_t store = {0};
        app_store_snapshot_t snapshot;
        app_decoder_t decoder = {0};
        tr_raft_cflow_state_machine_config_v1_t adapter_config;
        tr_raft_cflow_state_machine_t *adapter = NULL;
        cflow_statechart statechart = {0};
        cflow_executor executor = {0};
        app_cflow_fixture_t instance = {0};
        tr_raft_entry_state_machine_v1_t state_machine;
        tr_raft_entry_state_machine_v1_t cflow_state_machine;
        app_persisting_state_machine_t persistence = {0};
        tr_raft_core_t *core = NULL;
        tr_raft_ready_t ready;
        tr_raft_apply_runtime_config_v1_t runtime_config;
        tr_raft_apply_runtime_t *runtime = NULL;
        tr_raft_apply_runtime_result_t result;
        tr_raft_status_t core_status;

        check_not_null(database_path);
        check_true(app_store_open(&store, database_path));
        check_true(app_store_create_schema(&store));
        memset(&adapter_config, 0, sizeof(adapter_config));
        adapter_config.abi_version =
            TR_RAFT_CFLOW_STATE_MACHINE_CONFIG_ABI_V1;
        adapter_config.struct_size = sizeof(adapter_config);
        adapter_config.decode_entry = app_decode_entry;
        adapter_config.decode_context = &decoder;
        check_equal(tr_raft_cflow_state_machine_create(&adapter_config,
                                                       &adapter),
                    SALTS_OK);
        check_equal(cflow_statechart_build(&statechart, &definition),
                    CFLOW_STATECHART_OK);
        check_true(cflow_executor_serial_init_with_capacity(&executor, 4U));
        check_true(app_instance_init(&instance, &statechart, &executor,
                                     adapter, &binding, 0));
        check_equal(tr_raft_cflow_state_machine_get_spi(adapter,
                                                        &cflow_state_machine),
                    SALTS_OK);
        check_true(app_persisting_state_machine_init(
            &persistence, &cflow_state_machine, &instance.instance, &store));
        app_persisting_state_machine_get_spi(&persistence, &state_machine);
        check_equal(app_create_core(entries, 2U, &core), SALTS_OK);
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

        store.failure = APP_STORE_FAIL_BEFORE_COMMIT;
        store.failure_index = 1U;
        check_equal(tr_raft_apply_runtime_start(runtime, &ready, &result),
                    SALTS_OK);
        check_true(cflow_executor_wait_idle(&executor));
        check_equal(tr_raft_apply_runtime_poll(runtime, &result), SALTS_OK);
        check_true(app_persisting_state_machine_wait(&persistence));
        check_equal(tr_raft_apply_runtime_poll(runtime, &result), SALTS_EIO);
        check_equal(result.state, TR_RAFT_APPLY_RUNTIME_FAULTED);
        check_equal(result.in_flight_token, 1U);
        check_equal(tr_raft_core_status(core, &core_status), SALTS_OK);
        check_equal(core_status.applied_index, 0U);

        check_true(app_instance_destroy(&instance, &executor, adapter));
        app_store_close(&store);
        check_true(app_store_open(&store, database_path));
        check_true(app_store_read_snapshot(&store, &snapshot));
        check_equal(snapshot.value, 0);
        check_equal(snapshot.applied_index, 0U);
        check_equal(snapshot.journal_rows, 0U);
        check_false(snapshot.has_identity);

        check_true(app_instance_init(&instance, &statechart, &executor,
                                     adapter, &binding, snapshot.value));
        store.failure = APP_STORE_FAIL_AFTER_COMMIT;
        store.failure_index = 2U;
        check_equal(tr_raft_apply_runtime_reconcile(
                        runtime, &entries[0], TR_RAFT_APPLY_OUTCOME_PENDING,
                        &result),
                    SALTS_OK);
        check_equal(result.state,
                    TR_RAFT_APPLY_RUNTIME_WAITING_SETTLEMENT);
        check_equal(result.in_flight_token, 1U);
        check_true(cflow_executor_wait_idle(&executor));
        check_equal(tr_raft_apply_runtime_poll(runtime, &result), SALTS_OK);
        check_true(app_persisting_state_machine_wait(&persistence));
        check_equal(tr_raft_apply_runtime_poll(runtime, &result), SALTS_OK);
        check_equal(result.state,
                    TR_RAFT_APPLY_RUNTIME_WAITING_SETTLEMENT);
        check_equal(result.applied_through, 1U);
        check_equal(result.in_flight_token, 2U);
        check_true(cflow_executor_wait_idle(&executor));
        check_equal(tr_raft_apply_runtime_poll(runtime, &result), SALTS_OK);
        check_true(app_persisting_state_machine_wait(&persistence));
        check_equal(tr_raft_apply_runtime_poll(runtime, &result), SALTS_EIO);
        check_equal(result.state, TR_RAFT_APPLY_RUNTIME_FAULTED);
        check_equal(result.in_flight_token, 2U);
        check_equal(tr_raft_core_status(core, &core_status), SALTS_OK);
        check_equal(core_status.applied_index, 1U);

        check_true(app_instance_destroy(&instance, &executor, adapter));
        app_store_close(&store);
        check_true(app_store_open(&store, database_path));
        check_true(app_store_read_snapshot(&store, &snapshot));
        check_equal(snapshot.value, 18);
        check_equal(snapshot.applied_index, 2U);
        check_equal(snapshot.journal_rows, 2U);
        check_true(app_identity_matches(&snapshot, &entries[1]));

        check_true(app_instance_init(&instance, &statechart, &executor,
                                     adapter, &binding, snapshot.value));
        check_equal(tr_raft_apply_runtime_reconcile(
                        runtime, &entries[1], TR_RAFT_APPLY_OUTCOME_APPLIED,
                        &result),
                    SALTS_OK);
        check_equal(result.state, TR_RAFT_APPLY_RUNTIME_COMPLETE);
        check_equal(result.applied_through, 2U);
        check_equal(tr_raft_core_status(core, &core_status), SALTS_OK);
        check_equal(core_status.applied_index, 2U);
        check_false(atomic_load_explicit(&store.wrong_executor,
                                         memory_order_acquire));

        check_equal(tr_raft_apply_runtime_destroy(runtime), SALTS_OK);
        tr_raft_core_destroy(core);
        check_true(app_instance_destroy(&instance, &executor, adapter));
        check_true(app_persisting_state_machine_destroy(&persistence));
        check_equal(tr_raft_cflow_state_machine_destroy(adapter), SALTS_OK);
        check_true(cflow_executor_shutdown(&executor));
        cflow_executor_destroy(&executor);
        cflow_statechart_destroy(&statechart);
        app_store_close(&store);
        check_equal(tt_remove_file(database_path), 0);
        free(database_path);
    }
}
