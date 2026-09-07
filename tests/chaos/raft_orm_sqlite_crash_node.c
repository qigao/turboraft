#include <orm.h>

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

enum {
    APP_CRASH_BEFORE_COMMIT = 41,
    APP_CRASH_AFTER_COMMIT = 42,
    APP_VALUE = 7
};

static orm_status_t app_execute(
    orm_connection_t *connection, orm_transaction_t *transaction,
    const char *sql, const orm_value_t *bindings, size_t binding_count,
    orm_result_t **out_result)
{
    orm_error_t error;
    orm_query_t *query = NULL;
    orm_result_t *result = NULL;
    orm_status_t status;
    size_t index;

    orm_error_init(&error);
    status = orm_raw(connection, orm_view(sql), &query, &error);
    for (index = 0U; status == ORM_STATUS_OK && index < binding_count;
         ++index) {
        status = orm_query_bind(query, bindings[index], &error);
    }
    if (status == ORM_STATUS_OK) {
        status = transaction == NULL
                     ? orm_query_execute(query, &result, &error)
                     : orm_query_execute_in_transaction(
                           query, transaction, &result, &error);
    }
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

static orm_connection_t *app_open(const char *path)
{
    orm_config_t config;
    orm_option_t option;
    orm_connection_t *connection = NULL;
    orm_error_t error;

    orm_error_init(&error);
    orm_config(&config);
    option.keyword = orm_view("filename");
    option.value = orm_view(path);
    config.driver = orm_view("sqlite");
    config.options = &option;
    config.option_count = 1U;
    if (orm_connect(&config, &connection, &error) != ORM_STATUS_OK) {
        return NULL;
    }
    return connection;
}

static int app_init(orm_connection_t *connection)
{
    static const char state_schema[] =
        "create table app_state(singleton integer primary key,"
        "value integer not null,applied_index integer not null)";
    static const char state_seed[] =
        "insert into app_state(singleton,value,applied_index) values(1,0,0)";
    static const char journal_schema[] =
        "create table raft_journal(log_index integer primary key,"
        "term integer not null,command_id integer not null,payload blob not null)";

    return app_execute(connection, NULL, state_schema, NULL, 0U, NULL) ==
               ORM_STATUS_OK &&
           app_execute(connection, NULL, state_seed, NULL, 0U, NULL) ==
               ORM_STATUS_OK &&
           app_execute(connection, NULL, journal_schema, NULL, 0U, NULL) ==
               ORM_STATUS_OK
               ? EXIT_SUCCESS
               : EXIT_FAILURE;
}

static int app_crash(orm_connection_t *connection, int after_commit)
{
    static const char journal_sql[] =
        "insert into raft_journal(log_index,term,command_id,payload) "
        "values(?1,?2,?3,?4)";
    static const char state_sql[] =
        "update app_state set value=?1,applied_index=?2 "
        "where singleton=1 and applied_index=0";
    orm_transaction_t *transaction = NULL;
    orm_value_t journal[4];
    orm_value_t state[2];
    orm_error_t error;
    int payload = APP_VALUE;

    orm_error_init(&error);
    if (orm_transaction_begin(connection, ORM_ISOLATION_SERIALIZABLE,
                              &transaction, &error) != ORM_STATUS_OK) {
        return EXIT_FAILURE;
    }
    journal[0] = orm_i64(1);
    journal[1] = orm_i64(1);
    journal[2] = orm_i64(101);
    journal[3] = orm_blob(&payload, sizeof(payload));
    state[0] = orm_i64(APP_VALUE);
    state[1] = orm_i64(1);
    if (app_execute(connection, transaction, journal_sql, journal, 4U,
                    NULL) != ORM_STATUS_OK ||
        app_execute(connection, transaction, state_sql, state, 2U,
                    NULL) != ORM_STATUS_OK) {
        return EXIT_FAILURE;
    }
    if (!after_commit) {
        _Exit(APP_CRASH_BEFORE_COMMIT);
    }
    if (orm_transaction_commit(transaction, &error) != ORM_STATUS_OK) {
        return EXIT_FAILURE;
    }
    _Exit(APP_CRASH_AFTER_COMMIT);
}

static int app_read_i64(const orm_result_t *result, uint64_t row,
                        uint64_t column, int64_t *out)
{
    orm_error_t error;

    orm_error_init(&error);
    return orm_result_get_int64(result, row, column, out, &error) ==
           ORM_STATUS_OK;
}

static int app_verify(orm_connection_t *connection, int applied_expected)
{
    static const char state_sql[] =
        "select value,applied_index from app_state where singleton=1";
    static const char journal_sql[] =
        "select log_index,term,command_id,payload from raft_journal";
    orm_result_t *state = NULL;
    orm_result_t *journal = NULL;
    orm_blob_t payload = {0};
    orm_error_t error;
    uint64_t rows = 0U;
    int64_t value = -1;
    int64_t index = -1;
    int64_t term = -1;
    int64_t command_id = -1;
    int payload_value = 0;
    int ok;

    orm_error_init(&error);
    ok = app_execute(connection, NULL, state_sql, NULL, 0U, &state) ==
             ORM_STATUS_OK &&
         app_read_i64(state, 0U, 0U, &value) &&
         app_read_i64(state, 0U, 1U, &index) &&
         value == (applied_expected ? APP_VALUE : 0) &&
         index == (applied_expected ? 1 : 0);
    orm_result_destroy(state);
    if (!ok ||
        app_execute(connection, NULL, journal_sql, NULL, 0U, &journal) !=
            ORM_STATUS_OK ||
        orm_result_row_count(journal, &rows, &error) != ORM_STATUS_OK ||
        rows != (applied_expected ? 1U : 0U)) {
        orm_result_destroy(journal);
        return EXIT_FAILURE;
    }
    if (!applied_expected) {
        orm_result_destroy(journal);
        return EXIT_SUCCESS;
    }
    ok = app_read_i64(journal, 0U, 0U, &index) && index == 1 &&
         app_read_i64(journal, 0U, 1U, &term) && term == 1 &&
         app_read_i64(journal, 0U, 2U, &command_id) && command_id == 101 &&
         orm_result_get_blob(journal, 0U, 3U, &payload, &error) ==
             ORM_STATUS_OK &&
         payload.size == sizeof(payload_value);
    if (ok) {
        memcpy(&payload_value, payload.data, sizeof(payload_value));
        ok = payload_value == APP_VALUE;
    }
    orm_result_destroy(journal);
    return ok ? EXIT_SUCCESS : EXIT_FAILURE;
}

int main(int argc, char **argv)
{
    orm_connection_t *connection;
    int result = EXIT_FAILURE;

    if (argc != 3 || argv[1] == NULL || argv[2] == NULL) {
        return EXIT_FAILURE;
    }
    connection = app_open(argv[2]);
    if (connection == NULL) {
        return EXIT_FAILURE;
    }
    if (strcmp(argv[1], "init") == 0) {
        result = app_init(connection);
    } else if (strcmp(argv[1], "crash-before-commit") == 0) {
        result = app_crash(connection, 0);
    } else if (strcmp(argv[1], "crash-after-commit") == 0) {
        result = app_crash(connection, 1);
    } else if (strcmp(argv[1], "verify-pending") == 0) {
        result = app_verify(connection, 0);
    } else if (strcmp(argv[1], "verify-applied") == 0) {
        result = app_verify(connection, 1);
    }
    orm_disconnect(connection);
    return result;
}
