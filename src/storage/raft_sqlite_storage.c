#include <turboraft/raft_sqlite_storage.h>

#include <sqlite3.h>
#include <turbo_error.h>

#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
    TR_RAFT_SQLITE_SCHEMA_VERSION = 3,
    TR_RAFT_SQLITE_DEFAULT_BUSY_TIMEOUT_MS = 5000,
    TR_RAFT_SQLITE_ERROR_TEXT_CAPACITY = 256
};

struct tr_raft_sqlite_storage {
    sqlite3 *database;
    int transaction_active;
    size_t max_snapshot_bytes;
    int last_sqlite_code;
    char last_error[TR_RAFT_SQLITE_ERROR_TEXT_CAPACITY];
};

static int tr_sqlite_map_error(int sqlite_code)
{
    switch (sqlite_code & 0xff) {
    case SQLITE_OK:
        return TURBO_OK;
    case SQLITE_BUSY:
    case SQLITE_LOCKED:
        return TURBO_EBUSY;
    case SQLITE_NOMEM:
        return TURBO_ENOMEM;
    case SQLITE_READONLY:
        return TURBO_EROFS;
    case SQLITE_CANTOPEN:
    case SQLITE_NOTFOUND:
        return TURBO_ENOENT;
    case SQLITE_FULL:
        return TURBO_ENOSPC;
    case SQLITE_TOOBIG:
    case SQLITE_RANGE:
        return TURBO_ERANGE;
    case SQLITE_CONSTRAINT:
    case SQLITE_MISMATCH:
    case SQLITE_MISUSE:
    case SQLITE_SCHEMA:
        return TURBO_EPROTO;
    default:
        return TURBO_EIO;
    }
}

static int tr_sqlite_record_error(tr_raft_sqlite_storage_t *storage,
                                  int sqlite_code,
                                  const char *operation)
{
    const char *detail = "SQLite operation failed";

    if (storage == NULL) {
        return tr_sqlite_map_error(sqlite_code);
    }
    storage->last_sqlite_code = sqlite_code;
    if (storage->database != NULL) {
        detail = sqlite3_errmsg(storage->database);
    }
    snprintf(storage->last_error, sizeof(storage->last_error), "%s: %s",
             operation, detail != NULL ? detail : "unknown SQLite error");
    return tr_sqlite_map_error(sqlite_code);
}

static void tr_sqlite_clear_error(tr_raft_sqlite_storage_t *storage)
{
    storage->last_sqlite_code = SQLITE_OK;
    storage->last_error[0] = '\0';
}

static int tr_sqlite_exec(tr_raft_sqlite_storage_t *storage,
                          const char *sql,
                          const char *operation)
{
    int sqlite_result = sqlite3_exec(storage->database, sql, NULL, NULL, NULL);

    if (sqlite_result != SQLITE_OK) {
        return tr_sqlite_record_error(storage, sqlite_result, operation);
    }
    tr_sqlite_clear_error(storage);
    return TURBO_OK;
}

static int tr_sqlite_prepare(tr_raft_sqlite_storage_t *storage,
                             const char *sql,
                             sqlite3_stmt **out_statement,
                             const char *operation)
{
    int sqlite_result = sqlite3_prepare_v2(storage->database, sql, -1,
                                           out_statement, NULL);

    if (sqlite_result != SQLITE_OK) {
        return tr_sqlite_record_error(storage, sqlite_result, operation);
    }
    return TURBO_OK;
}

static int tr_sqlite_step_done(tr_raft_sqlite_storage_t *storage,
                               sqlite3_stmt *statement,
                               const char *operation)
{
    int sqlite_result = sqlite3_step(statement);

    if (sqlite_result != SQLITE_DONE) {
        return tr_sqlite_record_error(storage, sqlite_result, operation);
    }
    tr_sqlite_clear_error(storage);
    return TURBO_OK;
}

static int tr_sqlite_get_user_version(tr_raft_sqlite_storage_t *storage,
                                      int *out_version)
{
    sqlite3_stmt *statement = NULL;
    int result = tr_sqlite_prepare(storage, "PRAGMA user_version;",
                                   &statement, "read schema version");

    if (result == TURBO_OK) {
        int sqlite_result = sqlite3_step(statement);
        if (sqlite_result == SQLITE_ROW) {
            *out_version = sqlite3_column_int(statement, 0);
        } else {
            result = tr_sqlite_record_error(storage, sqlite_result,
                                            "read schema version");
        }
    }
    sqlite3_finalize(statement);
    return result;
}

static int tr_sqlite_quick_check(tr_raft_sqlite_storage_t *storage)
{
    sqlite3_stmt *statement = NULL;
    const unsigned char *detail;
    int sqlite_result;
    int result = tr_sqlite_prepare(storage, "PRAGMA quick_check(1);",
                                   &statement, "prepare database quick check");

    if (result != TURBO_OK) {
        return result;
    }
    sqlite_result = sqlite3_step(statement);
    if (sqlite_result != SQLITE_ROW) {
        result = tr_sqlite_record_error(storage, sqlite_result,
                                        "run database quick check");
    } else {
        detail = sqlite3_column_text(statement, 0);
        if (detail == NULL || strcmp((const char *) detail, "ok") != 0) {
            storage->last_sqlite_code = SQLITE_CORRUPT;
            snprintf(storage->last_error, sizeof(storage->last_error),
                     "database quick check failed: %s",
                     detail != NULL ? (const char *) detail
                                    : "missing diagnostic");
            result = TURBO_EIO;
        } else {
            tr_sqlite_clear_error(storage);
        }
    }
    sqlite3_finalize(statement);
    return result;
}

static int tr_sqlite_create_schema(tr_raft_sqlite_storage_t *storage)
{
    static const char schema_sql[] =
        "BEGIN IMMEDIATE;"
        "CREATE TABLE raft_state("
        "singleton INTEGER PRIMARY KEY CHECK(singleton=1),"
        "current_term INTEGER NOT NULL CHECK(current_term>=0),"
        "voted_for INTEGER NOT NULL CHECK(voted_for>=0),"
        "commit_index INTEGER NOT NULL CHECK(commit_index>=0));"
        "INSERT INTO raft_state(singleton,current_term,voted_for,commit_index)"
        " VALUES(1,0,0,0);"
        "CREATE TABLE raft_log("
        "log_index INTEGER PRIMARY KEY CHECK(log_index>0),"
        "term INTEGER NOT NULL CHECK(term>=0),"
        "command_id INTEGER NOT NULL CHECK(command_id>=0),"
        "data BLOB NOT NULL);"
        "CREATE TABLE raft_snapshot("
        "singleton INTEGER PRIMARY KEY CHECK(singleton=1),"
        "last_included_index INTEGER NOT NULL CHECK(last_included_index>=0),"
        "last_included_term INTEGER NOT NULL CHECK(last_included_term>=0),"
        "data BLOB NOT NULL,"
        "configuration BLOB NOT NULL);"
        "INSERT INTO raft_snapshot(singleton,last_included_index,"
        "last_included_term,data,configuration) VALUES(1,0,0,x'',x'');"
        "PRAGMA user_version=3;"
        "COMMIT;";

    return tr_sqlite_exec(storage, schema_sql, "create schema");
}

static int tr_sqlite_migrate_v1_to_v2(
    tr_raft_sqlite_storage_t *storage)
{
    static const char migration_sql[] =
        "BEGIN IMMEDIATE;"
        "CREATE TABLE raft_snapshot("
        "singleton INTEGER PRIMARY KEY CHECK(singleton=1),"
        "last_included_index INTEGER NOT NULL CHECK(last_included_index>=0),"
        "last_included_term INTEGER NOT NULL CHECK(last_included_term>=0),"
        "data BLOB NOT NULL);"
        "INSERT INTO raft_snapshot(singleton,last_included_index,"
        "last_included_term,data) VALUES(1,0,0,x'');"
        "PRAGMA user_version=2;"
        "COMMIT;";

    return tr_sqlite_exec(storage, migration_sql,
                          "migrate schema version 1 to 2");
}

static int tr_sqlite_migrate_v2_to_v3(
    tr_raft_sqlite_storage_t *storage)
{
    static const char migration_sql[] =
        "BEGIN IMMEDIATE;"
        "ALTER TABLE raft_snapshot ADD COLUMN configuration "
        "BLOB NOT NULL DEFAULT x'';"
        "PRAGMA user_version=3;"
        "COMMIT;";

    return tr_sqlite_exec(storage, migration_sql,
                          "migrate schema version 2 to 3");
}

static int tr_sqlite_validate_schema(tr_raft_sqlite_storage_t *storage)
{
    sqlite3_stmt *statement = NULL;
    int result = tr_sqlite_prepare(
        storage,
        "SELECT current_term,voted_for,commit_index,last_included_index,"
        "last_included_term,length(configuration) "
        "FROM raft_state CROSS JOIN raft_snapshot "
        "WHERE raft_state.singleton=1 AND raft_snapshot.singleton=1;",
        &statement, "validate schema");

    if (result == TURBO_OK) {
        int sqlite_result = sqlite3_step(statement);
        if (sqlite_result != SQLITE_ROW) {
            if (sqlite_result == SQLITE_DONE) {
                result = TURBO_EPROTO;
                snprintf(storage->last_error, sizeof(storage->last_error),
                         "validate schema: missing raft_state row");
            } else {
                result = tr_sqlite_record_error(storage, sqlite_result,
                                                "validate schema");
            }
        }
    }
    sqlite3_finalize(statement);
    return result;
}

int tr_raft_sqlite_storage_open(
    const tr_raft_sqlite_storage_config_t *config,
    tr_raft_sqlite_storage_t **out_storage)
{
    tr_raft_sqlite_storage_t *storage;
    int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_FULLMUTEX;
    int timeout_ms;
    int version = 0;
    int sqlite_result;
    int result;

    if (out_storage == NULL) {
        return TURBO_EINVAL;
    }
    *out_storage = NULL;
    if (config == NULL || config->path == NULL || config->path[0] == '\0' ||
        config->busy_timeout_ms < 0 || config->max_snapshot_bytes == 0U ||
        config->max_snapshot_bytes > TR_RAFT_SQLITE_MAX_SNAPSHOT_BYTES) {
        return TURBO_EINVAL;
    }
    if (config->create_if_missing) {
        flags |= SQLITE_OPEN_CREATE;
    }
    storage = (tr_raft_sqlite_storage_t *) calloc(1U, sizeof(*storage));
    if (storage == NULL) {
        return TURBO_ENOMEM;
    }
    storage->max_snapshot_bytes = config->max_snapshot_bytes;
    sqlite_result = sqlite3_open_v2(config->path, &storage->database, flags,
                                    NULL);
    if (sqlite_result != SQLITE_OK) {
        result = tr_sqlite_record_error(storage, sqlite_result,
                                        "open database");
        sqlite3_close(storage->database);
        free(storage);
        return result;
    }
    sqlite3_extended_result_codes(storage->database, 1);
    timeout_ms = config->busy_timeout_ms != 0
                     ? config->busy_timeout_ms
                     : TR_RAFT_SQLITE_DEFAULT_BUSY_TIMEOUT_MS;
    sqlite_result = sqlite3_busy_timeout(storage->database, timeout_ms);
    if (sqlite_result != SQLITE_OK) {
        result = tr_sqlite_record_error(storage, sqlite_result,
                                        "configure busy timeout");
        goto fail;
    }
    result = tr_sqlite_exec(storage,
                            "PRAGMA trusted_schema=OFF;"
                            "PRAGMA journal_mode=WAL;"
                            "PRAGMA synchronous=FULL;",
                            "configure durability");
    if (result != TURBO_OK) {
        goto fail;
    }
    result = tr_sqlite_quick_check(storage);
    if (result != TURBO_OK) {
        goto fail;
    }
    result = tr_sqlite_get_user_version(storage, &version);
    if (result != TURBO_OK) {
        goto fail;
    }
    if (version == 0 && config->create_if_missing) {
        result = tr_sqlite_create_schema(storage);
    } else if (version == 1) {
        result = tr_sqlite_migrate_v1_to_v2(storage);
        if (result == TURBO_OK) {
            result = tr_sqlite_migrate_v2_to_v3(storage);
        }
    } else if (version == 2) {
        result = tr_sqlite_migrate_v2_to_v3(storage);
    } else if (version != TR_RAFT_SQLITE_SCHEMA_VERSION) {
        result = TURBO_EPROTO;
        snprintf(storage->last_error, sizeof(storage->last_error),
                 "unsupported schema version: %d", version);
    }
    if (result == TURBO_OK) {
        result = tr_sqlite_validate_schema(storage);
    }
    if (result != TURBO_OK) {
        goto fail;
    }
    *out_storage = storage;
    return TURBO_OK;

fail:
    sqlite3_close(storage->database);
    free(storage);
    return result;
}

static int tr_sqlite_require_transaction(
    tr_raft_sqlite_storage_t *storage)
{
    if (storage == NULL) {
        return TURBO_EINVAL;
    }
    return storage->transaction_active ? TURBO_OK : TURBO_EPROTO;
}

static int tr_sqlite_begin(void *context)
{
    tr_raft_sqlite_storage_t *storage =
        (tr_raft_sqlite_storage_t *) context;
    int result;

    if (storage == NULL) {
        return TURBO_EINVAL;
    }
    if (storage->transaction_active) {
        return TURBO_EBUSY;
    }
    result = tr_sqlite_exec(storage, "BEGIN IMMEDIATE;",
                            "begin transaction");
    if (result == TURBO_OK) {
        storage->transaction_active = 1;
    }
    return result;
}

static int tr_sqlite_write_hard_state(void *context,
                                      tr_raft_term_t term,
                                      tr_raft_node_id_t voted_for)
{
    tr_raft_sqlite_storage_t *storage =
        (tr_raft_sqlite_storage_t *) context;
    sqlite3_stmt *statement = NULL;
    int result = tr_sqlite_require_transaction(storage);

    if (result != TURBO_OK) {
        return result;
    }
    if (term > INT64_MAX || voted_for > INT64_MAX) {
        return TURBO_ERANGE;
    }
    result = tr_sqlite_prepare(
        storage,
        "UPDATE raft_state SET current_term=?1,voted_for=?2 "
        "WHERE singleton=1;",
        &statement, "prepare hard state");
    if (result == TURBO_OK) {
        int bind_result = sqlite3_bind_int64(statement, 1,
                                             (sqlite3_int64) term);
        if (bind_result == SQLITE_OK) {
            bind_result = sqlite3_bind_int64(statement, 2,
                                             (sqlite3_int64) voted_for);
        }
        if (bind_result != SQLITE_OK) {
            result = tr_sqlite_record_error(storage, bind_result,
                                            "bind hard state");
        }
    }
    if (result == TURBO_OK) {
        result = tr_sqlite_step_done(storage, statement, "write hard state");
    }
    sqlite3_finalize(statement);
    return result;
}

static int tr_sqlite_truncate_log(void *context,
                                  tr_raft_index_t from_index)
{
    tr_raft_sqlite_storage_t *storage =
        (tr_raft_sqlite_storage_t *) context;
    sqlite3_stmt *statement = NULL;
    sqlite3_int64 snapshot_index = 0;
    int result = tr_sqlite_require_transaction(storage);

    if (result != TURBO_OK) {
        return result;
    }
    if (from_index == 0U || from_index > INT64_MAX) {
        return TURBO_ERANGE;
    }
    result = tr_sqlite_prepare(
        storage,
        "SELECT last_included_index FROM raft_snapshot WHERE singleton=1;",
        &statement, "prepare snapshot boundary");
    if (result == TURBO_OK) {
        int sqlite_result = sqlite3_step(statement);
        if (sqlite_result == SQLITE_ROW) {
            snapshot_index = sqlite3_column_int64(statement, 0);
        } else {
            result = tr_sqlite_record_error(storage, sqlite_result,
                                            "read snapshot boundary");
        }
    }
    sqlite3_finalize(statement);
    statement = NULL;
    if (result != TURBO_OK) {
        return result;
    }
    if (snapshot_index < 0 || from_index <= (tr_raft_index_t) snapshot_index) {
        return TURBO_EPROTO;
    }
    result = tr_sqlite_prepare(storage,
                               "DELETE FROM raft_log WHERE log_index>=?1;",
                               &statement, "prepare log truncation");
    if (result == TURBO_OK) {
        int bind_result = sqlite3_bind_int64(
            statement, 1, (sqlite3_int64) from_index);
        if (bind_result != SQLITE_OK) {
            result = tr_sqlite_record_error(storage, bind_result,
                                            "bind log truncation");
        }
    }
    if (result == TURBO_OK) {
        result = tr_sqlite_step_done(storage, statement, "truncate log");
    }
    sqlite3_finalize(statement);
    return result;
}

static int tr_sqlite_last_log_index(tr_raft_sqlite_storage_t *storage,
                                    tr_raft_index_t *out_index)
{
    sqlite3_stmt *statement = NULL;
    int result = tr_sqlite_prepare(
        storage,
        "SELECT MAX(last_index) FROM ("
        "SELECT COALESCE(MAX(log_index),0) AS last_index FROM raft_log "
        "UNION ALL SELECT last_included_index AS last_index "
        "FROM raft_snapshot WHERE singleton=1);",
        &statement, "prepare last log index");

    if (result == TURBO_OK) {
        int sqlite_result = sqlite3_step(statement);
        if (sqlite_result == SQLITE_ROW) {
            sqlite3_int64 value = sqlite3_column_int64(statement, 0);
            if (value < 0) {
                result = TURBO_EPROTO;
            } else {
                *out_index = (tr_raft_index_t) value;
            }
        } else {
            result = tr_sqlite_record_error(storage, sqlite_result,
                                            "read last log index");
        }
    }
    sqlite3_finalize(statement);
    return result;
}

static int tr_sqlite_append_log(void *context,
                                const tr_raft_entry_t *entries,
                                size_t entry_count)
{
    tr_raft_sqlite_storage_t *storage =
        (tr_raft_sqlite_storage_t *) context;
    sqlite3_stmt *statement = NULL;
    tr_raft_index_t expected_index;
    size_t index;
    int result = tr_sqlite_require_transaction(storage);

    if (result != TURBO_OK) {
        return result;
    }
    if (entry_count != 0U && entries == NULL) {
        return TURBO_EINVAL;
    }
    if (entry_count == 0U) {
        return TURBO_OK;
    }
    result = tr_sqlite_last_log_index(storage, &expected_index);
    if (result != TURBO_OK || expected_index == UINT64_MAX) {
        return result != TURBO_OK ? result : TURBO_ERANGE;
    }
    ++expected_index;
    result = tr_sqlite_prepare(
        storage,
        "INSERT INTO raft_log(log_index,term,command_id,data)"
        " VALUES(?1,?2,?3,?4);",
        &statement, "prepare log append");
    for (index = 0U; result == TURBO_OK && index < entry_count; ++index) {
        const tr_raft_entry_t *entry = &entries[index];
        int bind_result;

        if (entry->index != expected_index || entry->index > INT64_MAX ||
            entry->term > INT64_MAX || entry->command_id > INT64_MAX ||
            entry->data_length > TR_RAFT_MAX_ENTRY_BYTES ||
            entry->data_length > INT_MAX) {
            result = TURBO_ERANGE;
            break;
        }
        bind_result = sqlite3_bind_int64(statement, 1,
                                         (sqlite3_int64) entry->index);
        if (bind_result == SQLITE_OK) {
            bind_result = sqlite3_bind_int64(statement, 2,
                                             (sqlite3_int64) entry->term);
        }
        if (bind_result == SQLITE_OK) {
            bind_result = sqlite3_bind_int64(
                statement, 3, (sqlite3_int64) entry->command_id);
        }
        if (bind_result == SQLITE_OK) {
            bind_result = sqlite3_bind_blob(statement, 4, entry->data,
                                             (int) entry->data_length,
                                             SQLITE_TRANSIENT);
        }
        if (bind_result != SQLITE_OK) {
            result = tr_sqlite_record_error(storage, bind_result,
                                            "bind log entry");
            break;
        }
        result = tr_sqlite_step_done(storage, statement, "append log entry");
        if (result == TURBO_OK && index + 1U < entry_count) {
            sqlite3_reset(statement);
            sqlite3_clear_bindings(statement);
            ++expected_index;
        }
    }
    sqlite3_finalize(statement);
    return result;
}

static int tr_sqlite_write_commit_index(void *context,
                                        tr_raft_index_t commit_index)
{
    tr_raft_sqlite_storage_t *storage =
        (tr_raft_sqlite_storage_t *) context;
    sqlite3_stmt *statement = NULL;
    tr_raft_index_t last_log_index = 0U;
    sqlite3_int64 snapshot_index = 0;
    int result = tr_sqlite_require_transaction(storage);

    if (result != TURBO_OK) {
        return result;
    }
    if (commit_index > INT64_MAX) {
        return TURBO_ERANGE;
    }
    result = tr_sqlite_last_log_index(storage, &last_log_index);
    if (result != TURBO_OK) {
        return result;
    }
    if (commit_index > last_log_index) {
        return TURBO_EPROTO;
    }
    result = tr_sqlite_prepare(
        storage,
        "SELECT last_included_index FROM raft_snapshot WHERE singleton=1;",
        &statement, "prepare commit snapshot boundary");
    if (result == TURBO_OK) {
        int sqlite_result = sqlite3_step(statement);
        if (sqlite_result == SQLITE_ROW) {
            snapshot_index = sqlite3_column_int64(statement, 0);
        } else {
            result = tr_sqlite_record_error(storage, sqlite_result,
                                            "read commit snapshot boundary");
        }
    }
    sqlite3_finalize(statement);
    statement = NULL;
    if (result != TURBO_OK) {
        return result;
    }
    if (snapshot_index < 0 || commit_index < (tr_raft_index_t) snapshot_index) {
        return TURBO_EPROTO;
    }
    result = tr_sqlite_prepare(
        storage,
        "UPDATE raft_state SET commit_index=?1 WHERE singleton=1;",
        &statement, "prepare commit index");
    if (result == TURBO_OK) {
        int bind_result = sqlite3_bind_int64(
            statement, 1, (sqlite3_int64) commit_index);
        if (bind_result != SQLITE_OK) {
            result = tr_sqlite_record_error(storage, bind_result,
                                            "bind commit index");
        }
    }
    if (result == TURBO_OK) {
        result = tr_sqlite_step_done(storage, statement,
                                     "write commit index");
    }
    sqlite3_finalize(statement);
    return result;
}

static int tr_sqlite_commit(void *context)
{
    tr_raft_sqlite_storage_t *storage =
        (tr_raft_sqlite_storage_t *) context;
    int result = tr_sqlite_require_transaction(storage);

    if (result != TURBO_OK) {
        return result;
    }
    result = tr_sqlite_exec(storage, "COMMIT;", "commit transaction");
    if (result == TURBO_OK) {
        storage->transaction_active = 0;
    }
    return result;
}

static int tr_sqlite_rollback(void *context)
{
    tr_raft_sqlite_storage_t *storage =
        (tr_raft_sqlite_storage_t *) context;
    int result;

    if (storage == NULL) {
        return TURBO_EINVAL;
    }
    if (!storage->transaction_active ||
        sqlite3_get_autocommit(storage->database)) {
        storage->transaction_active = 0;
        return TURBO_OK;
    }
    result = tr_sqlite_exec(storage, "ROLLBACK;", "rollback transaction");
    if (result == TURBO_OK) {
        storage->transaction_active = 0;
    }
    return result;
}

int tr_raft_sqlite_storage_store_snapshot(
    tr_raft_sqlite_storage_t *storage,
    tr_raft_index_t last_included_index,
    tr_raft_term_t last_included_term,
    const tr_raft_conf_t *configuration,
    const void *data,
    size_t size)
{
    sqlite3_stmt *statement = NULL;
    sqlite3_int64 current_snapshot_index = 0;
    sqlite3_int64 commit_index = 0;
    sqlite3_int64 log_term = 0;
    uint8_t encoded_configuration[TR_RAFT_CONF_MAX_ENCODED_SIZE];
    size_t encoded_configuration_size = 0U;
    int result;

    if (storage == NULL || configuration == NULL ||
        last_included_index == 0U ||
        last_included_term == 0U || (size != 0U && data == NULL)) {
        return TURBO_EINVAL;
    }
    if (storage->transaction_active) {
        return TURBO_EBUSY;
    }
    if (last_included_index > INT64_MAX ||
        last_included_term > INT64_MAX || size > storage->max_snapshot_bytes ||
        size > INT_MAX) {
        return TURBO_ERANGE;
    }
    result = tr_raft_conf_encode(
        configuration, encoded_configuration,
        sizeof(encoded_configuration), &encoded_configuration_size);
    if (result != TURBO_OK) {
        return result;
    }
    result = tr_sqlite_begin(storage);
    if (result != TURBO_OK) {
        return result;
    }
    result = tr_sqlite_prepare(
        storage,
        "SELECT last_included_index FROM raft_snapshot WHERE singleton=1;",
        &statement, "prepare current snapshot");
    if (result == TURBO_OK) {
        int sqlite_result = sqlite3_step(statement);
        if (sqlite_result == SQLITE_ROW) {
            current_snapshot_index = sqlite3_column_int64(statement, 0);
        } else {
            result = tr_sqlite_record_error(storage, sqlite_result,
                                            "read current snapshot");
        }
    }
    sqlite3_finalize(statement);
    statement = NULL;
    if (result == TURBO_OK &&
        (current_snapshot_index < 0 ||
         last_included_index <= (tr_raft_index_t) current_snapshot_index)) {
        result = TURBO_EPROTO;
    }
    if (result == TURBO_OK) {
        result = tr_sqlite_prepare(
            storage,
            "SELECT commit_index FROM raft_state WHERE singleton=1;",
            &statement, "prepare snapshot commit boundary");
    }
    if (result == TURBO_OK) {
        int sqlite_result = sqlite3_step(statement);
        if (sqlite_result == SQLITE_ROW) {
            commit_index = sqlite3_column_int64(statement, 0);
        } else {
            result = tr_sqlite_record_error(storage, sqlite_result,
                                            "read snapshot commit boundary");
        }
    }
    sqlite3_finalize(statement);
    statement = NULL;
    if (result == TURBO_OK &&
        (commit_index < 0 ||
         last_included_index > (tr_raft_index_t) commit_index)) {
        result = TURBO_EPROTO;
    }
    if (result == TURBO_OK) {
        result = tr_sqlite_prepare(
            storage, "SELECT term FROM raft_log WHERE log_index=?1;",
            &statement, "prepare snapshot log term");
    }
    if (result == TURBO_OK) {
        int bind_result = sqlite3_bind_int64(
            statement, 1, (sqlite3_int64) last_included_index);
        if (bind_result != SQLITE_OK) {
            result = tr_sqlite_record_error(storage, bind_result,
                                            "bind snapshot log index");
        }
    }
    if (result == TURBO_OK) {
        int sqlite_result = sqlite3_step(statement);
        if (sqlite_result == SQLITE_ROW) {
            log_term = sqlite3_column_int64(statement, 0);
        } else if (sqlite_result == SQLITE_DONE) {
            result = TURBO_EPROTO;
        } else {
            result = tr_sqlite_record_error(storage, sqlite_result,
                                            "read snapshot log term");
        }
    }
    sqlite3_finalize(statement);
    statement = NULL;
    if (result == TURBO_OK &&
        (log_term < 0 || last_included_term != (tr_raft_term_t) log_term)) {
        result = TURBO_EPROTO;
    }
    if (result == TURBO_OK) {
        result = tr_sqlite_prepare(
            storage,
            "UPDATE raft_snapshot SET last_included_index=?1,"
            "last_included_term=?2,data=?3,configuration=?4 "
            "WHERE singleton=1;",
            &statement, "prepare snapshot write");
    }
    if (result == TURBO_OK) {
        int bind_result = sqlite3_bind_int64(
            statement, 1, (sqlite3_int64) last_included_index);
        if (bind_result == SQLITE_OK) {
            bind_result = sqlite3_bind_int64(
                statement, 2, (sqlite3_int64) last_included_term);
        }
        if (bind_result == SQLITE_OK) {
            bind_result = sqlite3_bind_blob(statement, 3,
                                             size != 0U ? data : "",
                                             (int) size, SQLITE_TRANSIENT);
        }
        if (bind_result == SQLITE_OK) {
            bind_result = sqlite3_bind_blob(
                statement, 4, encoded_configuration,
                (int) encoded_configuration_size, SQLITE_TRANSIENT);
        }
        if (bind_result != SQLITE_OK) {
            result = tr_sqlite_record_error(storage, bind_result,
                                            "bind snapshot write");
        }
    }
    if (result == TURBO_OK) {
        result = tr_sqlite_step_done(storage, statement, "write snapshot");
    }
    sqlite3_finalize(statement);
    statement = NULL;
    if (result == TURBO_OK) {
        result = tr_sqlite_prepare(
            storage, "DELETE FROM raft_log WHERE log_index<=?1;",
            &statement, "prepare snapshot log compaction");
    }
    if (result == TURBO_OK) {
        int bind_result = sqlite3_bind_int64(
            statement, 1, (sqlite3_int64) last_included_index);
        if (bind_result != SQLITE_OK) {
            result = tr_sqlite_record_error(storage, bind_result,
                                            "bind snapshot log compaction");
        }
    }
    if (result == TURBO_OK) {
        result = tr_sqlite_step_done(storage, statement,
                                     "compact snapshot log prefix");
    }
    sqlite3_finalize(statement);
    if (result == TURBO_OK) {
        result = tr_sqlite_commit(storage);
    }
    if (result != TURBO_OK) {
        int operation_result = result;
        tr_sqlite_rollback(storage);
        return operation_result;
    }
    return TURBO_OK;
}

int tr_raft_sqlite_storage_install_snapshot(
    tr_raft_sqlite_storage_t *storage,
    tr_raft_term_t leader_term,
    tr_raft_index_t last_included_index,
    tr_raft_term_t last_included_term,
    const tr_raft_conf_t *configuration,
    const void *data,
    size_t size)
{
    sqlite3_stmt *statement = NULL;
    sqlite3_int64 current_term = 0;
    sqlite3_int64 commit_index = 0;
    sqlite3_int64 current_snapshot_index = 0;
    sqlite3_int64 local_log_term = 0;
    int preserve_suffix = 0;
    uint8_t encoded_configuration[TR_RAFT_CONF_MAX_ENCODED_SIZE];
    size_t encoded_configuration_size = 0U;
    int result;

    if (storage == NULL || configuration == NULL || leader_term == 0U ||
        last_included_index == 0U || last_included_term == 0U ||
        last_included_term > leader_term || (size != 0U && data == NULL)) {
        return TURBO_EINVAL;
    }
    if (storage->transaction_active) {
        return TURBO_EBUSY;
    }
    if (leader_term > INT64_MAX || last_included_index > INT64_MAX ||
        last_included_term > INT64_MAX || size > storage->max_snapshot_bytes ||
        size > INT_MAX) {
        return TURBO_ERANGE;
    }
    result = tr_raft_conf_encode(
        configuration, encoded_configuration,
        sizeof(encoded_configuration), &encoded_configuration_size);
    if (result != TURBO_OK) {
        return result;
    }
    result = tr_sqlite_begin(storage);
    if (result != TURBO_OK) {
        return result;
    }
    result = tr_sqlite_prepare(
        storage,
        "SELECT current_term,commit_index,last_included_index "
        "FROM raft_state CROSS JOIN raft_snapshot "
        "WHERE raft_state.singleton=1 AND raft_snapshot.singleton=1;",
        &statement, "prepare remote snapshot state");
    if (result == TURBO_OK) {
        int sqlite_result = sqlite3_step(statement);
        if (sqlite_result == SQLITE_ROW) {
            current_term = sqlite3_column_int64(statement, 0);
            commit_index = sqlite3_column_int64(statement, 1);
            current_snapshot_index = sqlite3_column_int64(statement, 2);
        } else {
            result = tr_sqlite_record_error(storage, sqlite_result,
                                            "read remote snapshot state");
        }
    }
    sqlite3_finalize(statement);
    statement = NULL;
    if (result == TURBO_OK &&
        (current_term < 0 || commit_index < 0 || current_snapshot_index < 0 ||
         leader_term < (tr_raft_term_t) current_term ||
         last_included_index < (tr_raft_index_t) commit_index ||
         last_included_index <= (tr_raft_index_t) current_snapshot_index)) {
        result = TURBO_EPROTO;
    }
    if (result == TURBO_OK) {
        result = tr_sqlite_prepare(
            storage, "SELECT term FROM raft_log WHERE log_index=?1;",
            &statement, "prepare remote snapshot log match");
    }
    if (result == TURBO_OK) {
        int bind_result = sqlite3_bind_int64(
            statement, 1, (sqlite3_int64) last_included_index);
        if (bind_result != SQLITE_OK) {
            result = tr_sqlite_record_error(storage, bind_result,
                                            "bind remote snapshot log match");
        }
    }
    if (result == TURBO_OK) {
        int sqlite_result = sqlite3_step(statement);
        if (sqlite_result == SQLITE_ROW) {
            local_log_term = sqlite3_column_int64(statement, 0);
            preserve_suffix = local_log_term >= 0 &&
                last_included_term == (tr_raft_term_t) local_log_term;
        } else if (sqlite_result != SQLITE_DONE) {
            result = tr_sqlite_record_error(storage, sqlite_result,
                                            "read remote snapshot log match");
        }
    }
    sqlite3_finalize(statement);
    statement = NULL;
    if (result == TURBO_OK) {
        result = tr_sqlite_prepare(
            storage,
            "UPDATE raft_snapshot SET last_included_index=?1,"
            "last_included_term=?2,data=?3,configuration=?4 "
            "WHERE singleton=1;",
            &statement, "prepare remote snapshot write");
    }
    if (result == TURBO_OK) {
        int bind_result = sqlite3_bind_int64(
            statement, 1, (sqlite3_int64) last_included_index);
        if (bind_result == SQLITE_OK) {
            bind_result = sqlite3_bind_int64(
                statement, 2, (sqlite3_int64) last_included_term);
        }
        if (bind_result == SQLITE_OK) {
            bind_result = sqlite3_bind_blob(statement, 3,
                                             size != 0U ? data : "",
                                             (int) size, SQLITE_TRANSIENT);
        }
        if (bind_result == SQLITE_OK) {
            bind_result = sqlite3_bind_blob(
                statement, 4, encoded_configuration,
                (int) encoded_configuration_size, SQLITE_TRANSIENT);
        }
        if (bind_result != SQLITE_OK) {
            result = tr_sqlite_record_error(storage, bind_result,
                                            "bind remote snapshot write");
        }
    }
    if (result == TURBO_OK) {
        result = tr_sqlite_step_done(storage, statement,
                                     "write remote snapshot");
    }
    sqlite3_finalize(statement);
    statement = NULL;
    if (result == TURBO_OK) {
        result = tr_sqlite_prepare(
            storage,
            preserve_suffix
                ? "DELETE FROM raft_log WHERE log_index<=?1;"
                : "DELETE FROM raft_log;",
            &statement, "prepare remote snapshot log reconciliation");
    }
    if (result == TURBO_OK && preserve_suffix) {
        int bind_result = sqlite3_bind_int64(
            statement, 1, (sqlite3_int64) last_included_index);
        if (bind_result != SQLITE_OK) {
            result = tr_sqlite_record_error(
                storage, bind_result,
                "bind remote snapshot log reconciliation");
        }
    }
    if (result == TURBO_OK) {
        result = tr_sqlite_step_done(storage, statement,
                                     "reconcile remote snapshot log");
    }
    sqlite3_finalize(statement);
    statement = NULL;
    if (result == TURBO_OK) {
        result = tr_sqlite_prepare(
            storage,
            "UPDATE raft_state SET "
            "voted_for=CASE WHEN current_term<?1 THEN 0 ELSE voted_for END,"
            "current_term=?1,commit_index=?2 WHERE singleton=1;",
            &statement, "prepare remote snapshot hard state");
    }
    if (result == TURBO_OK) {
        int bind_result = sqlite3_bind_int64(
            statement, 1, (sqlite3_int64) leader_term);
        if (bind_result == SQLITE_OK) {
            bind_result = sqlite3_bind_int64(
                statement, 2, (sqlite3_int64) last_included_index);
        }
        if (bind_result != SQLITE_OK) {
            result = tr_sqlite_record_error(storage, bind_result,
                                            "bind remote snapshot hard state");
        }
    }
    if (result == TURBO_OK) {
        result = tr_sqlite_step_done(storage, statement,
                                     "write remote snapshot hard state");
    }
    sqlite3_finalize(statement);
    if (result == TURBO_OK) {
        result = tr_sqlite_commit(storage);
    }
    if (result != TURBO_OK) {
        int operation_result = result;
        tr_sqlite_rollback(storage);
        return operation_result;
    }
    return TURBO_OK;
}

int tr_raft_sqlite_storage_bind(tr_raft_sqlite_storage_t *storage,
                                tr_raft_storage_t *out_storage)
{
    if (storage == NULL || out_storage == NULL) {
        return TURBO_EINVAL;
    }
    memset(out_storage, 0, sizeof(*out_storage));
    out_storage->context = storage;
    out_storage->begin = tr_sqlite_begin;
    out_storage->write_hard_state = tr_sqlite_write_hard_state;
    out_storage->truncate_log = tr_sqlite_truncate_log;
    out_storage->append_log = tr_sqlite_append_log;
    out_storage->write_commit_index = tr_sqlite_write_commit_index;
    out_storage->commit = tr_sqlite_commit;
    out_storage->rollback = tr_sqlite_rollback;
    return TURBO_OK;
}

int tr_raft_sqlite_storage_load(
    tr_raft_sqlite_storage_t *storage,
    tr_raft_sqlite_recovery_t *out_recovery)
{
    sqlite3_stmt *statement = NULL;
    tr_raft_sqlite_recovery_t recovery;
    sqlite3_int64 count_value;
    sqlite3_int64 max_index;
    size_t index = 0U;
    int sqlite_result = SQLITE_DONE;
    int result;

    if (storage == NULL || out_recovery == NULL) {
        return TURBO_EINVAL;
    }
    if (storage->transaction_active) {
        return TURBO_EBUSY;
    }
    memset(out_recovery, 0, sizeof(*out_recovery));
    memset(&recovery, 0, sizeof(recovery));
    result = tr_sqlite_prepare(
        storage,
        "SELECT current_term,voted_for,commit_index FROM raft_state "
        "WHERE singleton=1;",
        &statement, "prepare recovery state");
    if (result != TURBO_OK) {
        return result;
    }
    sqlite_result = sqlite3_step(statement);
    if (sqlite_result != SQLITE_ROW || sqlite3_column_int64(statement, 0) < 0 ||
        sqlite3_column_int64(statement, 1) < 0 ||
        sqlite3_column_int64(statement, 2) < 0) {
        sqlite3_finalize(statement);
        return sqlite_result == SQLITE_ROW
                   ? TURBO_EPROTO
                   : tr_sqlite_record_error(storage, sqlite_result,
                                            "read recovery state");
    }
    recovery.term = (tr_raft_term_t) sqlite3_column_int64(statement, 0);
    recovery.voted_for =
        (tr_raft_node_id_t) sqlite3_column_int64(statement, 1);
    recovery.commit_index =
        (tr_raft_index_t) sqlite3_column_int64(statement, 2);
    sqlite3_finalize(statement);
    statement = NULL;

    result = tr_sqlite_prepare(
        storage,
        "SELECT last_included_index,last_included_term,data,configuration "
        "FROM raft_snapshot WHERE singleton=1;",
        &statement, "prepare recovery snapshot");
    if (result != TURBO_OK) {
        return result;
    }
    sqlite_result = sqlite3_step(statement);
    if (sqlite_result != SQLITE_ROW || sqlite3_column_int64(statement, 0) < 0 ||
        sqlite3_column_int64(statement, 1) < 0 ||
        sqlite3_column_bytes(statement, 2) < 0 ||
        sqlite3_column_bytes(statement, 3) < 0) {
        sqlite3_finalize(statement);
        return sqlite_result == SQLITE_ROW
                   ? TURBO_EPROTO
                   : tr_sqlite_record_error(storage, sqlite_result,
                                            "read recovery snapshot");
    }
    recovery.snapshot_index =
        (tr_raft_index_t) sqlite3_column_int64(statement, 0);
    recovery.snapshot_term =
        (tr_raft_term_t) sqlite3_column_int64(statement, 1);
    recovery.snapshot_size = (size_t) sqlite3_column_bytes(statement, 2);
    {
        int configuration_size = sqlite3_column_bytes(statement, 3);
        const void *configuration_data = sqlite3_column_blob(statement, 3);

        if ((configuration_size == 0 && configuration_data != NULL) ||
            (configuration_size != 0 && configuration_data == NULL) ||
            (size_t) configuration_size > TR_RAFT_CONF_MAX_ENCODED_SIZE ||
            (recovery.snapshot_index == 0U && configuration_size != 0)) {
            sqlite3_finalize(statement);
            return TURBO_EPROTO;
        }
        if (configuration_size != 0) {
            result = tr_raft_conf_decode(
                (const uint8_t *) configuration_data,
                (size_t) configuration_size,
                &recovery.snapshot_configuration);
            if (result != TURBO_OK) {
                sqlite3_finalize(statement);
                return TURBO_EPROTO;
            }
            recovery.has_snapshot_configuration = true;
        }
    }
    if ((recovery.snapshot_index == 0U) != (recovery.snapshot_term == 0U) ||
        recovery.snapshot_size > storage->max_snapshot_bytes ||
        recovery.commit_index < recovery.snapshot_index) {
        sqlite3_finalize(statement);
        return TURBO_EPROTO;
    }
    if (recovery.snapshot_size != 0U) {
        const void *snapshot_data = sqlite3_column_blob(statement, 2);
        if (snapshot_data == NULL) {
            sqlite3_finalize(statement);
            return TURBO_EPROTO;
        }
        recovery.snapshot_data = (uint8_t *) malloc(recovery.snapshot_size);
        if (recovery.snapshot_data == NULL) {
            sqlite3_finalize(statement);
            return TURBO_ENOMEM;
        }
        memcpy(recovery.snapshot_data, snapshot_data, recovery.snapshot_size);
    }
    sqlite3_finalize(statement);
    statement = NULL;

    result = tr_sqlite_prepare(
        storage,
        "SELECT COUNT(*),COALESCE(MAX(log_index),0) FROM raft_log;",
        &statement, "prepare recovery log count");
    if (result != TURBO_OK) {
        tr_raft_sqlite_recovery_destroy(&recovery);
        return result;
    }
    sqlite_result = sqlite3_step(statement);
    if (sqlite_result != SQLITE_ROW) {
        result = tr_sqlite_record_error(storage, sqlite_result,
                                        "read recovery log count");
        sqlite3_finalize(statement);
        tr_raft_sqlite_recovery_destroy(&recovery);
        return result;
    }
    count_value = sqlite3_column_int64(statement, 0);
    max_index = sqlite3_column_int64(statement, 1);
    sqlite3_finalize(statement);
    statement = NULL;
    if (count_value < 0 || max_index < 0 ||
        (uint64_t) count_value > SIZE_MAX ||
        (uint64_t) count_value > UINT64_MAX - recovery.snapshot_index ||
        (count_value == 0 && max_index != 0) ||
        (count_value != 0 &&
         (tr_raft_index_t) max_index !=
             recovery.snapshot_index + (tr_raft_index_t) count_value) ||
        recovery.commit_index >
            (count_value == 0 ? recovery.snapshot_index
                              : (tr_raft_index_t) max_index)) {
        tr_raft_sqlite_recovery_destroy(&recovery);
        return TURBO_EPROTO;
    }
    recovery.entry_count = (size_t) count_value;
    if (recovery.entry_count != 0U) {
        if (recovery.entry_count > SIZE_MAX / sizeof(recovery.entries[0])) {
            tr_raft_sqlite_recovery_destroy(&recovery);
            return TURBO_ERANGE;
        }
        recovery.entries = (tr_raft_entry_t *) calloc(
            recovery.entry_count, sizeof(recovery.entries[0]));
        if (recovery.entries == NULL) {
            tr_raft_sqlite_recovery_destroy(&recovery);
            return TURBO_ENOMEM;
        }
    }
    result = tr_sqlite_prepare(
        storage,
        "SELECT log_index,term,command_id,data FROM raft_log "
        "ORDER BY log_index;",
        &statement, "prepare recovery log");
    while (result == TURBO_OK &&
           (sqlite_result = sqlite3_step(statement)) == SQLITE_ROW) {
        sqlite3_int64 log_index = sqlite3_column_int64(statement, 0);
        sqlite3_int64 term = sqlite3_column_int64(statement, 1);
        sqlite3_int64 command_id = sqlite3_column_int64(statement, 2);
        const void *data = sqlite3_column_blob(statement, 3);
        int data_length = sqlite3_column_bytes(statement, 3);
        tr_raft_entry_t *entry;

        if (index >= recovery.entry_count ||
            log_index != (sqlite3_int64) recovery.snapshot_index +
                             (sqlite3_int64) index + 1 || term < 0 ||
            command_id < 0 || data_length < 0 ||
            (size_t) data_length > TR_RAFT_MAX_ENTRY_BYTES ||
            (data_length != 0 && data == NULL)) {
            result = TURBO_EPROTO;
            break;
        }
        entry = &recovery.entries[index];
        entry->index = (tr_raft_index_t) log_index;
        entry->term = (tr_raft_term_t) term;
        entry->command_id = (uint64_t) command_id;
        entry->data_length = (size_t) data_length;
        if (data_length != 0) {
            memcpy(entry->data, data, (size_t) data_length);
        }
        ++index;
    }
    if (result == TURBO_OK && sqlite_result != SQLITE_DONE) {
        result = tr_sqlite_record_error(storage, sqlite_result,
                                        "read recovery log");
    }
    if (result == TURBO_OK && index != recovery.entry_count) {
        result = TURBO_EPROTO;
    }
    sqlite3_finalize(statement);
    if (result != TURBO_OK) {
        tr_raft_sqlite_recovery_destroy(&recovery);
        return result;
    }
    tr_sqlite_clear_error(storage);
    *out_recovery = recovery;
    return TURBO_OK;
}

void tr_raft_sqlite_recovery_destroy(
    tr_raft_sqlite_recovery_t *recovery)
{
    if (recovery == NULL) {
        return;
    }
    free(recovery->snapshot_data);
    free(recovery->entries);
    memset(recovery, 0, sizeof(*recovery));
}

int tr_raft_sqlite_storage_close(tr_raft_sqlite_storage_t *storage)
{
    int result = TURBO_OK;
    int sqlite_result;

    if (storage == NULL) {
        return TURBO_OK;
    }
    if (storage->transaction_active) {
        result = tr_sqlite_rollback(storage);
    }
    sqlite_result = sqlite3_close(storage->database);
    if (result == TURBO_OK && sqlite_result != SQLITE_OK) {
        result = tr_sqlite_record_error(storage, sqlite_result,
                                        "close database");
    }
    if (sqlite_result == SQLITE_OK) {
        storage->database = NULL;
        free(storage);
    }
    return result;
}

int tr_raft_sqlite_storage_last_sqlite_code(
    const tr_raft_sqlite_storage_t *storage)
{
    return storage != NULL ? storage->last_sqlite_code : SQLITE_MISUSE;
}

const char *tr_raft_sqlite_storage_last_error(
    const tr_raft_sqlite_storage_t *storage)
{
    return storage != NULL ? storage->last_error : "invalid SQLite storage";
}
