#include <turboraft/raft_sqlite_storage.h>

#include <tinytest.h>
#include <turbo_error.h>
#include <sqlite3.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const tr_raft_conf_t sqlite_test_configuration = {
    TR_RAFT_CONF_FINAL,
    0U,
    1U,
    {{1U, TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER}}
};

enum {
    SQLITE_TEST_SOAK_CYCLE_COUNT = 64,
    SQLITE_TEST_SOAK_SNAPSHOT_INTERVAL = 8,
    SQLITE_TEST_SOAK_COMMAND_BASE = 10000
};

static tr_raft_entry_t sqlite_test_entry(tr_raft_index_t index,
                                         tr_raft_term_t term,
                                         uint64_t command_id,
                                         const char *data)
{
    tr_raft_entry_t entry;

    memset(&entry, 0, sizeof(entry));
    entry.index = index;
    entry.term = term;
    entry.command_id = command_id;
    entry.data_length = strlen(data);
    memcpy(entry.data, data, entry.data_length);
    return entry;
}

static tr_raft_sqlite_storage_t *sqlite_test_open(const char *path,
                                                   bool create)
{
    tr_raft_sqlite_storage_config_t config;
    tr_raft_sqlite_storage_t *storage = NULL;

    memset(&config, 0, sizeof(config));
    config.path = path;
    config.busy_timeout_ms = 1000;
    config.create_if_missing = create;
    config.max_snapshot_bytes = 1024U;
    check_int_eq(tr_raft_sqlite_storage_open(&config, &storage), TURBO_OK);
    check_not_null(storage);
    return storage;
}

static tr_raft_sqlite_storage_config_t sqlite_test_config(const char *path,
                                                          bool create)
{
    tr_raft_sqlite_storage_config_t config;

    memset(&config, 0, sizeof(config));
    config.path = path;
    config.busy_timeout_ms = 1000;
    config.create_if_missing = create;
    config.max_snapshot_bytes = 1024U;
    return config;
}

static void sqlite_test_exec_path(const char *path, const char *sql)
{
    sqlite3 *database = NULL;

    check_int_eq(sqlite3_open_v2(path, &database, SQLITE_OPEN_READWRITE, NULL),
                 SQLITE_OK);
    check_int_eq(sqlite3_exec(database, sql, NULL, NULL, NULL), SQLITE_OK);
    check_int_eq(sqlite3_close(database), SQLITE_OK);
}

spec("raft SQLite durable storage")
{
    it("reopens committed hard state and log for core recovery")
    {
        static const tr_raft_node_id_t voters[] = {1U};
        char *path = tt_make_temp_file("turboraft", ".db");
        tr_raft_sqlite_storage_t *storage;
        tr_raft_storage_t adapter;
        tr_raft_sqlite_recovery_t recovery;
        static const unsigned char snapshot[] = {0x01U, 0x00U, 0x7fU};
        tr_raft_entry_t entries[3];
        tr_raft_core_config_t core_config;
        tr_raft_core_t *core = NULL;
        tr_raft_ready_t ready;

        check_not_null(path);
        storage = sqlite_test_open(path, true);
        check_int_eq(tr_raft_sqlite_storage_bind(storage, &adapter), TURBO_OK);
        entries[0] = sqlite_test_entry(1U, 1U, 11U, "one");
        entries[1] = sqlite_test_entry(2U, 2U, 12U, "two");
        entries[2] = sqlite_test_entry(3U, 2U, 13U, "three");
        check_int_eq(adapter.begin(adapter.context), TURBO_OK);
        check_int_eq(adapter.write_hard_state(adapter.context, 2U, 1U),
                     TURBO_OK);
        check_int_eq(adapter.append_log(adapter.context, entries, 3U),
                     TURBO_OK);
        check_int_eq(adapter.write_commit_index(adapter.context, 3U),
                     TURBO_OK);
        check_int_eq(adapter.commit(adapter.context), TURBO_OK);
        check_int_eq(tr_raft_sqlite_storage_store_snapshot(
                         storage, 2U, 2U, &sqlite_test_configuration,
                         snapshot, sizeof(snapshot)),
                     TURBO_OK);
        check_int_eq(tr_raft_sqlite_storage_close(storage), TURBO_OK);

        storage = sqlite_test_open(path, false);
        memset(&recovery, 0, sizeof(recovery));
        check_int_eq(tr_raft_sqlite_storage_load(storage, &recovery),
                     TURBO_OK);
        check_long_eq(recovery.term, 2U);
        check_long_eq(recovery.voted_for, 1U);
        check_long_eq(recovery.commit_index, 3U);
        check_long_eq(recovery.snapshot_index, 2U);
        check_long_eq(recovery.snapshot_term, 2U);
        check(recovery.has_snapshot_configuration);
        check_size_eq(recovery.snapshot_configuration.member_count, 1U);
        check_size_eq(recovery.snapshot_size, sizeof(snapshot));
        check_int_eq(memcmp(recovery.snapshot_data, snapshot,
                            sizeof(snapshot)), 0);
        check_size_eq(recovery.entry_count, 1U);
        check_long_eq(recovery.entries[0].index, 3U);
        check_long_eq(recovery.entries[0].term, 2U);
        check_str_eq((const char *) recovery.entries[0].data, "three");

        memset(&core_config, 0, sizeof(core_config));
        core_config.self_id = 1U;
        core_config.voters = voters;
        core_config.voter_count = 1U;
        core_config.initial_configuration =
            &recovery.snapshot_configuration;
        core_config.heartbeat_ticks = 2U;
        core_config.election_min_ticks = 5U;
        core_config.election_max_ticks = 10U;
        core_config.initial_election_timeout_ticks = 5U;
        core_config.initial_term = recovery.term;
        core_config.initial_vote = recovery.voted_for;
        core_config.initial_last_log_index = recovery.snapshot_index;
        core_config.initial_last_log_term = recovery.snapshot_term;
        core_config.initial_log_entries = recovery.entries;
        core_config.initial_log_entry_count = recovery.entry_count;
        core_config.initial_commit_index = recovery.commit_index;
        core_config.initial_applied_index = recovery.snapshot_index;
        core_config.max_log_entries = 8U;
        check_int_eq(tr_raft_core_create(&core_config, &core), TURBO_OK);
        memset(&ready, 0, sizeof(ready));
        check_int_eq(tr_raft_core_poll(core, &ready), TURBO_OK);
        check_size_eq(ready.committed_entry_count, 1U);
        check_long_eq(ready.committed_entries[0].command_id, 13U);
        check_int_eq(tr_raft_core_advance(core), TURBO_OK);

        tr_raft_core_destroy(core);
        tr_raft_sqlite_recovery_destroy(&recovery);
        check_int_eq(tr_raft_sqlite_storage_close(storage), TURBO_OK);
        check_int_eq(tt_remove_file(path), 0);
        free(path);
    }

    it("atomically migrates a version 1 database")
    {
        static const char schema_v1[] =
            "CREATE TABLE raft_state("
            "singleton INTEGER PRIMARY KEY CHECK(singleton=1),"
            "current_term INTEGER NOT NULL CHECK(current_term>=0),"
            "voted_for INTEGER NOT NULL CHECK(voted_for>=0),"
            "commit_index INTEGER NOT NULL CHECK(commit_index>=0));"
            "INSERT INTO raft_state VALUES(1,0,0,0);"
            "CREATE TABLE raft_log("
            "log_index INTEGER PRIMARY KEY CHECK(log_index>0),"
            "term INTEGER NOT NULL CHECK(term>=0),"
            "command_id INTEGER NOT NULL CHECK(command_id>=0),"
            "data BLOB NOT NULL);"
            "PRAGMA user_version=1;";
        char *path = tt_make_temp_file("turboraft-v1", ".db");
        sqlite3 *database = NULL;
        tr_raft_sqlite_storage_t *storage;
        tr_raft_sqlite_recovery_t recovery;

        check_not_null(path);
        check_int_eq(sqlite3_open_v2(path, &database,
                                    SQLITE_OPEN_READWRITE, NULL), SQLITE_OK);
        check_int_eq(sqlite3_exec(database, schema_v1, NULL, NULL, NULL),
                     SQLITE_OK);
        check_int_eq(sqlite3_close(database), SQLITE_OK);
        storage = sqlite_test_open(path, false);
        memset(&recovery, 0, sizeof(recovery));
        check_int_eq(tr_raft_sqlite_storage_load(storage, &recovery),
                     TURBO_OK);
        check_long_eq(recovery.snapshot_index, 0U);
        check_long_eq(recovery.snapshot_term, 0U);
        check_size_eq(recovery.snapshot_size, 0U);
        check(!recovery.has_snapshot_configuration);
        tr_raft_sqlite_recovery_destroy(&recovery);
        check_int_eq(tr_raft_sqlite_storage_close(storage), TURBO_OK);
        check_int_eq(tt_remove_file(path), 0);
        free(path);
    }

    it("atomically migrates a version 2 legacy snapshot")
    {
        static const char schema_v2[] =
            "CREATE TABLE raft_state("
            "singleton INTEGER PRIMARY KEY CHECK(singleton=1),"
            "current_term INTEGER NOT NULL CHECK(current_term>=0),"
            "voted_for INTEGER NOT NULL CHECK(voted_for>=0),"
            "commit_index INTEGER NOT NULL CHECK(commit_index>=0));"
            "INSERT INTO raft_state VALUES(1,2,1,1);"
            "CREATE TABLE raft_log("
            "log_index INTEGER PRIMARY KEY CHECK(log_index>0),"
            "term INTEGER NOT NULL CHECK(term>=0),"
            "command_id INTEGER NOT NULL CHECK(command_id>=0),"
            "data BLOB NOT NULL);"
            "CREATE TABLE raft_snapshot("
            "singleton INTEGER PRIMARY KEY CHECK(singleton=1),"
            "last_included_index INTEGER NOT NULL,"
            "last_included_term INTEGER NOT NULL,"
            "data BLOB NOT NULL);"
            "INSERT INTO raft_snapshot VALUES(1,1,1,x'aa');"
            "PRAGMA user_version=2;";
        char *path = tt_make_temp_file("turboraft-v2", ".db");
        sqlite3 *database = NULL;
        tr_raft_sqlite_storage_t *storage;
        tr_raft_sqlite_recovery_t recovery;

        check_not_null(path);
        check_int_eq(sqlite3_open_v2(path, &database,
                                    SQLITE_OPEN_READWRITE, NULL), SQLITE_OK);
        check_int_eq(sqlite3_exec(database, schema_v2, NULL, NULL, NULL),
                     SQLITE_OK);
        check_int_eq(sqlite3_close(database), SQLITE_OK);
        storage = sqlite_test_open(path, false);
        memset(&recovery, 0, sizeof(recovery));
        check_int_eq(tr_raft_sqlite_storage_load(storage, &recovery),
                     TURBO_OK);
        check_long_eq(recovery.snapshot_index, 1U);
        check(!recovery.has_snapshot_configuration);
        tr_raft_sqlite_recovery_destroy(&recovery);
        check_int_eq(tr_raft_sqlite_storage_close(storage), TURBO_OK);
        check_int_eq(tt_remove_file(path), 0);
        free(path);
    }

    it("reopens a snapshot with an empty log suffix")
    {
        static const unsigned char snapshot[] = {0xa5U, 0x5aU};
        char *path = tt_make_temp_file("turboraft-empty-suffix", ".db");
        tr_raft_sqlite_storage_t *storage;
        tr_raft_storage_t adapter;
        tr_raft_sqlite_recovery_t recovery;
        tr_raft_entry_t entry;

        check_not_null(path);
        storage = sqlite_test_open(path, true);
        check_int_eq(tr_raft_sqlite_storage_bind(storage, &adapter), TURBO_OK);
        entry = sqlite_test_entry(1U, 1U, 21U, "only");
        check_int_eq(adapter.begin(adapter.context), TURBO_OK);
        check_int_eq(adapter.append_log(adapter.context, &entry, 1U),
                     TURBO_OK);
        check_int_eq(adapter.write_commit_index(adapter.context, 1U),
                     TURBO_OK);
        check_int_eq(adapter.commit(adapter.context), TURBO_OK);
        check_int_eq(tr_raft_sqlite_storage_store_snapshot(
                         storage, 1U, 1U, &sqlite_test_configuration,
                         snapshot, sizeof(snapshot)),
                     TURBO_OK);
        check_int_eq(tr_raft_sqlite_storage_close(storage), TURBO_OK);

        storage = sqlite_test_open(path, false);
        memset(&recovery, 0, sizeof(recovery));
        check_int_eq(tr_raft_sqlite_storage_load(storage, &recovery),
                     TURBO_OK);
        check_long_eq(recovery.commit_index, 1U);
        check_long_eq(recovery.snapshot_index, 1U);
        check_long_eq(recovery.snapshot_term, 1U);
        check_size_eq(recovery.snapshot_size, sizeof(snapshot));
        check_int_eq(memcmp(recovery.snapshot_data, snapshot,
                            sizeof(snapshot)), 0);
        check_size_eq(recovery.entry_count, 0U);
        tr_raft_sqlite_recovery_destroy(&recovery);
        check_int_eq(tr_raft_sqlite_storage_close(storage), TURBO_OK);
        check_int_eq(tt_remove_file(path), 0);
        free(path);
    }

    it("rolls back an uncommitted transaction before reopen")
    {
        char *path = tt_make_temp_file("turboraft-rollback", ".db");
        tr_raft_sqlite_storage_t *storage;
        tr_raft_storage_t adapter;
        tr_raft_sqlite_recovery_t recovery;

        check_not_null(path);
        storage = sqlite_test_open(path, true);
        check_int_eq(tr_raft_sqlite_storage_bind(storage, &adapter), TURBO_OK);
        check_int_eq(adapter.begin(adapter.context), TURBO_OK);
        check_int_eq(adapter.write_hard_state(adapter.context, 9U, 1U),
                     TURBO_OK);
        check_int_eq(adapter.rollback(adapter.context), TURBO_OK);
        check_int_eq(tr_raft_sqlite_storage_close(storage), TURBO_OK);

        storage = sqlite_test_open(path, false);
        memset(&recovery, 0, sizeof(recovery));
        check_int_eq(tr_raft_sqlite_storage_load(storage, &recovery),
                     TURBO_OK);
        check_long_eq(recovery.term, 0U);
        check_long_eq(recovery.voted_for, 0U);
        check_long_eq(recovery.commit_index, 0U);
        check_size_eq(recovery.entry_count, 0U);
        tr_raft_sqlite_recovery_destroy(&recovery);
        check_int_eq(tr_raft_sqlite_storage_close(storage), TURBO_OK);
        check_int_eq(tt_remove_file(path), 0);
        free(path);
    }

    it("atomically installs a remote snapshot and preserves a matching suffix")
    {
        static const unsigned char snapshot[] = {0x10U, 0x20U};
        char *path = tt_make_temp_file("turboraft-remote-snapshot", ".db");
        tr_raft_sqlite_storage_t *storage;
        tr_raft_storage_t adapter;
        tr_raft_sqlite_recovery_t recovery;
        tr_raft_entry_t entries[3];

        check_not_null(path);
        storage = sqlite_test_open(path, true);
        check_int_eq(tr_raft_sqlite_storage_bind(storage, &adapter), TURBO_OK);
        entries[0] = sqlite_test_entry(1U, 1U, 31U, "one");
        entries[1] = sqlite_test_entry(2U, 2U, 32U, "two");
        entries[2] = sqlite_test_entry(3U, 3U, 33U, "three");
        check_int_eq(adapter.begin(adapter.context), TURBO_OK);
        check_int_eq(adapter.write_hard_state(adapter.context, 1U, 1U),
                     TURBO_OK);
        check_int_eq(adapter.append_log(adapter.context, entries, 3U),
                     TURBO_OK);
        check_int_eq(adapter.write_commit_index(adapter.context, 1U),
                     TURBO_OK);
        check_int_eq(adapter.commit(adapter.context), TURBO_OK);
        check_int_eq(tr_raft_sqlite_storage_install_snapshot(
                         storage, 4U, 2U, 2U, &sqlite_test_configuration,
                         snapshot, sizeof(snapshot)),
                     TURBO_OK);
        check_int_eq(tr_raft_sqlite_storage_close(storage), TURBO_OK);

        storage = sqlite_test_open(path, false);
        memset(&recovery, 0, sizeof(recovery));
        check_int_eq(tr_raft_sqlite_storage_load(storage, &recovery),
                     TURBO_OK);
        check_long_eq(recovery.term, 4U);
        check_long_eq(recovery.voted_for, 0U);
        check_long_eq(recovery.commit_index, 2U);
        check_long_eq(recovery.snapshot_index, 2U);
        check_long_eq(recovery.snapshot_term, 2U);
        check(recovery.has_snapshot_configuration);
        check_size_eq(recovery.entry_count, 1U);
        check_long_eq(recovery.entries[0].index, 3U);
        check_long_eq(recovery.entries[0].term, 3U);
        tr_raft_sqlite_recovery_destroy(&recovery);
        check_int_eq(tr_raft_sqlite_storage_close(storage), TURBO_OK);
        check_int_eq(tt_remove_file(path), 0);
        free(path);
    }

    it("rejects a database that fails SQLite quick-check integrity")
    {
        char *path = tt_make_temp_file("turboraft-corrupt-check", ".db");
        tr_raft_sqlite_storage_config_t config;
        tr_raft_sqlite_storage_t *storage;

        check_not_null(path);
        storage = sqlite_test_open(path, true);
        check_int_eq(tr_raft_sqlite_storage_close(storage), TURBO_OK);
        sqlite_test_exec_path(
            path,
            "PRAGMA ignore_check_constraints=ON;"
            "UPDATE raft_state SET current_term=-1 WHERE singleton=1;");

        config = sqlite_test_config(path, false);
        storage = NULL;
        check_int_eq(tr_raft_sqlite_storage_open(&config, &storage), TURBO_EIO);
        check_null(storage);
        check_int_eq(tt_remove_file(path), 0);
        free(path);
    }

    it("survives repeated snapshot compaction and durable restart cycles")
    {
        char *path = tt_make_temp_file("turboraft-storage-soak", ".db");
        size_t cycle;

        check_not_null(path);
        for (cycle = 1U; cycle <= SQLITE_TEST_SOAK_CYCLE_COUNT; ++cycle) {
            tr_raft_sqlite_storage_t *storage =
                sqlite_test_open(path, cycle == 1U);
            tr_raft_storage_t adapter;
            tr_raft_sqlite_recovery_t recovery;
            tr_raft_entry_t entry;
            tr_raft_index_t previous_index = (tr_raft_index_t) cycle - 1U;
            tr_raft_index_t expected_snapshot =
                previous_index -
                previous_index % SQLITE_TEST_SOAK_SNAPSHOT_INTERVAL;

            check_int_eq(tr_raft_sqlite_storage_bind(storage, &adapter),
                         TURBO_OK);
            memset(&recovery, 0, sizeof(recovery));
            check_int_eq(tr_raft_sqlite_storage_load(storage, &recovery),
                         TURBO_OK);
            check_long_eq(recovery.term, previous_index);
            check_long_eq(recovery.commit_index, previous_index);
            check_long_eq(recovery.snapshot_index, expected_snapshot);
            check_size_eq(recovery.entry_count,
                          (size_t) (previous_index - expected_snapshot));
            if (recovery.entry_count != 0U) {
                check_long_eq(
                    recovery.entries[recovery.entry_count - 1U].index,
                    previous_index);
            }
            tr_raft_sqlite_recovery_destroy(&recovery);

            entry = sqlite_test_entry(
                (tr_raft_index_t) cycle, (tr_raft_term_t) cycle,
                SQLITE_TEST_SOAK_COMMAND_BASE + cycle, "soak");
            check_int_eq(adapter.begin(adapter.context), TURBO_OK);
            check_int_eq(adapter.write_hard_state(
                             adapter.context, (tr_raft_term_t) cycle, 1U),
                         TURBO_OK);
            check_int_eq(adapter.append_log(adapter.context, &entry, 1U),
                         TURBO_OK);
            check_int_eq(adapter.write_commit_index(
                             adapter.context, (tr_raft_index_t) cycle),
                         TURBO_OK);
            check_int_eq(adapter.commit(adapter.context), TURBO_OK);

            if (cycle % SQLITE_TEST_SOAK_SNAPSHOT_INTERVAL == 0U) {
                uint8_t snapshot[] = {
                    (uint8_t) cycle,
                    (uint8_t) (cycle >> 8U)
                };

                check_int_eq(tr_raft_sqlite_storage_store_snapshot(
                                 storage, (tr_raft_index_t) cycle,
                                 (tr_raft_term_t) cycle,
                                 &sqlite_test_configuration, snapshot,
                                 sizeof(snapshot)),
                             TURBO_OK);
            }
            check_int_eq(tr_raft_sqlite_storage_close(storage), TURBO_OK);
        }

        {
            tr_raft_sqlite_storage_t *storage = sqlite_test_open(path, false);
            tr_raft_sqlite_recovery_t recovery;
            static const uint8_t final_snapshot[] = {
                SQLITE_TEST_SOAK_CYCLE_COUNT,
                0U
            };

            memset(&recovery, 0, sizeof(recovery));
            check_int_eq(tr_raft_sqlite_storage_load(storage, &recovery),
                         TURBO_OK);
            check_long_eq(recovery.term, SQLITE_TEST_SOAK_CYCLE_COUNT);
            check_long_eq(recovery.commit_index,
                          SQLITE_TEST_SOAK_CYCLE_COUNT);
            check_long_eq(recovery.snapshot_index,
                          SQLITE_TEST_SOAK_CYCLE_COUNT);
            check_long_eq(recovery.snapshot_term,
                          SQLITE_TEST_SOAK_CYCLE_COUNT);
            check_size_eq(recovery.entry_count, 0U);
            check_size_eq(recovery.snapshot_size, sizeof(final_snapshot));
            check_mem_eq(recovery.snapshot_data, final_snapshot,
                         sizeof(final_snapshot));
            tr_raft_sqlite_recovery_destroy(&recovery);
            check_int_eq(tr_raft_sqlite_storage_close(storage), TURBO_OK);
        }
        check_int_eq(tt_remove_file(path), 0);
        free(path);
    }

    it("rejects deterministic corrupt recovery-state corpus entries")
    {
        static const struct sqlite_corrupt_case {
            const char *name;
            const char *mutation;
            size_t max_snapshot_bytes;
        } cases[] = {
            {"commit beyond durable log",
             "UPDATE raft_state SET commit_index=1 WHERE singleton=1;",
             1024U},
            {"unpaired snapshot term",
             "UPDATE raft_state SET commit_index=1 WHERE singleton=1;"
             "UPDATE raft_snapshot SET last_included_index=1 "
             "WHERE singleton=1;",
             1024U},
            {"log index gap",
             "INSERT INTO raft_log(log_index,term,command_id,data) "
             "VALUES(2,1,1,x'01');",
             1024U},
            {"malformed snapshot configuration",
             "UPDATE raft_state SET commit_index=1 WHERE singleton=1;"
             "UPDATE raft_snapshot SET last_included_index=1,"
             "last_included_term=1,configuration=x'01' WHERE singleton=1;",
             1024U},
            {"snapshot beyond configured quota",
             "UPDATE raft_state SET commit_index=1 WHERE singleton=1;"
             "UPDATE raft_snapshot SET last_included_index=1,"
             "last_included_term=1,data=x'0000000000' WHERE singleton=1;",
             4U}
        };
        size_t case_index;

        for (case_index = 0U;
             case_index < sizeof(cases) / sizeof(cases[0]);
             ++case_index) {
            char *path = tt_make_temp_file("turboraft-corrupt-state", ".db");
            tr_raft_sqlite_storage_config_t config;
            tr_raft_sqlite_storage_t *storage;
            tr_raft_sqlite_recovery_t recovery;
            int load_result;

            check_not_null(path);
            storage = sqlite_test_open(path, true);
            check_int_eq(tr_raft_sqlite_storage_close(storage), TURBO_OK);
            sqlite_test_exec_path(path, cases[case_index].mutation);

            config = sqlite_test_config(path, false);
            config.max_snapshot_bytes = cases[case_index].max_snapshot_bytes;
            storage = NULL;
            check_int_eq(tr_raft_sqlite_storage_open(&config, &storage),
                         TURBO_OK);
            memset(&recovery, 0, sizeof(recovery));
            load_result = tr_raft_sqlite_storage_load(storage, &recovery);
            if (load_result != TURBO_EPROTO) {
                fprintf(stderr, "corruption corpus case failed: %s\n",
                        cases[case_index].name);
            }
            check_int_eq(load_result, TURBO_EPROTO);
            check_size_eq(recovery.entry_count, 0U);
            check_size_eq(recovery.snapshot_size, 0U);
            check_int_eq(tr_raft_sqlite_storage_close(storage), TURBO_OK);
            check_int_eq(tt_remove_file(path), 0);
            free(path);
        }
    }
}
