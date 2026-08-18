#include <turboraft/raft_wal_storage.h>

#include <tinytest.h>
#include <turbo_error.h>
#include <turbo_fs.h>

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
    WAL_TEST_MAX_ENTRIES = 128U,
    WAL_TEST_MAX_SEGMENTS = 4U
};

static tr_raft_wal_storage_config_t wal_test_config(const char *prefix,
                                                    bool create)
{
    tr_raft_wal_storage_config_t config;
    memset(&config, 0, sizeof(config));
    config.path_prefix = prefix;
    config.segment_bytes = TR_RAFT_WAL_MIN_SEGMENT_BYTES;
    config.max_transaction_bytes = 8U * 1024U;
    config.max_segments = WAL_TEST_MAX_SEGMENTS;
    config.max_log_entries = WAL_TEST_MAX_ENTRIES;
    config.max_snapshot_bytes = 1024U;
    config.create_if_missing = create;
    return config;
}

static tr_raft_entry_t wal_test_entry(tr_raft_index_t index,
                                      tr_raft_term_t term,
                                      uint64_t command_id,
                                      const char *data)
{
    tr_raft_entry_t entry;
    size_t size = strlen(data) + 1U;
    memset(&entry, 0, sizeof(entry));
    entry.index = index;
    entry.term = term;
    entry.command_id = command_id;
    entry.data_length = size;
    memcpy(entry.data, data, size);
    return entry;
}

static void wal_test_path(char *output, size_t output_size,
                          const char *prefix, const char *suffix)
{
    snprintf(output, output_size, "%s%s", prefix, suffix);
}

static void wal_test_cleanup(char *prefix)
{
    char path[TURBO_FS_MAX_PATH];
    size_t index;
    for (index = 1U; index <= WAL_TEST_MAX_SEGMENTS; ++index) {
        snprintf(path, sizeof(path), "%s.%08zu.wal", prefix, index);
        if (turbo_fs_access(path, TURBO_FS_ACCESS_EXISTS) == TURBO_OK)
            check_int_eq(tt_remove_file(path), 0);
    }
    wal_test_path(path, sizeof(path), prefix, ".lock");
    if (turbo_fs_access(path, TURBO_FS_ACCESS_EXISTS) == TURBO_OK)
        check_int_eq(tt_remove_file(path), 0);
    check_int_eq(tt_remove_file(prefix), 0);
    free(prefix);
}

spec("raft segmented WAL storage")
{
    it("stores an FSM snapshot and retains the matching suffix")
    {
        const tr_raft_conf_t configuration = {
            TR_RAFT_CONF_FINAL, 7U, 1U,
            {{1U, TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER}}};
        const uint8_t snapshot[] = {0x10U, 0x20U, 0x30U};
        char *prefix = tt_make_temp_file("turboraft-wal-snapshot", ".data");
        tr_raft_wal_storage_config_t config = wal_test_config(prefix, true);
        tr_raft_wal_storage_t *storage = NULL;
        tr_raft_storage_t adapter;
        tr_raft_wal_recovery_t recovery;
        tr_raft_entry_t entries[3];
        char snapshot_path[TURBO_FS_MAX_PATH];

        entries[0] = wal_test_entry(1U, 1U, 1U, "one");
        entries[1] = wal_test_entry(2U, 2U, 2U, "two");
        entries[2] = wal_test_entry(3U, 2U, 3U, "three");
        check_int_eq(tr_raft_wal_storage_open(&config, &storage), TURBO_OK);
        check_int_eq(tr_raft_wal_storage_bind(storage, &adapter), TURBO_OK);
        check_int_eq(adapter.begin(adapter.context), TURBO_OK);
        check_int_eq(adapter.write_hard_state(adapter.context, 2U, 1U),
                     TURBO_OK);
        check_int_eq(adapter.append_log(adapter.context, entries, 3U),
                     TURBO_OK);
        check_int_eq(adapter.write_commit_index(adapter.context, 3U),
                     TURBO_OK);
        check_int_eq(adapter.commit(adapter.context), TURBO_OK);
        check_int_eq(tr_raft_wal_storage_store_snapshot(
                         storage, 2U, 2U, &configuration,
                         snapshot, sizeof(snapshot)), TURBO_OK);
        memset(&recovery, 0, sizeof(recovery));
        check_int_eq(tr_raft_wal_storage_load(storage, &recovery), TURBO_OK);
        check_long_eq(recovery.snapshot_index, 2U);
        check_long_eq(recovery.snapshot_term, 2U);
        check_long_eq(recovery.commit_index, 3U);
        check_size_eq(recovery.entry_count, 1U);
        check_long_eq(recovery.entries[0].index, 3U);
        check_mem_eq(recovery.snapshot_data, snapshot, sizeof(snapshot));
        check_long_eq(recovery.snapshot_configuration.transition_id, 7U);
        tr_raft_wal_recovery_destroy(&recovery);
        check_int_eq(tr_raft_wal_storage_close(storage), TURBO_OK);
        snprintf(snapshot_path, sizeof(snapshot_path), "%s.snapshot.2.2",
                 prefix);
        check_int_eq(tt_remove_file(snapshot_path), 0);
        wal_test_cleanup(prefix);
    }

    it("installs a remote FSM snapshot as the new recovery baseline")
    {
        const tr_raft_conf_t configuration = {
            TR_RAFT_CONF_FINAL, 9U, 1U,
            {{1U, TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER}}};
        const uint8_t snapshot[] = {0x41U, 0x42U};
        char *prefix = tt_make_temp_file("turboraft-wal-install", ".data");
        tr_raft_wal_storage_config_t config = wal_test_config(prefix, true);
        tr_raft_wal_storage_t *storage = NULL;
        tr_raft_wal_recovery_t recovery;
        char snapshot_path[TURBO_FS_MAX_PATH];

        check_int_eq(tr_raft_wal_storage_open(&config, &storage), TURBO_OK);
        check_int_eq(tr_raft_wal_storage_install_snapshot(
                         storage, 7U, 9U, 6U, &configuration,
                         snapshot, sizeof(snapshot)), TURBO_OK);
        check_int_eq(tr_raft_wal_storage_close(storage), TURBO_OK);
        config.create_if_missing = false;
        check_int_eq(tr_raft_wal_storage_open(&config, &storage), TURBO_OK);
        memset(&recovery, 0, sizeof(recovery));
        check_int_eq(tr_raft_wal_storage_load(storage, &recovery), TURBO_OK);
        check_long_eq(recovery.term, 7U);
        check_long_eq(recovery.voted_for, 0U);
        check_long_eq(recovery.snapshot_index, 9U);
        check_long_eq(recovery.snapshot_term, 6U);
        check_long_eq(recovery.commit_index, 9U);
        check_size_eq(recovery.entry_count, 0U);
        check_mem_eq(recovery.snapshot_data, snapshot, sizeof(snapshot));
        tr_raft_wal_recovery_destroy(&recovery);
        check_int_eq(tr_raft_wal_storage_close(storage), TURBO_OK);
        snprintf(snapshot_path, sizeof(snapshot_path), "%s.snapshot.9.6",
                 prefix);
        check_int_eq(tt_remove_file(snapshot_path), 0);
        wal_test_cleanup(prefix);
    }

    it("checkpoints a fully compacted log and removes older segments")
    {
        const tr_raft_conf_t configuration = {
            TR_RAFT_CONF_FINAL, 1U, 1U,
            {{1U, TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER}}};
        const uint8_t snapshot[] = {0x55U};
        char *prefix = tt_make_temp_file("turboraft-wal-checkpoint", ".data");
        tr_raft_wal_storage_config_t config = wal_test_config(prefix, true);
        tr_raft_wal_storage_t *storage = NULL;
        tr_raft_storage_t adapter;
        tr_raft_entry_t entry = wal_test_entry(1U, 1U, 1U, "one");
        tr_raft_wal_recovery_t recovery;
        char first_segment[TURBO_FS_MAX_PATH];
        char second_segment[TURBO_FS_MAX_PATH];
        char snapshot_path[TURBO_FS_MAX_PATH];

        check_int_eq(tr_raft_wal_storage_open(&config, &storage), TURBO_OK);
        check_int_eq(tr_raft_wal_storage_bind(storage, &adapter), TURBO_OK);
        check_int_eq(adapter.begin(adapter.context), TURBO_OK);
        check_int_eq(adapter.write_hard_state(adapter.context, 1U, 1U),
                     TURBO_OK);
        check_int_eq(adapter.append_log(adapter.context, &entry, 1U), TURBO_OK);
        check_int_eq(adapter.write_commit_index(adapter.context, 1U), TURBO_OK);
        check_int_eq(adapter.commit(adapter.context), TURBO_OK);
        check_int_eq(tr_raft_wal_storage_store_snapshot(
                         storage, 1U, 1U, &configuration,
                         snapshot, sizeof(snapshot)), TURBO_OK);
        check_int_eq(tr_raft_wal_storage_close(storage), TURBO_OK);
        snprintf(first_segment, sizeof(first_segment), "%s.00000001.wal",
                 prefix);
        snprintf(second_segment, sizeof(second_segment), "%s.00000002.wal",
                 prefix);
        check_int_ne(turbo_fs_access(first_segment, TURBO_FS_ACCESS_EXISTS),
                     TURBO_OK);
        check_int_eq(turbo_fs_access(second_segment, TURBO_FS_ACCESS_EXISTS),
                     TURBO_OK);
        config.create_if_missing = false;
        check_int_eq(tr_raft_wal_storage_open(&config, &storage), TURBO_OK);
        memset(&recovery, 0, sizeof(recovery));
        check_int_eq(tr_raft_wal_storage_load(storage, &recovery), TURBO_OK);
        check_long_eq(recovery.snapshot_index, 1U);
        check_long_eq(recovery.commit_index, 1U);
        check_size_eq(recovery.entry_count, 0U);
        tr_raft_wal_recovery_destroy(&recovery);
        check_int_eq(tr_raft_wal_storage_close(storage), TURBO_OK);
        snprintf(snapshot_path, sizeof(snapshot_path), "%s.snapshot.1.1",
                 prefix);
        check_int_eq(tt_remove_file(snapshot_path), 0);
        wal_test_cleanup(prefix);
    }

    it("recovers committed hard state log and commit index")
    {
        char *prefix = tt_make_temp_file("turboraft-wal", ".data");
        tr_raft_wal_storage_config_t config = wal_test_config(prefix, true);
        tr_raft_wal_storage_t *storage = NULL;
        tr_raft_storage_t adapter;
        tr_raft_wal_recovery_t recovery;
        tr_raft_entry_t entries[3];

        check_not_null(prefix);
        entries[0] = wal_test_entry(1U, 1U, 11U, "one");
        entries[1] = wal_test_entry(2U, 2U, 12U, "two");
        entries[2] = wal_test_entry(3U, 2U, 13U, "three");
        check_int_eq(tr_raft_wal_storage_open(&config, &storage), TURBO_OK);
        check_int_eq(tr_raft_wal_storage_bind(storage, &adapter), TURBO_OK);
        check_int_eq(adapter.begin(adapter.context), TURBO_OK);
        check_int_eq(adapter.write_hard_state(adapter.context, 2U, 1U),
                     TURBO_OK);
        check_int_eq(adapter.append_log(adapter.context, entries, 3U),
                     TURBO_OK);
        check_int_eq(adapter.write_commit_index(adapter.context, 3U),
                     TURBO_OK);
        check_int_eq(adapter.commit(adapter.context), TURBO_OK);
        check_int_eq(tr_raft_wal_storage_close(storage), TURBO_OK);

        config.create_if_missing = false;
        storage = NULL;
        check_int_eq(tr_raft_wal_storage_open(&config, &storage), TURBO_OK);
        memset(&recovery, 0, sizeof(recovery));
        check_int_eq(tr_raft_wal_storage_load(storage, &recovery), TURBO_OK);
        check_long_eq(recovery.term, 2U);
        check_long_eq(recovery.voted_for, 1U);
        check_long_eq(recovery.commit_index, 3U);
        check_size_eq(recovery.entry_count, 3U);
        check_str_eq((const char *)recovery.entries[2].data, "three");
        tr_raft_wal_recovery_destroy(&recovery);
        check_int_eq(tr_raft_wal_storage_close(storage), TURBO_OK);
        wal_test_cleanup(prefix);
    }

    it("discards rollback and a torn final frame")
    {
        char *prefix = tt_make_temp_file("turboraft-wal-tail", ".data");
        tr_raft_wal_storage_config_t config = wal_test_config(prefix, true);
        tr_raft_wal_storage_t *storage = NULL;
        tr_raft_storage_t adapter;
        tr_raft_wal_recovery_t recovery;
        tr_raft_entry_t entry = wal_test_entry(1U, 1U, 21U, "only");
        char path[TURBO_FS_MAX_PATH];
        turbo_file_t file;
        static const uint8_t torn[] = {0x54U, 0x52U, 0x57U};

        check_int_eq(tr_raft_wal_storage_open(&config, &storage), TURBO_OK);
        check_int_eq(tr_raft_wal_storage_bind(storage, &adapter), TURBO_OK);
        check_int_eq(adapter.begin(adapter.context), TURBO_OK);
        check_int_eq(adapter.append_log(adapter.context, &entry, 1U), TURBO_OK);
        check_int_eq(adapter.commit(adapter.context), TURBO_OK);
        check_int_eq(adapter.begin(adapter.context), TURBO_OK);
        check_int_eq(adapter.write_hard_state(adapter.context, 9U, 1U),
                     TURBO_OK);
        check_int_eq(adapter.rollback(adapter.context), TURBO_OK);
        check_int_eq(tr_raft_wal_storage_close(storage), TURBO_OK);

        wal_test_path(path, sizeof(path), prefix, ".00000001.wal");
        file = turbo_fs_open(path, TURBO_FS_O_WRONLY | TURBO_FS_O_APPEND, 0);
        check_int_ne(file, TURBO_INVALID_FILE);
        check_int_eq(turbo_fs_write(file, (const char *)torn, sizeof(torn)),
                     (int)sizeof(torn));
        check_int_eq(turbo_fs_close(file), TURBO_OK);

        config.create_if_missing = false;
        storage = NULL;
        check_int_eq(tr_raft_wal_storage_open(&config, &storage), TURBO_OK);
        memset(&recovery, 0, sizeof(recovery));
        check_int_eq(tr_raft_wal_storage_load(storage, &recovery), TURBO_OK);
        check_long_eq(recovery.term, 0U);
        check_size_eq(recovery.entry_count, 1U);
        tr_raft_wal_recovery_destroy(&recovery);
        check_int_eq(tr_raft_wal_storage_close(storage), TURBO_OK);
        wal_test_cleanup(prefix);
    }

    it("rejects checksum corruption instead of skipping data")
    {
        char *prefix = tt_make_temp_file("turboraft-wal-bad", ".data");
        tr_raft_wal_storage_config_t config = wal_test_config(prefix, true);
        tr_raft_wal_storage_t *storage = NULL;
        tr_raft_storage_t adapter;
        tr_raft_entry_t entry = wal_test_entry(1U, 1U, 31U, "value");
        char path[TURBO_FS_MAX_PATH];
        turbo_file_t file;
        uint8_t byte = 0xffU;

        check_int_eq(tr_raft_wal_storage_open(&config, &storage), TURBO_OK);
        check_int_eq(tr_raft_wal_storage_bind(storage, &adapter), TURBO_OK);
        check_int_eq(adapter.begin(adapter.context), TURBO_OK);
        check_int_eq(adapter.append_log(adapter.context, &entry, 1U), TURBO_OK);
        check_int_eq(adapter.commit(adapter.context), TURBO_OK);
        check_int_eq(tr_raft_wal_storage_close(storage), TURBO_OK);

        wal_test_path(path, sizeof(path), prefix, ".00000001.wal");
        file = turbo_fs_open(path, TURBO_FS_O_RDWR, 0);
        check_int_ne(file, TURBO_INVALID_FILE);
        check_int_eq(turbo_fs_pwrite(file, (const char *)&byte, 1U, 96), 1);
        check_int_eq(turbo_fs_fsync(file), TURBO_OK);
        check_int_eq(turbo_fs_close(file), TURBO_OK);
        storage = NULL;
        config.create_if_missing = false;
        check_int_eq(tr_raft_wal_storage_open(&config, &storage),
                     TURBO_EPROTO);
        check_null(storage);
        wal_test_cleanup(prefix);
    }

    it("persists truncate and replacement entries in one transaction")
    {
        char *prefix = tt_make_temp_file("turboraft-wal-rewrite", ".data");
        tr_raft_wal_storage_config_t config = wal_test_config(prefix, true);
        tr_raft_wal_storage_t *storage = NULL;
        tr_raft_storage_t adapter;
        tr_raft_wal_recovery_t recovery;
        tr_raft_entry_t entries[3];
        tr_raft_entry_t replacements[2];

        entries[0] = wal_test_entry(1U, 1U, 41U, "one");
        entries[1] = wal_test_entry(2U, 1U, 42U, "old-two");
        entries[2] = wal_test_entry(3U, 1U, 43U, "old-three");
        replacements[0] = wal_test_entry(2U, 2U, 44U, "new-two");
        replacements[1] = wal_test_entry(3U, 2U, 45U, "new-three");
        check_int_eq(tr_raft_wal_storage_open(&config, &storage), TURBO_OK);
        check_int_eq(tr_raft_wal_storage_bind(storage, &adapter), TURBO_OK);
        check_int_eq(adapter.begin(adapter.context), TURBO_OK);
        check_int_eq(adapter.append_log(adapter.context, entries, 3U),
                     TURBO_OK);
        check_int_eq(adapter.write_commit_index(adapter.context, 1U),
                     TURBO_OK);
        check_int_eq(adapter.commit(adapter.context), TURBO_OK);
        check_int_eq(adapter.begin(adapter.context), TURBO_OK);
        check_int_eq(adapter.truncate_log(adapter.context, 2U), TURBO_OK);
        check_int_eq(adapter.append_log(adapter.context, replacements, 2U),
                     TURBO_OK);
        check_int_eq(adapter.write_commit_index(adapter.context, 3U),
                     TURBO_OK);
        check_int_eq(adapter.commit(adapter.context), TURBO_OK);
        memset(&recovery, 0, sizeof(recovery));
        check_int_eq(tr_raft_wal_storage_load(storage, &recovery), TURBO_OK);
        check_size_eq(recovery.entry_count, 3U);
        check_long_eq(recovery.entries[1].term, 2U);
        check_str_eq((const char *)recovery.entries[2].data, "new-three");
        tr_raft_wal_recovery_destroy(&recovery);
        check_int_eq(tr_raft_wal_storage_close(storage), TURBO_OK);
        wal_test_cleanup(prefix);
    }

    it("rotates bounded segments and recovers a continuous log")
    {
        char *prefix = tt_make_temp_file("turboraft-wal-rotate", ".data");
        tr_raft_wal_storage_config_t config = wal_test_config(prefix, true);
        tr_raft_wal_storage_t *storage = NULL;
        tr_raft_storage_t adapter;
        tr_raft_wal_recovery_t recovery;
        char second_segment[TURBO_FS_MAX_PATH];
        size_t transaction_index;

        check_int_eq(tr_raft_wal_storage_open(&config, &storage), TURBO_OK);
        check_int_eq(tr_raft_wal_storage_bind(storage, &adapter), TURBO_OK);
        for (transaction_index = 0U; transaction_index < 16U;
             ++transaction_index) {
            tr_raft_entry_t entries[8];
            size_t entry_index;
            for (entry_index = 0U; entry_index < 8U; ++entry_index) {
                tr_raft_index_t index = transaction_index * 8U +
                                        entry_index + 1U;
                entries[entry_index] = wal_test_entry(index, 1U,
                                                       100U + index, "x");
                entries[entry_index].data_length = TR_RAFT_MAX_ENTRY_BYTES;
                memset(entries[entry_index].data, (int)index,
                       sizeof(entries[entry_index].data));
            }
            check_int_eq(adapter.begin(adapter.context), TURBO_OK);
            check_int_eq(adapter.append_log(adapter.context, entries, 8U),
                         TURBO_OK);
            check_int_eq(adapter.write_commit_index(
                             adapter.context,
                             (transaction_index + 1U) * 8U), TURBO_OK);
            check_int_eq(adapter.commit(adapter.context), TURBO_OK);
        }
        check_int_eq(tr_raft_wal_storage_close(storage), TURBO_OK);
        wal_test_path(second_segment, sizeof(second_segment), prefix,
                      ".00000002.wal");
        check_int_eq(turbo_fs_access(second_segment,
                                     TURBO_FS_ACCESS_EXISTS), TURBO_OK);
        config.create_if_missing = false;
        storage = NULL;
        check_int_eq(tr_raft_wal_storage_open(&config, &storage), TURBO_OK);
        memset(&recovery, 0, sizeof(recovery));
        check_int_eq(tr_raft_wal_storage_load(storage, &recovery), TURBO_OK);
        check_size_eq(recovery.entry_count, WAL_TEST_MAX_ENTRIES);
        check_long_eq(recovery.commit_index, WAL_TEST_MAX_ENTRIES);
        check_long_eq(recovery.entries[WAL_TEST_MAX_ENTRIES - 1U].index,
                      WAL_TEST_MAX_ENTRIES);
        tr_raft_wal_recovery_destroy(&recovery);
        check_int_eq(tr_raft_wal_storage_close(storage), TURBO_OK);
        wal_test_cleanup(prefix);
    }

    it("holds an exclusive lock for the storage lifetime")
    {
        char *prefix = tt_make_temp_file("turboraft-wal-lock", ".data");
        tr_raft_wal_storage_config_t config = wal_test_config(prefix, true);
        tr_raft_wal_storage_t *first = NULL;
        tr_raft_wal_storage_t *second = NULL;

        check_int_eq(tr_raft_wal_storage_open(&config, &first), TURBO_OK);
        check_int_eq(tr_raft_wal_storage_open(&config, &second), TURBO_EBUSY);
        check_null(second);
        check_int_eq(tr_raft_wal_storage_close(first), TURBO_OK);
        config.create_if_missing = false;
        check_int_eq(tr_raft_wal_storage_open(&config, &second), TURBO_OK);
        check_int_eq(tr_raft_wal_storage_close(second), TURBO_OK);
        wal_test_cleanup(prefix);
    }
}
