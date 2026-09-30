#include <turboraft/raft_wal_storage.h>

#include <salts_error.h>
#include <salts_fs.h>

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int expect(int condition, const char *message)
{
    if (condition) {
        return 0;
    }
    fprintf(stderr, "FAIL: %s\n", message);
    return 1;
}

static void cleanup_prefix(const char *prefix)
{
    char path[SALTS_FS_MAX_PATH];
    size_t sequence;

    for (sequence = 1U; sequence <= 4U; ++sequence) {
        (void)snprintf(path, sizeof(path), "%s.%08zu.wal",
                       prefix, sequence);
        (void)unlink(path);
        (void)snprintf(path, sizeof(path), "%s.%08zu.wal.tmp",
                       prefix, sequence);
        (void)unlink(path);
    }
    (void)snprintf(path, sizeof(path), "%s.snapshot.1.1", prefix);
    (void)unlink(path);
    (void)snprintf(path, sizeof(path), "%s.snapshot.1.1.tmp", prefix);
    (void)unlink(path);
    (void)snprintf(path, sizeof(path), "%s.lock", prefix);
    (void)unlink(path);
}

static int commit_one_entry(tr_raft_wal_storage_t *storage)
{
    tr_raft_storage_t adapter;
    tr_raft_entry_t entry;

    memset(&adapter, 0, sizeof(adapter));
    memset(&entry, 0, sizeof(entry));
    entry.index = 1U;
    entry.term = 1U;
    entry.command_id = 1U;
    entry.data_length = 3U;
    memcpy(entry.data, "one", 3U);

    if (tr_raft_wal_storage_bind(storage, &adapter) != SALTS_OK) {
        return SALTS_EIO;
    }
    if (adapter.begin(adapter.context) != SALTS_OK ||
        adapter.write_hard_state(adapter.context, 1U, 1U) != SALTS_OK ||
        adapter.append_log(adapter.context, &entry, 1U) != SALTS_OK ||
        adapter.write_commit_index(adapter.context, 1U) != SALTS_OK) {
        return SALTS_EIO;
    }
    return adapter.commit(adapter.context);
}

int main(void)
{
    const char *phase = getenv("TURBORAFT_FS_TEST_FAIL_PHASE");
    const int pre_publish =
        phase != NULL && strcmp(phase, "pre_publish") == 0;
    const int post_publish =
        phase != NULL && strcmp(phase, "post_publish") == 0;
    const tr_raft_conf_t configuration = {
        TR_RAFT_CONF_FINAL,
        1U,
        1U,
        {{1U, TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER}}
    };
    const uint8_t snapshot[] = {0x10U, 0x20U, 0x30U};
    tr_raft_wal_storage_config_t config;
    tr_raft_wal_storage_t *storage = NULL;
    tr_raft_wal_recovery_t recovery;
    char prefix[SALTS_FS_MAX_PATH];
    char snapshot_path[SALTS_FS_MAX_PATH];
    int result;
    int failed = 0;

    if (fail_call != 4 && fail_call != 5) {
        fprintf(stderr, "FAIL: expected fsync fault ordinal 4 or 5\n");
        return 2;
    }

    (void)snprintf(prefix, sizeof(prefix),
                   "/tmp/turboraft-durable-publish-%ld",
                   (long)getpid());
    (void)snprintf(snapshot_path, sizeof(snapshot_path),
                   "%s.snapshot.1.1", prefix);
    cleanup_prefix(prefix);

    memset(&config, 0, sizeof(config));
    config.path_prefix = prefix;
    config.segment_bytes = TR_RAFT_WAL_MIN_SEGMENT_BYTES;
    config.max_transaction_bytes = 8U * 1024U;
    config.max_segments = 4U;
    config.max_log_entries = 16U;
    config.max_snapshot_bytes = 1024U;
    config.create_if_missing = true;

    failed |= expect(
        tr_raft_wal_storage_open(&config, &storage) == SALTS_OK,
        "open storage");
    if (failed != 0) {
        cleanup_prefix(prefix);
        return 1;
    }
    failed |= expect(commit_one_entry(storage) == SALTS_OK,
                     "commit initial durable entry");

    result = tr_raft_wal_storage_store_snapshot(
        storage, 1U, 1U, &configuration, snapshot, sizeof(snapshot));
    failed |= expect(result == SALTS_EIO,
                     "injected snapshot publication fault must surface EIO");

    if (pre_publish) {
        failed |= expect(
            salts_fs_access(snapshot_path, SALTS_FS_ACCESS_EXISTS) != SALTS_OK,
            "pre-publication fault must not publish snapshot");
        failed |= expect(
            tr_raft_wal_storage_store_snapshot(
                storage, 1U, 1U, &configuration,
                snapshot, sizeof(snapshot)) == SALTS_OK,
            "pre-publication failure must remain retryable");
    } else {
        failed |= expect(
            salts_fs_access(snapshot_path, SALTS_FS_ACCESS_EXISTS) == SALTS_OK,
            "post-rename durability fault leaves published bytes visible");
        failed |= expect(
            tr_raft_wal_storage_store_snapshot(
                storage, 1U, 1U, &configuration,
                snapshot, sizeof(snapshot)) == SALTS_EIO,
            "unknown durability must fault the live storage owner");

        failed |= expect(tr_raft_wal_storage_close(storage) == SALTS_OK,
                         "close faulted storage");
        storage = NULL;
        config.create_if_missing = false;
        failed |= expect(
            tr_raft_wal_storage_open(&config, &storage) == SALTS_OK,
            "reopen after uncertain publication");

        memset(&recovery, 0, sizeof(recovery));
        failed |= expect(
            tr_raft_wal_storage_load(storage, &recovery) == SALTS_OK,
            "load authoritative WAL after uncertain publication");
        failed |= expect(recovery.snapshot_index == 0U,
                         "orphan published snapshot is not authoritative");
        failed |= expect(recovery.commit_index == 1U &&
                             recovery.entry_count == 1U,
                         "last durable WAL prefix remains authoritative");
        tr_raft_wal_recovery_destroy(&recovery);

        failed |= expect(
            tr_raft_wal_storage_store_snapshot(
                storage, 1U, 1U, &configuration,
                snapshot, sizeof(snapshot)) == SALTS_OK,
            "reopen may safely reuse identical orphan snapshot");
    }

    if (storage != NULL) {
        failed |= expect(tr_raft_wal_storage_close(storage) == SALTS_OK,
                         "close storage");
        storage = NULL;
    }

    config.create_if_missing = false;
    failed |= expect(
        tr_raft_wal_storage_open(&config, &storage) == SALTS_OK,
        "final reopen");
    if (storage != NULL) {
        memset(&recovery, 0, sizeof(recovery));
        failed |= expect(
            tr_raft_wal_storage_load(storage, &recovery) == SALTS_OK,
            "final recovery");
        failed |= expect(recovery.snapshot_index == 1U &&
                             recovery.snapshot_term == 1U &&
                             recovery.commit_index == 1U,
                         "successful retry establishes snapshot boundary");
        tr_raft_wal_recovery_destroy(&recovery);
        failed |= expect(tr_raft_wal_storage_close(storage) == SALTS_OK,
                         "close final storage");
    }

    cleanup_prefix(prefix);
    if (failed != 0) {
        return 1;
    }
    puts(pre_publish
             ? "PASS: snapshot pre-publication failure remains retryable"
             : "PASS: uncertain snapshot publication faults until reopen");
    return 0;
}
