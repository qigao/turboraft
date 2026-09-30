#include <turboraft/raft_wal_storage.h>

#include "raft_wal_storage_internal.h"

#include <salts_error.h>
#include <salts_fs.h>

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

typedef enum publish_fault_mode {
    PUBLISH_FAULT_PRE = 1,
    PUBLISH_FAULT_UNKNOWN = 2
} publish_fault_mode_t;

typedef struct publish_fault_provider {
    publish_fault_mode_t mode;
    size_t calls;
} publish_fault_provider_t;

static int expect(int condition, const char *message)
{
    if (condition) {
        return 0;
    }
    fprintf(stderr, "FAIL: %s\n", message);
    return 1;
}

static void unlink_if_exists(const char *path)
{
    if (salts_fs_access(path, SALTS_FS_ACCESS_EXISTS) == SALTS_OK) {
        (void)salts_fs_unlink(path);
    }
}

static void cleanup_prefix(const char *prefix)
{
    char path[SALTS_FS_MAX_PATH];
    size_t sequence;

    for (sequence = 1U; sequence <= 4U; ++sequence) {
        (void)snprintf(path, sizeof(path), "%s.%08zu.wal",
                       prefix, sequence);
        unlink_if_exists(path);
        (void)snprintf(path, sizeof(path), "%s.%08zu.wal.tmp",
                       prefix, sequence);
        unlink_if_exists(path);
    }
    (void)snprintf(path, sizeof(path), "%s.snapshot.1.1", prefix);
    unlink_if_exists(path);
    (void)snprintf(path, sizeof(path), "%s.snapshot.1.1.tmp", prefix);
    unlink_if_exists(path);
    (void)snprintf(path, sizeof(path), "%s.lock", prefix);
    unlink_if_exists(path);
}

static int injected_replace_durable(
    void *context,
    const char *staging_path,
    const char *destination_path,
    salts_fs_replace_state_t *state)
{
    publish_fault_provider_t *provider =
        (publish_fault_provider_t *)context;

    if (provider == NULL || state == NULL) {
        return SALTS_EINVAL;
    }
    ++provider->calls;
    if (provider->calls != 1U) {
        return salts_fs_replace_durable(
            staging_path, destination_path, state);
    }

    *state = SALTS_FS_REPLACE_NOT_PUBLISHED;
    if (provider->mode == PUBLISH_FAULT_PRE) {
        return SALTS_EIO;
    }
    if (provider->mode == PUBLISH_FAULT_UNKNOWN) {
        int result = salts_fs_rename(staging_path, destination_path);
        if (result != SALTS_OK) {
            return result;
        }
        *state = SALTS_FS_REPLACE_DURABILITY_UNKNOWN;
        return SALTS_EIO;
    }
    return SALTS_EINVAL;
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

int main(int argc, char **argv)
{
    const tr_raft_conf_t configuration = {
        TR_RAFT_CONF_FINAL,
        1U,
        1U,
        {{1U, TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER}}
    };
    const uint8_t snapshot[] = {0x10U, 0x20U, 0x30U};
    publish_fault_provider_t provider;
    tr_raft_wal_storage_config_t config;
    tr_raft_wal_storage_t *storage = NULL;
    tr_raft_wal_recovery_t recovery;
    char prefix[SALTS_FS_MAX_PATH];
    char snapshot_path[SALTS_FS_MAX_PATH];
    int result;
    int failed = 0;

    if (argc != 2 ||
        (strcmp(argv[1], "pre") != 0 &&
         strcmp(argv[1], "unknown") != 0)) {
        fprintf(stderr, "usage: %s pre|unknown\n", argv[0]);
        return 2;
    }
    memset(&provider, 0, sizeof(provider));
    provider.mode = strcmp(argv[1], "pre") == 0
                        ? PUBLISH_FAULT_PRE
                        : PUBLISH_FAULT_UNKNOWN;

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
    failed |= expect(
        tr_raft_wal_storage_set_replace_durable_for_test(
            storage, injected_replace_durable, &provider) == SALTS_OK,
        "install private durable-publish provider");

    result = tr_raft_wal_storage_store_snapshot(
        storage, 1U, 1U, &configuration, snapshot, sizeof(snapshot));
    failed |= expect(result == SALTS_EIO,
                     "injected snapshot publication fault must surface EIO");
    failed |= expect(provider.calls == 1U,
                     "exactly one publication call must be injected");

    if (provider.mode == PUBLISH_FAULT_PRE) {
        failed |= expect(
            salts_fs_access(snapshot_path, SALTS_FS_ACCESS_EXISTS) != SALTS_OK,
            "pre-publication fault must not publish snapshot");
        failed |= expect(
            tr_raft_wal_storage_store_snapshot(
                storage, 1U, 1U, &configuration,
                snapshot, sizeof(snapshot)) == SALTS_OK,
            "pre-publication failure must remain retryable");
        failed |= expect(provider.calls == 3U,
                         "retry must publish snapshot and checkpoint segment exactly once");
    } else {
        failed |= expect(
            salts_fs_access(snapshot_path, SALTS_FS_ACCESS_EXISTS) == SALTS_OK,
            "uncertain publication leaves complete snapshot bytes visible");
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
                         "orphan snapshot is not authoritative before WAL commit");
        failed |= expect(recovery.commit_index == 1U &&
                             recovery.entry_count == 1U,
                         "last durable WAL prefix remains authoritative");
        tr_raft_wal_recovery_destroy(&recovery);

        failed |= expect(
            tr_raft_wal_storage_store_snapshot(
                storage, 1U, 1U, &configuration,
                snapshot, sizeof(snapshot)) == SALTS_OK,
            "reopen may reuse identical orphan snapshot safely");
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
                         "successful publication establishes snapshot boundary");
        tr_raft_wal_recovery_destroy(&recovery);
        failed |= expect(tr_raft_wal_storage_close(storage) == SALTS_OK,
                         "close final storage");
    }

    cleanup_prefix(prefix);
    if (failed != 0) {
        return 1;
    }
    puts(provider.mode == PUBLISH_FAULT_PRE
             ? "PASS: pre-publication failure remains retryable"
             : "PASS: uncertain publication faults until reopen");
    return 0;
}
