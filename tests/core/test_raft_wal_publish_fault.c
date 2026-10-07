#include <turboraft/raft_wal_storage.h>

#include "raft_wal_storage_internal.h"

#include <cmeta_error.h>
#include <cmeta_fs.h>

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

typedef enum publish_fault_mode {
    PUBLISH_FAULT_STAGING_FSYNC = 1,
    PUBLISH_FAULT_DIRECTORY_FSYNC_UNKNOWN = 2
} publish_fault_mode_t;

typedef struct publish_fault_provider {
    publish_fault_mode_t mode;
    int fault_manifest;
    size_t calls;
    size_t snapshot_calls;
    size_t segment_calls;
    size_t manifest_calls;
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
    if (cmeta_fs_access(path, SALTS_FS_ACCESS_EXISTS) == SALTS_OK) {
        (void)cmeta_fs_unlink(path);
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
    (void)snprintf(path, sizeof(path), "%s.manifest", prefix);
    unlink_if_exists(path);
    (void)snprintf(path, sizeof(path), "%s.manifest.tmp", prefix);
    unlink_if_exists(path);
    (void)snprintf(path, sizeof(path), "%s.lock", prefix);
    unlink_if_exists(path);
}

static int injected_replace_durable(
    void *context,
    const char *staging_path,
    const char *destination_path,
    cmeta_fs_replace_state_t *state)
{
    publish_fault_provider_t *provider =
        (publish_fault_provider_t *)context;

    if (provider == NULL || state == NULL) {
        return SALTS_EINVAL;
    }
    ++provider->calls;
    if (strstr(destination_path, ".snapshot.") != NULL) {
        ++provider->snapshot_calls;
    } else if (strstr(destination_path, ".manifest") != NULL) {
        ++provider->manifest_calls;
    } else if (strstr(destination_path, ".wal") != NULL) {
        ++provider->segment_calls;
    }

    {
        int target_snapshot =
            !provider->fault_manifest &&
            provider->snapshot_calls == 1U &&
            strstr(destination_path, ".snapshot.") != NULL;
        int target_manifest =
            provider->fault_manifest &&
            provider->manifest_calls == 1U &&
            strstr(destination_path, ".manifest") != NULL;

        if (!target_snapshot && !target_manifest) {
            return cmeta_fs_replace_durable(
                staging_path, destination_path, state);
        }
    }

    *state = SALTS_FS_REPLACE_NOT_PUBLISHED;
    if (provider->mode == PUBLISH_FAULT_STAGING_FSYNC) {
        /*
         * Model cmeta_fs_replace_durable() failing its staging-file fsync:
         * no namespace publication occurred and the staging path remains
         * caller-owned.
         */
        return SALTS_EIO;
    }
    if (provider->mode == PUBLISH_FAULT_DIRECTORY_FSYNC_UNKNOWN) {
        int result = cmeta_fs_rename(staging_path, destination_path);
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
        (strcmp(argv[1], "staging-fsync") != 0 &&
         strcmp(argv[1], "directory-fsync") != 0 &&
         strcmp(argv[1], "manifest-staging-fsync") != 0 &&
         strcmp(argv[1], "manifest-directory-fsync") != 0)) {
        fprintf(stderr,
                "usage: %s staging-fsync|directory-fsync|"
                "manifest-staging-fsync|manifest-directory-fsync\n",
                argv[0]);
        return 2;
    }
    memset(&provider, 0, sizeof(provider));
    provider.fault_manifest =
        strncmp(argv[1], "manifest-", 9U) == 0;
    provider.mode =
        strstr(argv[1], "directory-fsync") != NULL
            ? PUBLISH_FAULT_DIRECTORY_FSYNC_UNKNOWN
            : PUBLISH_FAULT_STAGING_FSYNC;

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
                     "injected durable publication fault must surface EIO");
    if (provider.fault_manifest) {
        failed |= expect(provider.snapshot_calls == 1U &&
                             provider.segment_calls == 1U &&
                             provider.manifest_calls == 1U,
                         "manifest fault occurs after durable snapshot and checkpoint segment publication");
    } else {
        failed |= expect(provider.snapshot_calls == 1U &&
                             provider.segment_calls == 0U &&
                             provider.manifest_calls == 0U,
                         "snapshot fault targets the first snapshot publication");
    }

    if (provider.fault_manifest) {
        failed |= expect(
            cmeta_fs_access(snapshot_path, SALTS_FS_ACCESS_EXISTS) == SALTS_OK,
            "manifest failure leaves complete snapshot bytes visible");
        failed |= expect(
            tr_raft_wal_storage_store_snapshot(
                storage, 1U, 1U, &configuration,
                snapshot, sizeof(snapshot)) == SALTS_EIO,
            "manifest publication failure faults the live storage owner");

        failed |= expect(tr_raft_wal_storage_close(storage) == SALTS_OK,
                         "close manifest-faulted storage");
        storage = NULL;
        config.create_if_missing = false;
        failed |= expect(
            tr_raft_wal_storage_open(&config, &storage) == SALTS_OK,
            "reopen after manifest publication failure");

        memset(&recovery, 0, sizeof(recovery));
        failed |= expect(
            tr_raft_wal_storage_load(storage, &recovery) == SALTS_OK,
            "recover authoritative range after manifest failure");
        if (provider.mode == PUBLISH_FAULT_STAGING_FSYNC) {
            failed |= expect(
                recovery.snapshot_index == 0U &&
                    recovery.commit_index == 1U &&
                    recovery.entry_count == 1U,
                "unpublished manifest preserves the old authoritative WAL");
        } else {
            failed |= expect(
                recovery.snapshot_index == 1U &&
                    recovery.snapshot_term == 1U &&
                    recovery.commit_index == 1U &&
                    recovery.entry_count == 0U,
                "uncertain manifest may make the complete checkpoint authoritative");
        }
        tr_raft_wal_recovery_destroy(&recovery);

        if (provider.mode == PUBLISH_FAULT_STAGING_FSYNC) {
            failed |= expect(
                tr_raft_wal_storage_store_snapshot(
                    storage, 1U, 1U, &configuration,
                    snapshot, sizeof(snapshot)) == SALTS_OK,
                "retry after unpublished manifest may safely publish checkpoint range");
        }
    } else if (provider.mode == PUBLISH_FAULT_STAGING_FSYNC) {
        failed |= expect(
            cmeta_fs_access(snapshot_path, SALTS_FS_ACCESS_EXISTS) != SALTS_OK,
            "pre-publication fault must not publish snapshot");
        failed |= expect(
            tr_raft_wal_storage_store_snapshot(
                storage, 1U, 1U, &configuration,
                snapshot, sizeof(snapshot)) == SALTS_OK,
            "pre-publication failure must remain retryable");
        failed |= expect(provider.snapshot_calls == 2U &&
                             provider.segment_calls == 1U &&
                             provider.manifest_calls == 1U,
                         "retry publishes snapshot, checkpoint segment and manifest exactly once");
    } else {
        failed |= expect(
            cmeta_fs_access(snapshot_path, SALTS_FS_ACCESS_EXISTS) == SALTS_OK,
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
    if (provider.fault_manifest) {
        puts(provider.mode == PUBLISH_FAULT_STAGING_FSYNC
                 ? "PASS: manifest staging fsync failure preserves old authoritative WAL"
                 : "PASS: manifest directory fsync uncertainty reopens one complete authoritative range");
    } else {
        puts(provider.mode == PUBLISH_FAULT_STAGING_FSYNC
                 ? "PASS: staging fsync failure remains retryable"
                 : "PASS: parent-directory fsync uncertainty faults until reopen");
    }
    return 0;
}
