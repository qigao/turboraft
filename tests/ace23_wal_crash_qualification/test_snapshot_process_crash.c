#define _POSIX_C_SOURCE 200809L

#include <turboraft/raft_wal_storage.h>

#include "raft_wal_storage_internal.h" /* private fault seam, NOT installed ABI */

#include <cmeta_error.h>
#include <cmeta_fs.h>

#include <errno.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

/*
 * This is a real process-death / restart test, not a simulated EIO test
 * and not a power-loss/fsync hardware qualification.
 *
 * Each CTest launches a fresh writer child using exec (never fork with an
 * inherited live WAL owner). The private replacement provider kills it at
 * one precisely chosen namespace-publication boundary. A new process
 * reopens the official installed WalStorage SDK and checks the authoritative
 * manifest range, snapshot digest/bytes, and subsequent committed WAL suffix.
 */
static const tr_raft_conf_t CONFIGURATION = {
    TR_RAFT_CONF_FINAL, 1U, 1U,
    {{1U, TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER}}
};
static const uint8_t SNAPSHOT_BYTES[] = {0x10U, 0x20U, 0x30U, 0x40U};

typedef struct crash_point {
    const char *phase;
    int after_replace;
} crash_point_t;

static int expect(int yes, const char *message)
{
    if (yes) return 0;
    fprintf(stderr, "FAIL: %s\n", message);
    return 1;
}

static tr_raft_wal_storage_config_t storage_config(
    const char *prefix, int create)
{
    tr_raft_wal_storage_config_t config;
    memset(&config, 0, sizeof(config));
    config.path_prefix = prefix;
    config.segment_bytes = TR_RAFT_WAL_MIN_SEGMENT_BYTES;
    config.max_transaction_bytes = 8U * 1024U;
    config.max_live_segments = 4U;
    config.max_log_entries = 16U;
    config.max_snapshot_bytes = 1024U;
    config.create_if_missing = create != 0;
    return config;
}

static void cleanup_path(const char *path)
{
    if (cmeta_fs_access(path, SALTS_FS_ACCESS_EXISTS) == SALTS_OK)
        (void)cmeta_fs_unlink(path);
}

static void cleanup_prefix(const char *prefix)
{
    char path[SALTS_FS_MAX_PATH];
    size_t sequence;
    for (sequence = 1U; sequence <= 4U; ++sequence) {
        (void)snprintf(path, sizeof(path), "%s.%08zu.wal", prefix, sequence);
        cleanup_path(path);
        (void)snprintf(path, sizeof(path), "%s.%08zu.wal.tmp",
                       prefix, sequence);
        cleanup_path(path);
    }
    (void)snprintf(path, sizeof(path), "%s.snapshot.1.1", prefix);
    cleanup_path(path);
    (void)snprintf(path, sizeof(path), "%s.snapshot.1.1.tmp", prefix);
    cleanup_path(path);
    (void)snprintf(path, sizeof(path), "%s.manifest", prefix);
    cleanup_path(path);
    (void)snprintf(path, sizeof(path), "%s.manifest.tmp", prefix);
    cleanup_path(path);
    (void)snprintf(path, sizeof(path), "%s.lock", prefix);
    cleanup_path(path);
}

static int append_entry(tr_raft_wal_storage_t *storage,
                        tr_raft_index_t index)
{
    tr_raft_storage_t adapter = {0};
    tr_raft_entry_t entry = {0};
    int result;

    entry.index = index;
    entry.term = 1U;
    entry.command_id = index;
    entry.data_length = 3U;
    memcpy(entry.data, index == 1U ? "one" : "two", 3U);

    result = tr_raft_wal_storage_bind(storage, &adapter);
    if (result != SALTS_OK) return result;
    result = adapter.begin(adapter.context);
    if (result != SALTS_OK) return result;
    if (index == 1U)
        result = adapter.write_hard_state(adapter.context, 1U, 1U);
    if (result == SALTS_OK)
        result = adapter.append_log(adapter.context, &entry, 1U);
    if (result == SALTS_OK)
        result = adapter.write_commit_index(adapter.context, index);
    if (result == SALTS_OK)
        return adapter.commit(adapter.context);
    (void)adapter.rollback(adapter.context);
    return result;
}

static int selected_phase(const char *destination, const char *phase)
{
    if (strcmp(phase, "snapshot") == 0)
        return strstr(destination, ".snapshot.") != NULL;
    if (strcmp(phase, "segment") == 0)
        return strstr(destination, ".wal") != NULL;
    return strstr(destination, ".manifest") != NULL;
}

static int kill_on_replace(void *context, const char *staging,
                           const char *destination,
                           cmeta_fs_replace_state_t *state)
{
    const crash_point_t *point = (const crash_point_t *)context;
    int result;

    if (point == NULL || staging == NULL || destination == NULL ||
        state == NULL) return SALTS_EINVAL;
    if (!selected_phase(destination, point->phase))
        return cmeta_fs_replace_durable(staging, destination, state);

    if (point->after_replace) {
        result = cmeta_fs_replace_durable(staging, destination, state);
        if (result != SALTS_OK) _exit(92);
    }
    /* Uncatchable death: no WAL close, DSO drain, destructor, or retry. */
    if (kill(getpid(), SIGKILL) != 0) _exit(93);
    _exit(94);
}

static int child_writer(const char *prefix, const char *phase, int after)
{
    tr_raft_wal_storage_config_t config = storage_config(prefix, 0);
    tr_raft_wal_storage_t *storage = NULL;
    crash_point_t point = {phase, after};
    int result = tr_raft_wal_storage_open(&config, &storage);
    if (result != SALTS_OK) {
        fprintf(stderr, "child reopen failed: %d\n", result);
        _exit(81);
    }
    result = tr_raft_wal_storage_set_replace_durable_for_test(
        storage, kill_on_replace, &point);
    if (result != SALTS_OK) _exit(82);
    result = tr_raft_wal_storage_install_snapshot(
        storage, 1U, 1U, 1U, &CONFIGURATION,
        SNAPSHOT_BYTES, sizeof(SNAPSHOT_BYTES));
    fprintf(stderr, "child failed to reach crash point: %d\n", result);
    _exit(83);
}

static int verify_snapshot(const tr_raft_wal_recovery_t *recovery)
{
    uint8_t data[sizeof(SNAPSHOT_BYTES)] = {0};
    size_t received = 0U;
    if (recovery->snapshot_index != 1U ||
        recovery->snapshot_term != 1U ||
        !recovery->has_snapshot_configuration ||
        recovery->snapshot_configuration.member_count != 1U ||
        recovery->snapshot_configuration.members[0].node_id != 1U ||
        recovery->snapshot_size != sizeof(SNAPSHOT_BYTES) ||
        recovery->snapshot_source.read_at == NULL) {
        return expect(0, "published snapshot metadata and source");
    }
    if (recovery->snapshot_source.read_at(
            recovery->snapshot_source.context, 0U, data,
            sizeof(data), &received) != SALTS_OK) {
        return expect(0, "read published snapshot after process restart");
    }
    return expect(received == sizeof(data) &&
                  memcmp(data, SNAPSHOT_BYTES, sizeof(data)) == 0,
                  "snapshot bytes survive real process death");
}

static int run_case(const char *program, const char *phase, int after)
{
    tr_raft_wal_storage_t *storage = NULL;
    tr_raft_wal_recovery_t recovery;
    tr_raft_wal_storage_config_t config;
    char prefix[SALTS_FS_MAX_PATH];
    const char *temp = getenv("TMPDIR");
    pid_t child;
    int status = 0;
    int failed = 0;
    int result;
    int expected_new = strcmp(phase, "manifest") == 0 && after;

    if (temp == NULL || *temp == '\0') temp = "/tmp";
    result = snprintf(prefix, sizeof(prefix),
                      "%s/turboraft-ace23-crash-%ld-%s-%s",
                      temp, (long)getpid(), phase,
                      after ? "after" : "before");
    if (result < 0 || (size_t)result >= sizeof(prefix))
        return expect(0, "temporary fixture prefix fits");

    cleanup_prefix(prefix);
    config = storage_config(prefix, 1);
    failed |= expect(tr_raft_wal_storage_open(&config, &storage) ==
                     SALTS_OK, "initial open");
    if (storage == NULL) goto done;
    failed |= expect(append_entry(storage, 1U) == SALTS_OK,
                     "fsync initial committed WAL entry");
    failed |= expect(tr_raft_wal_storage_close(storage) == SALTS_OK,
                     "close initial writer before launching child");
    storage = NULL;
    if (failed) goto done;

    child = fork();
    if (child == 0) {
        execl(program, program, "--child", prefix, phase,
              after ? "after" : "before", (char *)NULL);
        _exit(84);
    }
    if (child < 0) {
        failed |= expect(0, "fork new writer process");
        goto done;
    }
    do {
        result = waitpid(child, &status, 0);
    } while (result < 0 && errno == EINTR);
    failed |= expect(result == child && WIFSIGNALED(status) &&
                     WTERMSIG(status) == SIGKILL,
                     "writer must actually die by SIGKILL at chosen replace");
    if (failed) goto done;

    config.create_if_missing = false;
    failed |= expect(tr_raft_wal_storage_open(&config, &storage) == SALTS_OK,
                     "fresh owner reopens after killed writer");
    if (storage == NULL) goto done;

    memset(&recovery, 0, sizeof(recovery));
    result = tr_raft_wal_storage_load(storage, &recovery);
    failed |= expect(result == SALTS_OK, "replay authoritative manifest");
    if (result == SALTS_OK) {
        failed |= expect(recovery.term == 1U &&
                         recovery.commit_index == 1U,
                         "committed prefix survives crash");
        if (expected_new) {
            failed |= verify_snapshot(&recovery);
            failed |= expect(recovery.entry_count == 0U,
                             "new manifest excludes old compacted WAL");
        } else {
            failed |= expect(recovery.snapshot_index == 0U &&
                             recovery.entry_count == 1U &&
                             recovery.entries[0].index == 1U &&
                             recovery.entries[0].term == 1U,
                             "pre-manifest crash keeps old WAL authoritative");
        }
    }
    tr_raft_wal_recovery_destroy(&recovery);
    if (failed) goto done;

    if (!expected_new) {
        failed |= expect(tr_raft_wal_storage_install_snapshot(
            storage, 1U, 1U, 1U, &CONFIGURATION,
            SNAPSHOT_BYTES, sizeof(SNAPSHOT_BYTES)) == SALTS_OK,
            "recovery installs verified snapshot without stale replay");
    }
    if (!failed)
        failed |= expect(append_entry(storage, 2U) == SALTS_OK,
                         "catch-up commits next Raft entry after snapshot");
    failed |= expect(tr_raft_wal_storage_close(storage) == SALTS_OK,
                     "close after catch-up");
    storage = NULL;
    if (failed) goto done;

    failed |= expect(tr_raft_wal_storage_open(&config, &storage) == SALTS_OK,
                     "second real reopen validates catch-up durability");
    if (storage == NULL) goto done;
    memset(&recovery, 0, sizeof(recovery));
    result = tr_raft_wal_storage_load(storage, &recovery);
    failed |= expect(result == SALTS_OK, "load catch-up committed suffix");
    if (result == SALTS_OK) {
        failed |= verify_snapshot(&recovery);
        failed |= expect(recovery.commit_index == 2U &&
                         recovery.entry_count == 1U &&
                         recovery.entries[0].index == 2U &&
                         recovery.entries[0].term == 1U &&
                         recovery.entries[0].data_length == 3U &&
                         memcmp(recovery.entries[0].data, "two", 3U) == 0,
                         "new authoritative snapshot plus committed WAL suffix");
    }
    tr_raft_wal_recovery_destroy(&recovery);

done:
    if (storage != NULL)
        failed |= expect(tr_raft_wal_storage_close(storage) == SALTS_OK,
                         "final storage close");
    cleanup_prefix(prefix);
    if (!failed)
        printf("PASS: process SIGKILL %s %s; authoritative restart + WAL catch-up\n",
               phase, after ? "after" : "before");
    return failed ? 1 : 0;
}

int main(int argc, char **argv)
{
    const char *phase;
    const char *point;
    if (argc == 5 && strcmp(argv[1], "--child") == 0) {
        phase = argv[3];
        point = argv[4];
        if ((strcmp(phase, "snapshot") != 0 &&
             strcmp(phase, "segment") != 0 &&
             strcmp(phase, "manifest") != 0) ||
            (strcmp(point, "before") != 0 &&
             strcmp(point, "after") != 0)) return 2;
        return child_writer(argv[2], phase, strcmp(point, "after") == 0);
    }
    if (argc != 3 ||
        (strcmp(argv[1], "snapshot") != 0 &&
         strcmp(argv[1], "segment") != 0 &&
         strcmp(argv[1], "manifest") != 0) ||
        (strcmp(argv[2], "before") != 0 &&
         strcmp(argv[2], "after") != 0)) {
        fprintf(stderr,
                "usage: %s snapshot|segment|manifest before|after\n",
                argv[0]);
        return 2;
    }
    return run_case(argv[0], argv[1], strcmp(argv[2], "after") == 0);
}
