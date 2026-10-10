#define _POSIX_C_SOURCE 200809L

#include <turboraft/raft_wal_storage.h>
#include <cmeta_error.h>

#include <errno.h>
#include <limits.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

/*
 * Separate-process certified CNet/TLS + Group Owner + WAL recovery.
 *
 * This launcher itself owns no Raft, CNet, or Snapshot objects. It creates
 * a unique directory for the WAL namespace, then invokes two fresh exec'ed
 * test workers. No live handle, heap pointer, TLS epoch, ACK or callback
 * context can cross fork+exec.
 *
 * Worker N: real certified TLS Node3 sends Snapshot19 offset0..12, receives
 * one non-durable progress ACK from real Group103 SnapshotReceiver, and is
 * immediately killed by SIGKILL with NO teardown.
 *
 * Worker N+1: reopens the *same* WAL path with create_if_missing=false;
 * rejects any recovered partial/invented Snapshot state, establishes fresh
 * certified TLS and Group103 owners in a new process, explicitly transmits
 * the complete 12+12 Snapshot19, receives the legal durable final ACK, and
 * verifies Snapshot19 plus the subsequently fsynced Raft index20 suffix.
 *
 * OS process-loss test, not power loss or automatic multi-node convergence.
 */
/* The original WAL carries one fsynced committed Raft entry BEFORE either
 * certified TLS worker starts. Both actual child processes must reopen it
 * with create_if_missing=false, never infer progress from an empty WAL. */
static int seed_committed_wal(const char *prefix)
{
    tr_raft_wal_storage_config_t config = {0};
    tr_raft_wal_storage_t *wal = NULL;
    tr_raft_storage_t adapter = {0};
    tr_raft_entry_t entry = {0};
    int result;

    config.path_prefix = prefix;
    config.segment_bytes = TR_RAFT_WAL_MIN_SEGMENT_BYTES;
    config.max_transaction_bytes = 8U * 1024U;
    config.max_live_segments = 4U;
    config.max_log_entries = 16U;
    config.max_snapshot_bytes = 1024U;
    config.create_if_missing = true;
    result = tr_raft_wal_storage_open(&config, &wal);
    if (result != SALTS_OK) return result;
    entry.index = 1U;
    entry.term = 1U;
    entry.command_id = 1U;
    entry.data_length = 3U;
    memcpy(entry.data, "old", 3U);
    result = tr_raft_wal_storage_bind(wal, &adapter);
    if (result == SALTS_OK)
        result = adapter.begin(adapter.context);
    if (result == SALTS_OK)
        result = adapter.write_hard_state(adapter.context, 1U, 1U);
    if (result == SALTS_OK)
        result = adapter.append_log(adapter.context, &entry, 1U);
    if (result == SALTS_OK)
        result = adapter.write_commit_index(adapter.context, 1U);
    if (result == SALTS_OK)
        result = adapter.commit(adapter.context);
    else if (adapter.rollback != NULL)
        (void)adapter.rollback(adapter.context);
    {
        int close_result = tr_raft_wal_storage_close(wal);
        if (result == SALTS_OK) result = close_result;
    }
    return result;
}

static int check_worker(const char *worker, const char *prefix,
                        int expect_sigkill)
{
    pid_t child;
    int status = 0, waited;

    child = fork();
    if (child == 0) {
        execl(worker, worker, prefix, (char *)NULL);
        _exit(127);
    }
    if (child < 0) {
        perror("fork real TLS worker");
        return 1;
    }
    do {
        waited = (int)waitpid(child, &status, 0);
    } while (waited < 0 && errno == EINTR);
    if (waited != (int)child) {
        perror("waitpid");
        return 1;
    }
    if (expect_sigkill) {
        if (!WIFSIGNALED(status) || WTERMSIG(status) != SIGKILL) {
            fprintf(stderr,
                    "FAIL: first certified TLS/Owner/WAL process MUST "
                    "terminate by SIGKILL, status=%d\n", status);
            return 1;
        }
    } else if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        fprintf(stderr,
                "FAIL: fresh authenticated TLS recovery process "
                "did not validate WAL/ACK, status=%d\n", status);
        return 1;
    }
    return 0;
}

int main(int argc, char **argv)
{
    const char *temp = getenv("TMPDIR");
    char dir[1024], prefix[1200];
    int n, failed = 0;

    if (argc != 3 || argv[1] == NULL || argv[2] == NULL ||
        argv[1][0] == '\0' || argv[2][0] == '\0') {
        fprintf(stderr,
                "usage: %s certified-partial-writer certified-recovery-writer\n",
                argv[0]);
        return 2;
    }
    if (temp == NULL || temp[0] == '\0') temp = "/tmp";
    n = snprintf(dir, sizeof(dir),
                 "%s/turboraft-cnet-process-restart-%ld-XXXXXX",
                 temp, (long)getpid());
    if (n < 0 || (size_t)n >= sizeof(dir) || mkdtemp(dir) == NULL) {
        fprintf(stderr, "FAIL: create isolated same-WAL namespace\n");
        return 1;
    }
    n = snprintf(prefix, sizeof(prefix), "%s/authoritative", dir);
    if (n < 0 || (size_t)n >= sizeof(prefix)) {
        (void)rmdir(dir);
        fprintf(stderr, "FAIL: WAL prefix exceeds bounded namespace\n");
        return 1;
    }

    failed |= seed_committed_wal(prefix) != SALTS_OK;
    if (failed) {
        fprintf(stderr, "FAIL: cannot seed durable Raft term1/index1 WAL\n");
    } else {
        failed |= check_worker(argv[1], prefix, 1);
    }
    if (!failed) {
        /* The second worker MUST open the exact old namespace. The initial
         * killed process is not allowed to create a new WAL for recovery. */
        failed |= check_worker(argv[2], prefix, 0);
    }
    if (!failed && rmdir(dir) != 0) {
        perror("recovery worker left WAL/temporary owners behind");
        failed = 1;
    }
    if (failed) {
        fprintf(stderr, "Process restart acceptance failed; WAL fixture: %s\n",
                prefix);
        return 1;
    }
    printf("PASS: durable index1 baseline; real certified Node3 TLS partial ACK; SIGKILL whole "
           "Group/WAL process; fresh TLS+Owner reopens old WAL, explicit "
           "offset0 recovery, one durable Snapshot and committed suffix\n");
    return 0;
}
