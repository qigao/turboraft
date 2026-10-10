#define _POSIX_C_SOURCE 200809L

#include <turboraft/raft_core.h>
#include <turboraft/raft_wal_storage.h>
#include <cmeta_error.h>

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <poll.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

/*
 * #149 P0 ONLY: three concurrently exec'ed RaftCore + WalStorage owners.
 *
 * Each child has a distinct PID, its own initialized Core, fsynced WAL,
 * process-visible exclusive file lock and private controller socket.
 * Child 2 contains a test-SEEDED uncommitted index1 (NOT an elected
 * leader/replication result). Kill that child without unwinding ownership,
 * restart exactly the same WAL, and prove its uncommitted entry stays
 * uncommitted while Node1/Node3 remain alive with their own WALs.
 *
 * Controller socket messages contain ONLY copied scalar health snapshots.
 * There is NO controller Raft message forwarding, no invented consensus
 * ACK, no native handle or Core state persistence, and NO mTLS yet.
 * Real three-process CNet consensus is a later #149 acceptance gate.
 */
enum { NODE_COUNT = 3, IPC_TIMEOUT_MS = 8000, LOG_LIMIT = 32 };
#define IPC_MAGIC UINT32_C(0x54523350)
static const tr_raft_node_id_t VOTERS[NODE_COUNT] = {1U, 2U, 3U};

typedef struct worker_reply {
    uint32_t magic;
    uint32_t op;
    uint64_t node;
    uint64_t pid;
    uint64_t term;
    uint64_t voted_for;
    uint64_t last_log_index;
    uint64_t commit_index;
    uint64_t applied_index;
    uint64_t seeded_command_id;
} worker_reply_t;

typedef struct child_process {
    pid_t pid;
    int control_fd;
    char wal_prefix[512];
} child_process_t;

static int send_all(int fd, const void *bytes, size_t size)
{
    const uint8_t *data = (const uint8_t *)bytes;
    while (size != 0U) {
        ssize_t n = send(fd, data, size, MSG_NOSIGNAL);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return -1;
        data += (size_t)n;
        size -= (size_t)n;
    }
    return 0;
}

static int recv_all(int fd, void *bytes, size_t size)
{
    uint8_t *data = (uint8_t *)bytes;
    while (size != 0U) {
        ssize_t n = recv(fd, data, size, 0);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return -1;
        data += (size_t)n;
        size -= (size_t)n;
    }
    return 0;
}

static tr_raft_wal_storage_config_t wal_config(const char *prefix,
                                                int create)
{
    tr_raft_wal_storage_config_t config = {0};
    config.path_prefix = prefix;
    config.segment_bytes = TR_RAFT_WAL_MIN_SEGMENT_BYTES;
    config.max_transaction_bytes = 8U * 1024U;
    config.max_live_segments = 4U;
    config.max_log_entries = LOG_LIMIT;
    config.max_snapshot_bytes = 1024U;
    config.create_if_missing = create != 0;
    return config;
}

/* This one pre-seeded uncommitted log is a fixture, NOT a majority commit.
 * WAL's atomic transaction makes it independently fsynced. The actual
 * Raft Core starts only AFTER loading these authoritative WAL values. */
static int seed_wal(tr_raft_wal_storage_t *wal, unsigned node_id)
{
    tr_raft_storage_t storage = {0};
    tr_raft_entry_t entry = {0};
    int result = tr_raft_wal_storage_bind(wal, &storage);
    if (result != SALTS_OK) return result;
    result = storage.begin(storage.context);
    if (result != SALTS_OK) return result;
    result = storage.write_hard_state(storage.context, 1U, node_id);
    if (result == SALTS_OK && node_id == 2U) {
        entry.index = 1U;
        entry.term = 1U;
        entry.command_id = UINT64_C(149001);
        entry.data_length = 11U;
        memcpy(entry.data, "uncommitted", 11U);
        result = storage.append_log(storage.context, &entry, 1U);
    }
    if (result == SALTS_OK) return storage.commit(storage.context);
    (void)storage.rollback(storage.context);
    return result;
}

static int load_core(tr_raft_wal_storage_t *wal, unsigned node_id,
                     tr_raft_core_t **out_core, uint64_t *command_id)
{
    tr_raft_wal_recovery_t recovered = {0};
    tr_raft_core_config_t config = {0};
    tr_raft_status_t status = {0};
    int result = tr_raft_wal_storage_load(wal, &recovered);
    if (result != SALTS_OK) return result;
    *command_id = 0U;
    if (recovered.term != 1U || recovered.voted_for != node_id ||
        recovered.commit_index != 0U ||
        recovered.snapshot_index != 0U ||
        recovered.entry_count != (node_id == 2U ? 1U : 0U)) {
        tr_raft_wal_recovery_destroy(&recovered);
        return SALTS_EPROTO;
    }
    if (node_id == 2U) {
        const tr_raft_entry_t *entry = &recovered.entries[0];
        if (entry->index != 1U || entry->term != 1U ||
            entry->command_id != UINT64_C(149001) ||
            entry->data_length != 11U ||
            memcmp(entry->data, "uncommitted", 11U) != 0) {
            tr_raft_wal_recovery_destroy(&recovered);
            return SALTS_EPROTO;
        }
        *command_id = entry->command_id;
    }
    config.self_id = node_id;
    config.voters = VOTERS;
    config.voter_count = NODE_COUNT;
    config.heartbeat_ticks = 1U;
    config.election_min_ticks = 3U;
    config.election_max_ticks = 6U;
    config.initial_election_timeout_ticks = node_id + 2U;
    config.initial_term = recovered.term;
    config.initial_vote = recovered.voted_for;
    config.initial_log_entries = recovered.entries;
    config.initial_log_entry_count = recovered.entry_count;
    config.initial_commit_index = recovered.commit_index;
    config.initial_applied_index = 0U;
    config.max_log_entries = LOG_LIMIT;
    result = tr_raft_core_create(&config, out_core);
    tr_raft_wal_recovery_destroy(&recovered);
    if (result != SALTS_OK) return result;
    result = tr_raft_core_status(*out_core, &status);
    if (result != SALTS_OK || status.role != TR_RAFT_FOLLOWER ||
        status.term != 1U || status.commit_index != 0U ||
        status.applied_index != 0U ||
        status.last_log_index != (node_id == 2U ? 1U : 0U))
        return SALTS_EPROTO;
    return SALTS_OK;
}

static int child_reply(int control_fd, tr_raft_core_t *core,
                       unsigned node_id, uint32_t op, uint64_t command_id)
{
    worker_reply_t reply = {0};
    tr_raft_status_t status = {0};
    if (tr_raft_core_status(core, &status) != SALTS_OK) return -1;
    reply.magic = IPC_MAGIC;
    reply.op = op;
    reply.node = node_id;
    reply.pid = (uint64_t)getpid();
    reply.term = status.term;
    reply.voted_for = status.voted_for;
    reply.last_log_index = status.last_log_index;
    reply.commit_index = status.commit_index;
    reply.applied_index = status.applied_index;
    reply.seeded_command_id = command_id;
    return send_all(control_fd, &reply, sizeof(reply));
}

static int worker_main(unsigned node_id, const char *wal_prefix,
                       int recovering, int control_fd)
{
    tr_raft_wal_storage_config_t config = wal_config(wal_prefix,!recovering);
    tr_raft_wal_storage_t *wal = NULL;
    tr_raft_core_t *core = NULL;
    uint64_t command_id = 0U;
    unsigned char command;
    int rc = tr_raft_wal_storage_open(&config, &wal);
    if (rc != SALTS_OK) goto done;
    if (!recovering) {
        rc = seed_wal(wal, node_id);
        if (rc != SALTS_OK) goto done;
    }
    rc = load_core(wal, node_id, &core, &command_id);
    if (rc != SALTS_OK) goto done;
    if (child_reply(control_fd, core, node_id, 'R', command_id) != 0) {
        rc = SALTS_EPROTO;
        goto done;
    }
    while (recv_all(control_fd, &command, 1U) == 0) {
        if (command != 'S' && command != 'Q') {
            rc = SALTS_EPROTO;
            goto done;
        }
        if (child_reply(control_fd, core, node_id, command,
                        command_id) != 0) {
            rc = SALTS_EPROTO;
            goto done;
        }
        if (command == 'Q') break;
    }
done:
    tr_raft_core_destroy(core);
    if (wal != NULL) {
        int closed = tr_raft_wal_storage_close(wal);
        if (rc == SALTS_OK) rc = closed;
    }
    (void)close(control_fd);
    if (rc != SALTS_OK)
        fprintf(stderr, "three-process WAL worker %u failed: %d\n",
                node_id, rc);
    return rc == SALTS_OK ? 0 : 1;
}

static int recv_reply_timed(int fd, worker_reply_t *reply)
{
    struct pollfd ready = {fd, POLLIN, 0};
    int rc;
    do { rc = poll(&ready, 1, IPC_TIMEOUT_MS); }
    while (rc < 0 && errno == EINTR);
    if (rc != 1 || (ready.revents & POLLIN) == 0) return -1;
    return recv_all(fd, reply, sizeof(*reply));
}

static int command_snapshot(child_process_t *child, unsigned node_id,
                            unsigned char command, worker_reply_t *reply)
{
    if (command != 'R' && send_all(child->control_fd,&command,1U) != 0)
        return -1;
    if (recv_reply_timed(child->control_fd,reply) != 0 ||
        reply->magic != IPC_MAGIC || reply->op != command ||
        reply->node != node_id || reply->pid != (uint64_t)child->pid ||
        reply->term != 1U || reply->voted_for != node_id ||
        reply->last_log_index != (node_id == 2U ? 1U : 0U) ||
        reply->commit_index != 0U || reply->applied_index != 0U ||
        reply->seeded_command_id !=
            (node_id == 2U ? UINT64_C(149001) : 0U))
        return -1;
    return 0;
}

static int start_child(const char *program, unsigned node_id,
                       const char *prefix, int recovering,
                       child_process_t *child, worker_reply_t *ready)
{
    char node_arg[16], fd_arg[16];
    int sockets[2] = {-1,-1};
    pid_t pid;
    if (socketpair(AF_UNIX,SOCK_STREAM,0,sockets) != 0) return -1;
    pid = fork();
    if (pid < 0) {
        (void)close(sockets[0]); (void)close(sockets[1]);
        return -1;
    }
    if (pid == 0) {
        (void)close(sockets[0]);
        (void)snprintf(node_arg,sizeof(node_arg),"%u",node_id);
        (void)snprintf(fd_arg,sizeof(fd_arg),"%d",sockets[1]);
        execl(program,program,"--node",node_arg,prefix,
              recovering ? "recover" : "create",fd_arg,(char *)NULL);
        _exit(127);
    }
    (void)close(sockets[1]);
    if (fcntl(sockets[0],F_SETFD,FD_CLOEXEC) < 0) {
        (void)kill(pid,SIGKILL);
        (void)waitpid(pid,NULL,0);
        (void)close(sockets[0]);
        return -1;
    }
    child->pid = pid;
    child->control_fd = sockets[0];
    if (command_snapshot(child,node_id,'R',ready) != 0) return -1;
    return 0;
}

static void kill_and_reap(child_process_t *child)
{
    if (child->pid > 0) {
        (void)kill(child->pid,SIGKILL);
        while (waitpid(child->pid,NULL,0) < 0 && errno == EINTR) {}
        child->pid = -1;
    }
    if (child->control_fd >= 0) {
        (void)close(child->control_fd);
        child->control_fd = -1;
    }
}

static int graceful_stop(child_process_t *child, unsigned node_id)
{
    worker_reply_t snapshot = {0};
    int status = 0, waited;
    if (command_snapshot(child,node_id,'Q',&snapshot) != 0) return -1;
    do { waited = (int)waitpid(child->pid,&status,0); }
    while (waited < 0 && errno == EINTR);
    if (waited != (int)child->pid || !WIFEXITED(status) ||
        WEXITSTATUS(status) != 0) return -1;
    child->pid = -1;
    (void)close(child->control_fd);
    child->control_fd = -1;
    return 0;
}

static void cleanup_namespace(const char *root)
{
    unsigned node;
    for (node = 1U; node <= NODE_COUNT; ++node) {
        char directory[512];
        DIR *handle;
        struct dirent *entry;
        if (snprintf(directory,sizeof(directory),"%s/node%u",
                     root,node) < 0) continue;
        handle = opendir(directory);
        if (handle == NULL) continue;
        while ((entry = readdir(handle)) != NULL) {
            char file[1024];
            int n;
            if (strcmp(entry->d_name,".") == 0 ||
                strcmp(entry->d_name,"..") == 0) continue;
            n = snprintf(file,sizeof(file),"%s/%s",
                         directory,entry->d_name);
            if (n > 0 && (size_t)n < sizeof(file)) (void)unlink(file);
        }
        (void)closedir(handle);
        (void)rmdir(directory);
    }
    (void)rmdir(root);
}

static int controller_main(const char *program)
{
    const char *temp = getenv("TMPDIR");
    char root[512], directory[512];
    child_process_t children[NODE_COUNT] = {{0}};
    worker_reply_t reply = {0}, before = {0}, after = {0};
    tr_raft_wal_storage_t *duplicate_writer = NULL;
    tr_raft_wal_storage_config_t probe = {0};
    int rc, status, waited, failed = 0;
    unsigned i;
    if (temp == NULL || *temp == '\0') temp = "/tmp";
    if (snprintf(root,sizeof(root),"%s/turboraft-3wal-XXXXXX",
                 temp) >= (int)sizeof(root))
        return 1;
    if (mkdtemp(root) == NULL) return 1;
    for (i = 0U; i < NODE_COUNT; ++i) {
        children[i].pid = -1;
        children[i].control_fd = -1;
        if (snprintf(directory,sizeof(directory),"%s/node%u",
                     root,i+1U) >= (int)sizeof(directory) ||
            mkdir(directory,0700) != 0 ||
            snprintf(children[i].wal_prefix,
                     sizeof(children[i].wal_prefix),"%s/raft",
                     directory) >=
                (int)sizeof(children[i].wal_prefix)) {
            failed = 1;
            goto done;
        }
    }
    for (i = 0U; i < NODE_COUNT; ++i) {
        if (start_child(program,i+1U,children[i].wal_prefix,0,
                        &children[i],&reply) != 0) {
            failed = 1;
            goto done;
        }
    }
    if (children[0].pid == children[1].pid ||
        children[1].pid == children[2].pid ||
        children[0].pid == children[2].pid) {
        failed = 1;
        goto done;
    }
    /* A second simultaneous writer must be refused by the same WAL
     * locking contract, including across different OS processes. */
    probe = wal_config(children[1].wal_prefix,0);
    rc = tr_raft_wal_storage_open(&probe,&duplicate_writer);
    if (rc == SALTS_OK || duplicate_writer != NULL) {
        if (duplicate_writer != NULL)
            (void)tr_raft_wal_storage_close(duplicate_writer);
        failed = 1;
        goto done;
    }
    if (command_snapshot(&children[1],2U,'S',&before) != 0) {
        failed = 1;
        goto done;
    }
    /* The uncommitted WAL entry is ALREADY fsynced, but it must not
     * become committed after losing an entire process. No teardown. */
    if (kill(children[1].pid,SIGKILL) != 0) {
        failed = 1;
        goto done;
    }
    do { waited = (int)waitpid(children[1].pid,&status,0); }
    while (waited < 0 && errno == EINTR);
    if (waited != (int)children[1].pid ||
        !WIFSIGNALED(status) || WTERMSIG(status) != SIGKILL) {
        failed = 1;
        goto done;
    }
    children[1].pid = -1;
    (void)close(children[1].control_fd);
    children[1].control_fd = -1;
    /* Other process-local Core and WAL owners remain alive and unchanged. */
    if (command_snapshot(&children[0],1U,'S',&reply) != 0 ||
        command_snapshot(&children[2],3U,'S',&reply) != 0 ||
        start_child(program,2U,children[1].wal_prefix,1,
                    &children[1],&after) != 0 ||
        before.seeded_command_id != after.seeded_command_id ||
        before.last_log_index != after.last_log_index ||
        before.commit_index != after.commit_index ||
        command_snapshot(&children[0],1U,'S',&reply) != 0 ||
        command_snapshot(&children[2],3U,'S',&reply) != 0) {
        failed = 1;
        goto done;
    }
    for (i = 0U; i < NODE_COUNT; ++i) {
        if (graceful_stop(&children[i],i+1U) != 0) {
            failed = 1;
            goto done;
        }
    }
done:
    for (i = 0U; i < NODE_COUNT; ++i)
        kill_and_reap(&children[i]);
    cleanup_namespace(root);
    if (failed) {
        fputs("FAIL: three independently exec'ed Core/WAL owners, "
              "exclusive writer or SIGKILL recovery\n",stderr);
        return 1;
    }
    puts("PASS: three concurrent separate Core/WAL processes; "
         "distinct WAL writer locks; Node2 fsynced uncommitted log1; "
         "SIGKILL+exec reopens log1 without commit/apply; "
         "Node1+Node3 unchanged (no Raft network quorum tested)");
    return 0;
}

int main(int argc, char **argv)
{
    if (argc == 6 && strcmp(argv[1],"--node") == 0) {
        char *end = NULL;
        unsigned long id = strtoul(argv[2],&end,10);
        int control_fd;
        if (end == argv[2] || *end != '\0' || id < 1UL ||
            id > NODE_COUNT) return 2;
        control_fd = atoi(argv[5]);
        if (control_fd < 3) return 2;
        if (strcmp(argv[4],"create") != 0 &&
            strcmp(argv[4],"recover") != 0) return 2;
        return worker_main((unsigned)id,argv[3],
                           strcmp(argv[4],"recover") == 0,
                           control_fd);
    }
    if (argc != 1) return 2;
    return controller_main(argv[0]);
}
