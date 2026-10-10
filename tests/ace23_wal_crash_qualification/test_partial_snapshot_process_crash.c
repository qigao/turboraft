#define _POSIX_C_SOURCE 200809L

#include <turboraft/raft_snapshot_receiver.h>
#include <turboraft/raft_wal_storage.h>

#include <cmeta_crypto.h>
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
 * Real SIGKILL boundary for an UNFINISHED Raft Snapshot receive.
 *
 * Parent closes its initial WAL owner before fork+exec. A fresh writer
 * process consumes only offset 0..12 via the installed SnapshotReceiver,
 * then is killed by SIGKILL: no destroy, abort, WAL close, or graceful
 * shutdown. In streamed mode the private fixture fsyncs the 12 raw bytes
 * to prefix.rx.partial, but that file is NEVER a published WAL Snapshot.
 *
 * A fresh recovery process opens the installed WalStorage binary, verifies
 * the original WAL manifest and index1 log remain authoritative, explicitly
 * discards any orphan .rx.partial, rejects an attempt to resume with only
 * offset12..24, then explicitly retransmits from offset 0. Only full digest
 * validation + WAL install may make the new Snapshot authoritative.
 *
 * This qualifies process death on Linux, not power loss or network delivery.
 * No production runtime state, public fault API, implicit retry, new WAL
 * format, or automatic orphan-file salvage is introduced.
 */
enum { SNAPSHOT_SIZE = 24U, FIRST_SIZE = 12U };

static const tr_raft_conf_t CONFIGURATION = {
    TR_RAFT_CONF_FINAL, 1U, 1U,
    {{1U, TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER}}
};

typedef struct rx_stage {
    tr_raft_wal_storage_t *wal; /* borrowed from the single process owner */
    cmeta_file_t file;
    char path[SALTS_FS_MAX_PATH];
    uint64_t expected_size;
    size_t written;
    unsigned beginnings;
    unsigned commits;
    unsigned aborts;
    tr_raft_index_t snapshot_index;
    tr_raft_term_t leader_term;
    tr_raft_term_t snapshot_term;
    tr_raft_conf_t configuration;
    uint8_t digest[TR_RAFT_WIRE_SNAPSHOT_DIGEST_SIZE];
} rx_stage_t;

typedef struct rx_fixture {
    tr_raft_wal_storage_t *wal;
    tr_raft_snapshot_receiver_t *receiver;
    rx_stage_t stream;
    unsigned installed;
    int streaming;
} rx_fixture_t;

static int ensure(int condition, const char *why)
{
    if (condition) return 0;
    fprintf(stderr, "FAIL: %s\n", why);
    return 1;
}

static tr_raft_wal_storage_config_t wal_config(
    const char *prefix, int create)
{
    tr_raft_wal_storage_config_t config = {0};
    config.path_prefix = prefix;
    config.segment_bytes = TR_RAFT_WAL_MIN_SEGMENT_BYTES;
    config.max_transaction_bytes = 8U * 1024U;
    config.max_live_segments = 4U;
    config.max_log_entries = 16U;
    config.max_snapshot_bytes = 1024U;
    config.create_if_missing = create != 0;
    return config;
}

static void unlink_if_present(const char *path)
{
    if (cmeta_fs_access(path, SALTS_FS_ACCESS_EXISTS) == SALTS_OK)
        (void)cmeta_fs_unlink(path);
}

static void cleanup_prefix(const char *prefix)
{
    char path[SALTS_FS_MAX_PATH];
    size_t i;
    for (i = 1U; i <= 4U; ++i) {
        (void)snprintf(path, sizeof(path), "%s.%08zu.wal", prefix, i);
        unlink_if_present(path);
        (void)snprintf(path, sizeof(path), "%s.%08zu.wal.tmp", prefix, i);
        unlink_if_present(path);
    }
    (void)snprintf(path, sizeof(path), "%s.snapshot.1.1", prefix);
    unlink_if_present(path);
    (void)snprintf(path, sizeof(path), "%s.snapshot.1.1.tmp", prefix);
    unlink_if_present(path);
    (void)snprintf(path, sizeof(path), "%s.manifest", prefix);
    unlink_if_present(path);
    (void)snprintf(path, sizeof(path), "%s.manifest.tmp", prefix);
    unlink_if_present(path);
    (void)snprintf(path, sizeof(path), "%s.rx.partial", prefix);
    unlink_if_present(path);
    (void)snprintf(path, sizeof(path), "%s.lock", prefix);
    unlink_if_present(path);
}

static int append_log(tr_raft_wal_storage_t *wal, tr_raft_index_t index)
{
    tr_raft_storage_t adapter = {0};
    tr_raft_entry_t entry = {0};
    int rc;

    entry.index = index;
    entry.term = 1U;
    entry.command_id = index;
    entry.data_length = 3U;
    memcpy(entry.data, index == 1U ? "one" : "two", 3U);
    rc = tr_raft_wal_storage_bind(wal, &adapter);
    if (rc != SALTS_OK) return rc;
    rc = adapter.begin(adapter.context);
    if (rc != SALTS_OK) return rc;
    if (index == 1U)
        rc = adapter.write_hard_state(adapter.context, 1U, 1U);
    if (rc == SALTS_OK)
        rc = adapter.append_log(adapter.context, &entry, 1U);
    if (rc == SALTS_OK)
        rc = adapter.write_commit_index(adapter.context, index);
    if (rc == SALTS_OK)
        return adapter.commit(adapter.context);
    (void)adapter.rollback(adapter.context);
    return rc;
}

static int buffered_install(
    void *context, tr_raft_term_t leader_term,
    tr_raft_index_t index, tr_raft_term_t term,
    const tr_raft_conf_t *config, const uint8_t *data, size_t size)
{
    rx_fixture_t *fixture = (rx_fixture_t *)context;
    int result = tr_raft_wal_storage_install_snapshot(
        fixture->wal, leader_term, index, term, config, data, size);
    if (result == SALTS_OK) ++fixture->installed;
    return result;
}

static int stream_begin(
    void *context, tr_raft_term_t leader_term,
    tr_raft_index_t index, tr_raft_term_t term,
    const tr_raft_conf_t *config, uint64_t total)
{
    rx_stage_t *stage = (rx_stage_t *)context;
    if (stage == NULL || stage->file != SALTS_INVALID_FILE ||
        config == NULL || leader_term != 1U || index != 1U ||
        term != 1U || total != SNAPSHOT_SIZE ||
        config->member_count != 1U ||
        config->members[0].node_id != 1U)
        return SALTS_EPROTO;

    stage->file = cmeta_fs_open(
        stage->path,
        SALTS_FS_O_RDWR | SALTS_FS_O_CREAT | SALTS_FS_O_TRUNC,
        SALTS_FS_DEFAULT_MODE);
    if (stage->file == SALTS_INVALID_FILE) return SALTS_EIO;
    stage->expected_size = total;
    stage->written = 0U;
    stage->leader_term = leader_term;
    stage->snapshot_index = index;
    stage->snapshot_term = term;
    stage->configuration = *config;
    ++stage->beginnings;
    return SALTS_OK;
}

static int stream_write(
    void *context, uint64_t offset, const uint8_t *data, size_t count)
{
    rx_stage_t *stage = (rx_stage_t *)context;
    int written;
    if (stage == NULL || stage->file == SALTS_INVALID_FILE ||
        (count != 0U && data == NULL) || offset != stage->written ||
        count > stage->expected_size - offset)
        return SALTS_EPROTO;
    if (count != 0U) {
        written = cmeta_fs_pwrite(
            stage->file, data, count, (int64_t)offset);
        if (written != (int)count) return SALTS_EIO;
        stage->written += count;
    }
    /* The orphan is deliberately durable TEST DATA, not authoritative WAL.
     * Filesystem publishing/manifest ownership remains entirely with WAL. */
    return cmeta_fs_fsync(stage->file);
}

static int staged_source_read(
    void *context, uint64_t offset,
    uint8_t *buffer, size_t capacity, size_t *out_size)
{
    rx_stage_t *stage = (rx_stage_t *)context;
    int got;
    if (out_size == NULL) return SALTS_EINVAL;
    *out_size = 0U;
    if (stage == NULL || stage->file == SALTS_INVALID_FILE ||
        offset > stage->written ||
        capacity > stage->written - offset ||
        (capacity != 0U && buffer == NULL))
        return SALTS_EINVAL;
    if (capacity == 0U) return SALTS_OK;
    got = cmeta_fs_pread(
        stage->file, buffer, capacity, (int64_t)offset);
    if (got != (int)capacity) return SALTS_EIO;
    *out_size = capacity;
    return SALTS_OK;
}

static int stream_commit(void *context)
{
    rx_stage_t *stage = (rx_stage_t *)context;
    tr_raft_snapshot_source_t source = {0};
    int rc;
    if (stage == NULL || stage->file == SALTS_INVALID_FILE ||
        stage->written != stage->expected_size)
        return SALTS_EPROTO;
    rc = cmeta_fs_fsync(stage->file);
    if (rc != SALTS_OK) return rc;

    /* A verified complete staged source is consumed synchronously by the
     * production streaming WAL install. Never promote a partial orphan. */
    source.context = stage;
    source.size = stage->expected_size;
    source.read_at = staged_source_read;
    memcpy(source.digest, stage->digest, sizeof(source.digest));
    rc = tr_raft_wal_storage_install_snapshot_source(
        stage->wal, stage->leader_term, stage->snapshot_index,
        stage->snapshot_term, &stage->configuration, &source);
    if (rc != SALTS_OK) return rc;
    ++stage->commits;
    rc = cmeta_fs_close(stage->file);
    stage->file = SALTS_INVALID_FILE;
    if (rc != SALTS_OK) return rc;
    return cmeta_fs_unlink(stage->path);
}

static void stream_abort(void *context)
{
    rx_stage_t *stage = (rx_stage_t *)context;
    if (stage == NULL) return;
    ++stage->aborts;
    if (stage->file != SALTS_INVALID_FILE) {
        (void)cmeta_fs_close(stage->file);
        stage->file = SALTS_INVALID_FILE;
    }
    unlink_if_present(stage->path);
}

static int fixture_receiver_create(
    rx_fixture_t *fixture, const char *prefix, int streaming)
{
    tr_raft_snapshot_receiver_config_t config = {0};
    int result;
    fixture->streaming = streaming;
    fixture->stream.file = SALTS_INVALID_FILE;
    fixture->stream.wal = fixture->wal;
    if (snprintf(fixture->stream.path, sizeof(fixture->stream.path),
                 "%s.rx.partial", prefix) < 0) return SALTS_ERANGE;

    config.self_id = 1U;
    config.max_snapshot_bytes = 1024U;
    if (streaming) {
        config.stream.context = &fixture->stream;
        config.stream.begin = stream_begin;
        config.stream.write = stream_write;
        config.stream.commit = stream_commit;
        config.stream.abort = stream_abort;
    } else {
        config.max_buffered_snapshot_bytes = 1024U;
        config.install = buffered_install;
        config.install_context = fixture;
    }
    result = tr_raft_snapshot_receiver_create(
        &config, &fixture->receiver);
    return result;
}

static tr_raft_snapshot_chunk_t make_chunk(
    const uint8_t *bytes, const uint8_t *digest, int final)
{
    tr_raft_snapshot_chunk_t chunk = {0};
    chunk.from = 2U;
    chunk.to = 1U;
    chunk.term = 1U;
    chunk.snapshot_index = 1U;
    chunk.snapshot_term = 1U;
    chunk.snapshot_offset = final ? FIRST_SIZE : 0U;
    chunk.snapshot_size = SNAPSHOT_SIZE;
    chunk.has_configuration = !final;
    if (chunk.has_configuration) chunk.configuration = CONFIGURATION;
    chunk.data_length = SNAPSHOT_SIZE - FIRST_SIZE;
    chunk.data = bytes + chunk.snapshot_offset;
    chunk.done = final != 0;
    memcpy(chunk.snapshot_digest, digest, sizeof(chunk.snapshot_digest));
    return chunk;
}

static int load_authoritative(tr_raft_wal_storage_t *wal,
                              int installed, int with_suffix,
                              const uint8_t *expected)
{
    tr_raft_wal_recovery_t recovery = {0};
    uint8_t bytes[SNAPSHOT_SIZE] = {0};
    size_t received = 0U;
    int rc = tr_raft_wal_storage_load(wal, &recovery);
    int failed = ensure(rc == SALTS_OK, "replay authoritative WAL");
    if (rc != SALTS_OK) return failed;

    failed |= ensure(recovery.term == 1U &&
                     recovery.voted_for == 1U &&
                     recovery.commit_index == (with_suffix ? 2U : 1U),
                     "term/vote/committed prefix after process death");
    if (!installed) {
        failed |= ensure(recovery.snapshot_index == 0U &&
                         recovery.snapshot_size == 0U &&
                         recovery.entry_count == 1U &&
                         recovery.entries != NULL &&
                         recovery.entries[0].index == 1U &&
                         recovery.entries[0].term == 1U,
                         "partial receive is NEVER authoritative WAL");
    } else {
        failed |= ensure(recovery.snapshot_index == 1U &&
                         recovery.snapshot_term == 1U &&
                         recovery.snapshot_size == SNAPSHOT_SIZE &&
                         recovery.has_snapshot_configuration &&
                         recovery.snapshot_configuration.member_count == 1U &&
                         recovery.snapshot_configuration.members[0].node_id == 1U &&
                         recovery.entry_count == (with_suffix ? 1U : 0U),
                         "published Snapshot and WAL suffix exact boundaries");
        if (with_suffix && recovery.entry_count > 0U)
            failed |= ensure(recovery.entries[0].index == 2U &&
                             recovery.entries[0].term == 1U &&
                             recovery.entries[0].command_id == 2U &&
                             recovery.entries[0].data_length == 3U &&
                             memcmp(recovery.entries[0].data, "two", 3U) == 0,
                             "catch-up committed entry index2");
        if (recovery.snapshot_source.read_at == NULL)
            failed |= ensure(0, "Snapshot source restored from manifest");
        else {
            rc = recovery.snapshot_source.read_at(
                recovery.snapshot_source.context, 0U,
                bytes, sizeof(bytes), &received);
            failed |= ensure(rc == SALTS_OK &&
                             received == sizeof(bytes) &&
                             memcmp(bytes, expected, sizeof(bytes)) == 0,
                             "reopened Snapshot bytes match exactly");
        }
    }
    tr_raft_wal_recovery_destroy(&recovery);
    return failed;
}

static int child_partial(const char *prefix, int streaming)
{
    rx_fixture_t fixture = {0};
    tr_raft_wal_storage_config_t config = wal_config(prefix, 0);
    tr_raft_snapshot_receive_result_t received = {0};
    uint8_t bytes[SNAPSHOT_SIZE], digest[TR_RAFT_WIRE_SNAPSHOT_DIGEST_SIZE];
    tr_raft_snapshot_chunk_t first;
    int rc;

    memset(bytes, 0x7b, sizeof(bytes));
    rc = cmeta_sha256(bytes, sizeof(bytes), digest);
    if (rc != SALTS_OK) _exit(81);
    rc = tr_raft_wal_storage_open(&config, &fixture.wal);
    if (rc != SALTS_OK) _exit(82);
    rc = fixture_receiver_create(&fixture, prefix, streaming);
    if (rc != SALTS_OK) _exit(83);
    memcpy(fixture.stream.digest, digest, sizeof(digest));
    first = make_chunk(bytes, digest, 0);
    rc = tr_raft_snapshot_receiver_handle(fixture.receiver, &first, &received);
    if (rc != SALTS_OK || !received.ack.accepted ||
        received.ack.next_offset != FIRST_SIZE || received.installed ||
        fixture.installed != 0U || fixture.stream.commits != 0U ||
        load_authoritative(fixture.wal, 0, 0, bytes) != 0) {
        fprintf(stderr, "child never reached valid partial receive, rc=%d\n", rc);
        _exit(84);
    }
    if (streaming && (fixture.stream.written != FIRST_SIZE ||
                      fixture.stream.beginnings != 1U ||
                      fixture.stream.file == SALTS_INVALID_FILE))
        _exit(85);
    /* No callback unwind, no stream.abort, no graceful WalStorage close. */
    if (kill(getpid(), SIGKILL) != 0) _exit(86);
    _exit(87);
}

static int run_case(const char *program, const char *mode)
{
    tr_raft_wal_storage_config_t config;
    tr_raft_wal_storage_t *wal = NULL;
    rx_fixture_t fixture = {0};
    tr_raft_snapshot_receive_result_t received = {0};
    tr_raft_snapshot_chunk_t first, last;
    uint8_t bytes[SNAPSHOT_SIZE], digest[TR_RAFT_WIRE_SNAPSHOT_DIGEST_SIZE];
    char prefix[SALTS_FS_MAX_PATH], partial[SALTS_FS_MAX_PATH];
    const char *temp = getenv("TMPDIR");
    pid_t child;
    int status = 0, failed = 0, rc, streaming;

    streaming = strcmp(mode, "streamed") == 0;
    if (temp == NULL || *temp == '\0') temp = "/tmp";
    rc = snprintf(prefix, sizeof(prefix), "%s/turboraft-rx-crash-%ld-%s",
                  temp, (long)getpid(), mode);
    if (rc < 0 || (size_t)rc >= sizeof(prefix))
        return ensure(0, "short unique WAL prefix");
    rc = snprintf(partial, sizeof(partial), "%s.rx.partial", prefix);
    if (rc < 0 || (size_t)rc >= sizeof(partial))
        return ensure(0, "bounded staged path");
    memset(bytes, 0x7b, sizeof(bytes));
    failed |= ensure(cmeta_sha256(bytes, sizeof(bytes), digest) == SALTS_OK,
                     "trusted fixture digest");
    if (failed) return 1;

    cleanup_prefix(prefix);
    config = wal_config(prefix, 1);
    failed |= ensure(tr_raft_wal_storage_open(&config, &wal) == SALTS_OK,
                     "open baseline writer");
    if (wal == NULL) goto done;
    failed |= ensure(append_log(wal, 1U) == SALTS_OK,
                     "fsync initial committed WAL index1");
    failed |= ensure(tr_raft_wal_storage_close(wal) == SALTS_OK,
                     "release baseline writer lock before fork+exec");
    wal = NULL;
    if (failed) goto done;

    child = fork();
    if (child == 0) {
        execl(program, program, "--child", prefix, mode, (char *)NULL);
        _exit(88);
    }
    if (child < 0) {
        failed |= ensure(0, "launch independent snapshot writer process");
        goto done;
    }
    do {
        rc = waitpid(child, &status, 0);
    } while (rc < 0 && errno == EINTR);
    failed |= ensure(rc == child && WIFSIGNALED(status) &&
                     WTERMSIG(status) == SIGKILL,
                     "Snapshot receiver process MUST actually die by SIGKILL");
    if (failed) goto done;

    /* The streaming stage is a durable orphan of raw SNAPSHOT BYTES, not
     * a valid WAL checkpoint or serialized SnapshotReceiver runtime state. */
    if (streaming) {
        cmeta_file_t staged = cmeta_fs_open(partial, SALTS_FS_O_RDONLY, 0);
        uint8_t orphan[FIRST_SIZE] = {0};
        failed |= ensure(staged != SALTS_INVALID_FILE,
                         "streamed 12-byte temporary orphan really survived death");
        if (staged != SALTS_INVALID_FILE) {
            rc = cmeta_fs_pread(staged, orphan, sizeof(orphan), 0);
            failed |= ensure(rc == (int)sizeof(orphan) &&
                             memcmp(orphan, bytes, sizeof(orphan)) == 0,
                             "orphan is exactly partial bytes, NOT authority");
            failed |= ensure(cmeta_fs_close(staged) == SALTS_OK,
                             "close inspected orphan");
        }
    } else {
        failed |= ensure(cmeta_fs_access(partial, SALTS_FS_ACCESS_EXISTS)
                         != SALTS_OK,
                         "buffered receiver persisted no raw runtime state");
    }

    config.create_if_missing = false;
    failed |= ensure(tr_raft_wal_storage_open(&config, &wal) == SALTS_OK,
                     "fresh process reopens authoritative old WAL");
    if (wal == NULL) goto done;
    failed |= load_authoritative(wal, 0, 0, bytes);
    if (failed) goto done;

    /* Explicit policy: an orphan is NOT a checkpoint. Destroy it before a
     * new receiver starts; no automatic salvage, offset guess or retry. */
    if (streaming) {
        failed |= ensure(cmeta_fs_unlink(partial) == SALTS_OK,
                         "discard orphaned staged bytes explicitly");
        if (failed) goto done;
    }
    fixture.wal = wal;
    failed |= ensure(fixture_receiver_create(&fixture, prefix, streaming) ==
                     SALTS_OK, "create fresh non-restored SnapshotReceiver");
    if (fixture.receiver == NULL) goto done;
    memcpy(fixture.stream.digest, digest, sizeof(digest));
    first = make_chunk(bytes, digest, 0);
    last = make_chunk(bytes, digest, 1);

    rc = tr_raft_snapshot_receiver_handle(fixture.receiver, &last, &received);
    failed |= ensure(rc == SALTS_EPROTO &&
                     !received.ack.accepted && !received.installed,
                     "tail-only resume rejected after actual process death");
    failed |= load_authoritative(wal, 0, 0, bytes);
    if (failed) goto done;

    rc = tr_raft_snapshot_receiver_handle(fixture.receiver, &first, &received);
    failed |= ensure(rc == SALTS_OK && received.ack.accepted &&
                     received.ack.next_offset == FIRST_SIZE &&
                     !received.installed && fixture.installed == 0U &&
                     fixture.stream.commits == 0U,
                     "explicit offset0 restart remains non-durable progress");
    failed |= load_authoritative(wal, 0, 0, bytes);
    if (failed) goto done;

    rc = tr_raft_snapshot_receiver_handle(fixture.receiver, &last, &received);
    failed |= ensure(rc == SALTS_OK && received.ack.accepted &&
                     received.ack.next_offset == SNAPSHOT_SIZE &&
                     received.installed,
                     "final chunk alone validates and installs WAL Snapshot");
    failed |= ensure(streaming ? fixture.stream.commits == 1U :
                     fixture.installed == 1U,
                     "exactly one real WAL Snapshot install per transfer");
    failed |= load_authoritative(wal, 1, 0, bytes);
    if (failed) goto done;

    /* Duplicate after install must carry the installed truth, but cannot
     * invoke the already-completed WAL publication twice. */
    rc = tr_raft_snapshot_receiver_handle(fixture.receiver, &last, &received);
    failed |= ensure(rc == SALTS_OK && received.ack.accepted &&
                     received.ack.next_offset == SNAPSHOT_SIZE &&
                     received.installed &&
                     (streaming ? fixture.stream.commits == 1U :
                                  fixture.installed == 1U),
                     "completed duplicate ACK without repeated install");
    if (failed) goto done;

    tr_raft_snapshot_receiver_destroy(fixture.receiver);
    fixture.receiver = NULL;
    failed |= ensure(append_log(wal, 2U) == SALTS_OK,
                     "new committed Raft index2 suffix after explicit recovery");
    failed |= ensure(tr_raft_wal_storage_close(wal) == SALTS_OK,
                     "close recovered WAL owner before second reopen");
    wal = NULL;
    if (failed) goto done;
    failed |= ensure(tr_raft_wal_storage_open(&config, &wal) == SALTS_OK,
                     "second reopen verifies durable Snapshot plus catch-up");
    if (wal != NULL)
        failed |= load_authoritative(wal, 1, 1, bytes);

done:
    if (fixture.receiver != NULL) {
        tr_raft_snapshot_receiver_destroy(fixture.receiver);
        fixture.receiver = NULL;
    }
    if (wal != NULL)
        failed |= ensure(tr_raft_wal_storage_close(wal) == SALTS_OK,
                         "release final WAL owner lock");
    cleanup_prefix(prefix);
    if (!failed)
        printf("PASS: real SIGKILL partial %s Snapshot; orphan rejected, offset0 retransmitted, WAL suffix durable\n",
               mode);
    return failed ? 1 : 0;
}

int main(int argc, char **argv)
{
    int streaming;
    if (argc == 4 && strcmp(argv[1], "--child") == 0) {
        if (strcmp(argv[3], "buffered") != 0 &&
            strcmp(argv[3], "streamed") != 0) return 2;
        streaming = strcmp(argv[3], "streamed") == 0;
        return child_partial(argv[2], streaming);
    }
    if (argc != 2 ||
        (strcmp(argv[1], "buffered") != 0 &&
         strcmp(argv[1], "streamed") != 0)) {
        fprintf(stderr, "usage: %s buffered|streamed\n", argv[0]);
        return 2;
    }
    return run_case(argv[0], argv[1]);
}
