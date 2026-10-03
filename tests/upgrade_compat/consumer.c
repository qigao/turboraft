#include <turboraft/raft_wal_storage.h>
#include <turboraft/raft_wire_codec.h>

#include <salts_error.h>

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int fail(const char *stage, int code)
{
    fprintf(stderr, "consumer failure at %s: %d\n", stage, code);
    return 1;
}

static int read_frame(const char *path, uint8_t *data,
                      size_t capacity, size_t *out_size)
{
    FILE *input;
    long end;

    if (out_size == NULL) return SALTS_EINVAL;
    *out_size = 0U;
    input = fopen(path, "rb");
    if (input == NULL) return SALTS_EIO;
    if (fseek(input, 0L, SEEK_END) != 0) {
        (void)fclose(input);
        return SALTS_EIO;
    }
    end = ftell(input);
    if (end < 0 || (size_t)end > capacity ||
        fseek(input, 0L, SEEK_SET) != 0) {
        (void)fclose(input);
        return SALTS_EIO;
    }
    if (fread(data, 1U, (size_t)end, input) != (size_t)end) {
        (void)fclose(input);
        return SALTS_EIO;
    }
    if (fclose(input) != 0) return SALTS_EIO;
    *out_size = (size_t)end;
    return SALTS_OK;
}

int main(int argc, char **argv)
{
    static const uint8_t expected_snapshot[] = "v0.2.0-snapshot-fixture";
    tr_raft_wal_storage_config_t config;
    tr_raft_wal_storage_t *storage = NULL;
    tr_raft_wal_recovery_t recovery;
    tr_raft_wire_codec_t *codec = NULL;
    tr_raft_wire_metadata_t metadata;
    tr_raft_message_t message;
    uint8_t snapshot[64];
    size_t snapshot_size = 0U;
    uint8_t frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];
    size_t frame_size = 0U;
    size_t index;
    int result;

    if (argc != 3) {
        fprintf(stderr, "usage: %s <wal-prefix> <wire-frame>\n", argv[0]);
        return 2;
    }

    memset(&config, 0, sizeof(config));
    config.path_prefix = argv[1];
    config.segment_bytes = TR_RAFT_WAL_MIN_SEGMENT_BYTES;
    config.max_transaction_bytes = 16U * 1024U;
    config.max_live_segments = 8U;
    config.max_log_entries = 16U;
    config.max_snapshot_bytes = 1024U;
    config.create_if_missing = false;

    result = tr_raft_wal_storage_open(&config, &storage);
    if (result != SALTS_OK) return fail("wal-open", result);

    memset(&recovery, 0, sizeof(recovery));
    result = tr_raft_wal_storage_load(storage, &recovery);
    if (result != SALTS_OK) return fail("wal-load", result);

    if (recovery.term != 7U || recovery.voted_for != 1U ||
        recovery.commit_index != 2U ||
        recovery.snapshot_index != 1U ||
        recovery.snapshot_term != 7U ||
        !recovery.has_snapshot_configuration ||
        recovery.snapshot_configuration.phase != TR_RAFT_CONF_FINAL ||
        recovery.snapshot_configuration.transition_id != 41U ||
        recovery.snapshot_configuration.member_count != 1U ||
        recovery.snapshot_configuration.members[0].node_id != 1U ||
        recovery.entry_count != 1U ||
        recovery.entries[0].index != 2U ||
        recovery.entries[0].term != 7U ||
        recovery.entries[0].command_id != 1002U ||
        recovery.entries[0].data_length != 4U ||
        memcmp(recovery.entries[0].data, "two!", 4U) != 0) {
        tr_raft_wal_recovery_destroy(&recovery);
        return fail("wal-content", SALTS_EPROTO);
    }

    if (recovery.snapshot_source.read_at == NULL ||
        recovery.snapshot_source.size != sizeof(expected_snapshot) - 1U) {
        tr_raft_wal_recovery_destroy(&recovery);
        return fail("snapshot-source", SALTS_EPROTO);
    }
    if (recovery.snapshot_source.size > sizeof(snapshot)) {
        tr_raft_wal_recovery_destroy(&recovery);
        return fail("snapshot-capacity", SALTS_ERANGE);
    }
    result = recovery.snapshot_source.read_at(
        recovery.snapshot_source.context, 0U,
        snapshot, (size_t)recovery.snapshot_source.size, &snapshot_size);
    if (result != SALTS_OK ||
        snapshot_size != sizeof(expected_snapshot) - 1U ||
        memcmp(snapshot, expected_snapshot, snapshot_size) != 0) {
        tr_raft_wal_recovery_destroy(&recovery);
        return fail("snapshot-bytes", result == SALTS_OK ? SALTS_EPROTO : result);
    }
    tr_raft_wal_recovery_destroy(&recovery);

    result = tr_raft_wal_storage_close(storage);
    storage = NULL;
    if (result != SALTS_OK) return fail("wal-close", result);

    result = read_frame(argv[2], frame, sizeof(frame), &frame_size);
    if (result != SALTS_OK) return fail("wire-read", result);
    result = tr_raft_wire_codec_create(&codec);
    if (result != SALTS_OK) return fail("wire-create", result);
    memset(&metadata, 0, sizeof(metadata));
    memset(&message, 0, sizeof(message));
    result = tr_raft_wire_decode(
        codec, frame, frame_size, &metadata, &message);
    tr_raft_wire_codec_destroy(codec);
    if (result != SALTS_OK) return fail("wire-decode", result);

    for (index = 0U; index < sizeof(metadata.cluster_id.bytes); ++index) {
        if (metadata.cluster_id.bytes[index] != (uint8_t)(0xA0U + index)) {
            return fail("wire-cluster", SALTS_EPROTO);
        }
    }
    if (metadata.group_id != 55U || metadata.message_id != 77U ||
        message.type != TR_RAFT_MSG_HEARTBEAT_REQUEST ||
        message.from != 1U || message.to != 2U ||
        message.term != 7U ||
        message.last_log_index != 2U ||
        message.last_log_term != 7U ||
        message.leader_commit != 2U) {
        return fail("wire-content", SALTS_EPROTO);
    }

    puts("upgrade_compat_ok prior=v0.2.0 current=current");
    return 0;
}
