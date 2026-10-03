#include <turboraft/raft_wal_storage.h>
#include <turboraft/raft_wire_codec.h>

#include <salts_error.h>

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int fail(const char *stage, int code)
{
    fprintf(stderr, "producer failure at %s: %d\n", stage, code);
    return 1;
}

static int write_frame(const char *path, const uint8_t *data, size_t size)
{
    FILE *output = fopen(path, "wb");
    if (output == NULL) {
        return SALTS_EIO;
    }
    if (fwrite(data, 1U, size, output) != size) {
        (void)fclose(output);
        return SALTS_EIO;
    }
    return fclose(output) == 0 ? SALTS_OK : SALTS_EIO;
}

int main(int argc, char **argv)
{
    static const uint8_t snapshot_bytes[] = "v0.2.0-snapshot-fixture";
    tr_raft_wal_storage_config_t config;
    tr_raft_wal_storage_t *storage = NULL;
    tr_raft_storage_t adapter;
    tr_raft_entry_t entries[2];
    tr_raft_conf_t configuration;
    tr_raft_wire_codec_t *codec = NULL;
    tr_raft_wire_metadata_t metadata;
    tr_raft_message_t message;
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
    config.max_segments = 8U;
    config.max_log_entries = 16U;
    config.max_snapshot_bytes = 1024U;
    config.create_if_missing = true;

    result = tr_raft_wal_storage_open(&config, &storage);
    if (result != SALTS_OK) return fail("wal-open", result);

    memset(&adapter, 0, sizeof(adapter));
    result = tr_raft_wal_storage_bind(storage, &adapter);
    if (result != SALTS_OK) return fail("wal-bind", result);

    memset(entries, 0, sizeof(entries));
    entries[0].index = 1U;
    entries[0].term = 7U;
    entries[0].command_id = 1001U;
    entries[0].data_length = 4U;
    memcpy(entries[0].data, "one!", 4U);
    entries[1].index = 2U;
    entries[1].term = 7U;
    entries[1].command_id = 1002U;
    entries[1].data_length = 4U;
    memcpy(entries[1].data, "two!", 4U);

    result = adapter.begin(adapter.context);
    if (result == SALTS_OK) {
        result = adapter.write_hard_state(adapter.context, 7U, 1U);
    }
    if (result == SALTS_OK) {
        result = adapter.append_log(adapter.context, entries, 2U);
    }
    if (result == SALTS_OK) {
        result = adapter.write_commit_index(adapter.context, 2U);
    }
    if (result == SALTS_OK) {
        result = adapter.commit(adapter.context);
    }
    if (result != SALTS_OK) return fail("wal-commit", result);

    memset(&configuration, 0, sizeof(configuration));
    configuration.phase = TR_RAFT_CONF_FINAL;
    configuration.transition_id = 41U;
    configuration.member_count = 1U;
    configuration.members[0].node_id = 1U;
    configuration.members[0].roles =
        TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER;

    result = tr_raft_wal_storage_store_snapshot(
        storage, 1U, 7U, &configuration,
        snapshot_bytes, sizeof(snapshot_bytes) - 1U);
    if (result != SALTS_OK) return fail("snapshot-store", result);

    result = tr_raft_wal_storage_close(storage);
    storage = NULL;
    if (result != SALTS_OK) return fail("wal-close", result);

    result = tr_raft_wire_codec_create(&codec);
    if (result != SALTS_OK) return fail("wire-create", result);

    memset(&metadata, 0, sizeof(metadata));
    for (index = 0U; index < sizeof(metadata.cluster_id.bytes); ++index) {
        metadata.cluster_id.bytes[index] = (uint8_t)(0xA0U + index);
    }
    metadata.group_id = 55U;
    metadata.message_id = 77U;

    memset(&message, 0, sizeof(message));
    message.type = TR_RAFT_MSG_HEARTBEAT_REQUEST;
    message.from = 1U;
    message.to = 2U;
    message.term = 7U;
    message.last_log_index = 2U;
    message.last_log_term = 7U;
    message.leader_commit = 2U;

    result = tr_raft_wire_encode(
        codec, &metadata, &message,
        frame, sizeof(frame), &frame_size);
    if (result == SALTS_OK) {
        result = write_frame(argv[2], frame, frame_size);
    }
    tr_raft_wire_codec_destroy(codec);
    if (result != SALTS_OK) return fail("wire-write", result);

    printf("producer_ok prefix=%s frame=%s bytes=%zu\n",
           argv[1], argv[2], frame_size);
    return 0;
}
