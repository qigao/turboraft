#include <turboraft/raft_wal_storage.h>
#include <turboraft/raft_wire_codec.h>

#include <salts_error.h>

#include <stdint.h>
#include <stdio.h>
#include <string.h>

static tr_raft_entry_t make_entry(tr_raft_index_t index,
                                  tr_raft_term_t term,
                                  uint64_t command_id,
                                  const char *text)
{
    tr_raft_entry_t entry;
    size_t size = strlen(text);

    memset(&entry, 0, sizeof(entry));
    entry.index = index;
    entry.term = term;
    entry.command_id = command_id;
    entry.data_length = size;
    memcpy(entry.data, text, size);
    return entry;
}

static int write_wire_fixture(const char *prefix)
{
    tr_raft_wire_codec_t *codec = NULL;
    tr_raft_wire_metadata_t metadata;
    tr_raft_message_t message;
    uint8_t frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];
    char path[1024];
    size_t frame_size = 0U;
    FILE *output;
    int result;

    memset(&metadata, 0, sizeof(metadata));
    memset(&message, 0, sizeof(message));
    metadata.cluster_id.bytes[0] = 0x42U;
    metadata.cluster_id.bytes[15] = 0xa5U;
    metadata.group_id = 100U;
    metadata.message_id = 55U;

    message.type = TR_RAFT_MSG_APPEND_REQUEST;
    message.from = 1U;
    message.to = 2U;
    message.term = 3U;
    message.leader_commit = 2U;
    message.previous_log_index = 1U;
    message.previous_log_term = 1U;
    message.entry_count = 1U;
    message.entries[0] = make_entry(2U, 2U, 42U, "wire");

    result = tr_raft_wire_codec_create(&codec);
    if (result != SALTS_OK) return result;
    result = tr_raft_wire_encode(codec, &metadata, &message, frame,
                                 sizeof(frame), &frame_size);
    tr_raft_wire_codec_destroy(codec);
    if (result != SALTS_OK) return result;

    if (snprintf(path, sizeof(path), "%s.wire.bin", prefix) <= 0) {
        return SALTS_EINVAL;
    }
    output = fopen(path, "wb");
    if (output == NULL) return SALTS_EIO;
    if (fwrite(frame, 1U, frame_size, output) != frame_size) {
        (void)fclose(output);
        return SALTS_EIO;
    }
    if (fclose(output) != 0) return SALTS_EIO;
    return SALTS_OK;
}

static int fail(const char *stage, int result)
{
    fprintf(stderr, "v0.2.0 fixture producer failed at %s: %d\n",
            stage, result);
    return 1;
}

int main(int argc, char **argv)
{
    static const uint8_t snapshot[] = "v0.2.0-snapshot";
    const tr_raft_conf_t configuration = {
        TR_RAFT_CONF_FINAL, 7U, 1U,
        {{1U, TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER}}
    };
    tr_raft_wal_storage_config_t config;
    tr_raft_wal_storage_t *storage = NULL;
    tr_raft_storage_t adapter;
    tr_raft_entry_t entries[3];
    int result;

    if (argc != 2 || argv[1][0] == '\0') {
        fprintf(stderr, "usage: %s <wal-prefix>\n", argv[0]);
        return 2;
    }

    memset(&config, 0, sizeof(config));
    config.path_prefix = argv[1];
    config.segment_bytes = TR_RAFT_WAL_MIN_SEGMENT_BYTES;
    config.max_transaction_bytes = 16U * 1024U;
    config.max_segments = 4U;
    config.max_log_entries = 16U;
    config.max_snapshot_bytes = 1024U;
    config.create_if_missing = true;

    entries[0] = make_entry(1U, 1U, 1U, "one");
    entries[1] = make_entry(2U, 2U, 2U, "two");
    entries[2] = make_entry(3U, 2U, 3U, "three");

    result = tr_raft_wal_storage_open(&config, &storage);
    if (result != SALTS_OK) return fail("open", result);
    result = tr_raft_wal_storage_bind(storage, &adapter);
    if (result != SALTS_OK) return fail("bind", result);
    result = adapter.begin(adapter.context);
    if (result != SALTS_OK) return fail("begin", result);
    result = adapter.write_hard_state(adapter.context, 2U, 1U);
    if (result == SALTS_OK) {
        result = adapter.append_log(adapter.context, entries, 3U);
    }
    if (result == SALTS_OK) {
        result = adapter.write_commit_index(adapter.context, 3U);
    }
    if (result == SALTS_OK) {
        result = adapter.commit(adapter.context);
    }
    if (result != SALTS_OK) return fail("commit", result);

    result = tr_raft_wal_storage_store_snapshot(
        storage, 2U, 2U, &configuration, snapshot, sizeof(snapshot) - 1U);
    if (result != SALTS_OK) return fail("snapshot", result);

    result = tr_raft_wal_storage_close(storage);
    if (result != SALTS_OK) return fail("close", result);

    result = write_wire_fixture(argv[1]);
    if (result != SALTS_OK) return fail("wire", result);

    printf("producer=v0.2.0 term=2 voted_for=1 commit=3 snapshot=2/2 "
           "transition=7 suffix_index=3\n");
    return 0;
}
