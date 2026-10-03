#include <turboraft/raft_wal_storage.h>
#include <turboraft/raft_wire_codec.h>

#include <salts_error.h>

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const uint8_t k_snapshot[] = "turboraft-v0.2.0-upgrade-snapshot";
static const uint8_t k_entry_three[] = "entry-three-after-snapshot";
static const uint8_t k_wire_entry[] = "wire-v6-entry";

static int fail(const char *what, int code)
{
    fprintf(stderr, "FAIL: %s (%d)\n", what, code);
    return 1;
}

static int expect_true(int condition, const char *what)
{
    if (!condition) {
        fprintf(stderr, "FAIL: %s\n", what);
        return 1;
    }
    return 0;
}

static tr_raft_entry_t fixture_entry(tr_raft_index_t index,
                                     tr_raft_term_t term,
                                     uint64_t command_id,
                                     const char *payload)
{
    tr_raft_entry_t entry;
    size_t length = strlen(payload);

    memset(&entry, 0, sizeof(entry));
    entry.index = index;
    entry.term = term;
    entry.command_id = command_id;
    if (length > sizeof(entry.data)) {
        length = sizeof(entry.data);
    }
    entry.data_length = length;
    memcpy(entry.data, payload, length);
    return entry;
}

static tr_raft_conf_t fixture_configuration(void)
{
    tr_raft_conf_t configuration;

    memset(&configuration, 0, sizeof(configuration));
    configuration.phase = TR_RAFT_CONF_FINAL;
    configuration.transition_id = 77U;
    configuration.member_count = 2U;
    configuration.members[0].node_id = 1U;
    configuration.members[0].roles =
        TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER;
    configuration.members[1].node_id = 2U;
    configuration.members[1].roles =
        TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER;
    return configuration;
}

static tr_raft_wal_storage_config_t fixture_storage_config(
    const char *prefix,
    int create_if_missing)
{
    tr_raft_wal_storage_config_t config;

    memset(&config, 0, sizeof(config));
    config.path_prefix = prefix;
    config.segment_bytes = TR_RAFT_WAL_MIN_SEGMENT_BYTES;
    config.max_transaction_bytes = 8U * 1024U;
    config.max_segments = 4U;
    config.max_log_entries = 16U;
    config.max_snapshot_bytes = 1024U;
    config.create_if_missing = create_if_missing != 0;
    return config;
}

static int wal_produce(const char *prefix)
{
    tr_raft_wal_storage_config_t config =
        fixture_storage_config(prefix, 1);
    tr_raft_wal_storage_t *storage = NULL;
    tr_raft_storage_t adapter;
    tr_raft_entry_t entries[3];
    tr_raft_conf_t configuration = fixture_configuration();
    int result;

    entries[0] = fixture_entry(1U, 1U, 301U, "entry-one");
    entries[1] = fixture_entry(2U, 2U, 302U, "entry-two");
    entries[2] = fixture_entry(3U, 2U, 303U,
                               (const char *)k_entry_three);

    result = tr_raft_wal_storage_open(&config, &storage);
    if (result != SALTS_OK) return fail("open producer storage", result);
    result = tr_raft_wal_storage_bind(storage, &adapter);
    if (result != SALTS_OK) return fail("bind producer storage", result);
    result = adapter.begin(adapter.context);
    if (result != SALTS_OK) return fail("begin producer transaction", result);
    result = adapter.write_hard_state(adapter.context, 2U, 1U);
    if (result != SALTS_OK) return fail("write producer hard state", result);
    result = adapter.append_log(adapter.context, entries, 3U);
    if (result != SALTS_OK) return fail("append producer log", result);
    result = adapter.write_commit_index(adapter.context, 3U);
    if (result != SALTS_OK) return fail("write producer commit", result);
    result = adapter.commit(adapter.context);
    if (result != SALTS_OK) return fail("commit producer transaction", result);

    result = tr_raft_wal_storage_store_snapshot(
        storage, 2U, 2U, &configuration,
        k_snapshot, sizeof(k_snapshot) - 1U);
    if (result != SALTS_OK) return fail("store producer snapshot", result);

    result = tr_raft_wal_storage_close(storage);
    if (result != SALTS_OK) return fail("close producer storage", result);
    puts("PASS: produced durable upgrade fixture");
    return 0;
}

static int wal_verify(const char *prefix)
{
    tr_raft_wal_storage_config_t config =
        fixture_storage_config(prefix, 0);
    tr_raft_wal_storage_t *storage = NULL;
    tr_raft_wal_recovery_t recovery;
    uint8_t snapshot[sizeof(k_snapshot)];
    size_t snapshot_size = 0U;
    int result;
    int failed = 0;

    memset(&recovery, 0, sizeof(recovery));
    memset(snapshot, 0, sizeof(snapshot));

    result = tr_raft_wal_storage_open(&config, &storage);
    if (result != SALTS_OK) return fail("open upgrade fixture", result);
    result = tr_raft_wal_storage_load(storage, &recovery);
    if (result != SALTS_OK) return fail("load upgrade fixture", result);

    failed |= expect_true(recovery.term == 2U, "term == 2");
    failed |= expect_true(recovery.voted_for == 1U, "voted_for == 1");
    failed |= expect_true(recovery.commit_index == 3U, "commit_index == 3");
    failed |= expect_true(recovery.snapshot_index == 2U,
                          "snapshot_index == 2");
    failed |= expect_true(recovery.snapshot_term == 2U,
                          "snapshot_term == 2");
    failed |= expect_true(
        recovery.snapshot_configuration.phase == TR_RAFT_CONF_FINAL,
        "snapshot configuration final");
    failed |= expect_true(
        recovery.snapshot_configuration.transition_id == 77U,
        "snapshot transition_id == 77");
    failed |= expect_true(
        recovery.snapshot_configuration.member_count == 2U,
        "snapshot member_count == 2");
    failed |= expect_true(
        recovery.snapshot_configuration.members[0].node_id == 1U &&
        recovery.snapshot_configuration.members[1].node_id == 2U,
        "snapshot member ids preserved");

    failed |= expect_true(recovery.entry_count == 1U,
                          "one post-snapshot log entry");
    if (recovery.entry_count == 1U) {
        const tr_raft_entry_t *entry = &recovery.entries[0];
        failed |= expect_true(entry->index == 3U, "entry index == 3");
        failed |= expect_true(entry->term == 2U, "entry term == 2");
        failed |= expect_true(entry->command_id == 303U,
                              "entry command_id == 303");
        failed |= expect_true(
            entry->data_length == sizeof(k_entry_three) - 1U &&
            memcmp(entry->data, k_entry_three,
                   sizeof(k_entry_three) - 1U) == 0,
            "post-snapshot payload preserved");
    }

    failed |= expect_true(
        recovery.snapshot_source.size == sizeof(k_snapshot) - 1U,
        "snapshot source size preserved");
    failed |= expect_true(recovery.snapshot_source.read_at != NULL,
                          "snapshot source is readable");
    if (recovery.snapshot_source.read_at != NULL) {
        result = recovery.snapshot_source.read_at(
            recovery.snapshot_source.context, 0U,
            snapshot, sizeof(snapshot), &snapshot_size);
        failed |= expect_true(result == SALTS_OK,
                              "snapshot read_at succeeds");
        failed |= expect_true(
            snapshot_size == sizeof(k_snapshot) - 1U &&
            memcmp(snapshot, k_snapshot, sizeof(k_snapshot) - 1U) == 0,
            "snapshot bytes preserved");
    }

    tr_raft_wal_recovery_destroy(&recovery);
    result = tr_raft_wal_storage_close(storage);
    if (result != SALTS_OK) return fail("close upgrade fixture", result);
    if (failed != 0) return 1;
    puts("PASS: verified durable upgrade fixture");
    return 0;
}

static void wire_metadata(tr_raft_wire_metadata_t *metadata)
{
    size_t index;

    memset(metadata, 0, sizeof(*metadata));
    for (index = 0U; index < sizeof(metadata->cluster_id.bytes); ++index) {
        metadata->cluster_id.bytes[index] = (uint8_t)(0xa0U + index);
    }
    metadata->group_id = 17U;
    metadata->message_id = 1234U;
}

static tr_raft_message_t wire_message(void)
{
    tr_raft_message_t message;

    memset(&message, 0, sizeof(message));
    message.type = TR_RAFT_MSG_APPEND_REQUEST;
    message.from = 1U;
    message.to = 2U;
    message.term = 2U;
    message.leader_commit = 2U;
    message.previous_log_index = 2U;
    message.previous_log_term = 2U;
    message.entry_count = 1U;
    message.entries[0] =
        fixture_entry(3U, 2U, 303U, (const char *)k_wire_entry);
    return message;
}

static int wire_produce(const char *path)
{
    tr_raft_wire_codec_t *codec = NULL;
    tr_raft_wire_metadata_t metadata;
    tr_raft_message_t message = wire_message();
    uint8_t frame[TR_RAFT_WIRE_HEADER_SIZE +
                  TR_RAFT_WIRE_MAX_RAFT_PAYLOAD_SIZE];
    size_t frame_size = 0U;
    FILE *file;
    int result;

    wire_metadata(&metadata);
    result = tr_raft_wire_codec_create(&codec);
    if (result != SALTS_OK) return fail("create wire codec", result);
    result = tr_raft_wire_encode(codec, &metadata, &message,
                                 frame, sizeof(frame), &frame_size);
    tr_raft_wire_codec_destroy(codec);
    if (result != SALTS_OK) return fail("encode wire fixture", result);

    file = fopen(path, "wb");
    if (file == NULL) return fail("open wire fixture output", SALTS_EIO);
    if (fwrite(frame, 1U, frame_size, file) != frame_size) {
        fclose(file);
        return fail("write wire fixture", SALTS_EIO);
    }
    if (fclose(file) != 0) return fail("close wire fixture", SALTS_EIO);
    puts("PASS: produced wire-v6 fixture");
    return 0;
}

static int wire_verify(const char *path)
{
    tr_raft_wire_codec_t *codec = NULL;
    tr_raft_wire_metadata_t metadata;
    tr_raft_wire_metadata_t expected_metadata;
    tr_raft_message_t message;
    uint8_t *frame = NULL;
    long length;
    size_t frame_size;
    uint16_t version = 0U;
    FILE *file;
    int result;
    int failed = 0;

    file = fopen(path, "rb");
    if (file == NULL) return fail("open wire fixture", SALTS_EIO);
    if (fseek(file, 0L, SEEK_END) != 0) {
        fclose(file);
        return fail("seek wire fixture", SALTS_EIO);
    }
    length = ftell(file);
    if (length <= 0 || (size_t)length > TR_RAFT_WIRE_MAX_FRAME_SIZE) {
        fclose(file);
        return fail("wire fixture length", SALTS_EPROTO);
    }
    if (fseek(file, 0L, SEEK_SET) != 0) {
        fclose(file);
        return fail("rewind wire fixture", SALTS_EIO);
    }
    frame_size = (size_t)length;
    frame = (uint8_t *)malloc(frame_size);
    if (frame == NULL) {
        fclose(file);
        return fail("allocate wire fixture", SALTS_ENOMEM);
    }
    if (fread(frame, 1U, frame_size, file) != frame_size) {
        free(frame);
        fclose(file);
        return fail("read wire fixture", SALTS_EIO);
    }
    fclose(file);

    result = tr_raft_wire_peek_version(frame, frame_size, &version);
    failed |= expect_true(result == SALTS_OK, "peek wire version succeeds");
    failed |= expect_true(version == TR_RAFT_WIRE_VERSION,
                          "wire version is current v6");

    memset(&metadata, 0, sizeof(metadata));
    memset(&message, 0, sizeof(message));
    wire_metadata(&expected_metadata);
    result = tr_raft_wire_codec_create(&codec);
    if (result != SALTS_OK) {
        free(frame);
        return fail("create decode codec", result);
    }
    result = tr_raft_wire_decode(codec, frame, frame_size,
                                 &metadata, &message);
    tr_raft_wire_codec_destroy(codec);
    free(frame);
    if (result != SALTS_OK) return fail("decode wire fixture", result);

    failed |= expect_true(
        memcmp(metadata.cluster_id.bytes,
               expected_metadata.cluster_id.bytes,
               sizeof(metadata.cluster_id.bytes)) == 0,
        "cluster id preserved");
    failed |= expect_true(metadata.group_id == 17U, "group id preserved");
    failed |= expect_true(metadata.message_id == 1234U,
                          "message id preserved");
    failed |= expect_true(message.type == TR_RAFT_MSG_APPEND_REQUEST,
                          "message kind preserved");
    failed |= expect_true(message.from == 1U && message.to == 2U,
                          "message endpoints preserved");
    failed |= expect_true(message.term == 2U &&
                          message.previous_log_index == 2U &&
                          message.previous_log_term == 2U &&
                          message.leader_commit == 2U,
                          "message Raft indices preserved");
    failed |= expect_true(message.entry_count == 1U,
                          "wire entry count preserved");
    if (message.entry_count == 1U) {
        const tr_raft_entry_t *entry = &message.entries[0];
        failed |= expect_true(
            entry->index == 3U && entry->term == 2U &&
            entry->command_id == 303U,
            "wire entry identity preserved");
        failed |= expect_true(
            entry->data_length == sizeof(k_wire_entry) - 1U &&
            memcmp(entry->data, k_wire_entry,
                   sizeof(k_wire_entry) - 1U) == 0,
            "wire entry payload preserved");
    }

    if (failed != 0) return 1;
    puts("PASS: verified wire-v6 fixture");
    return 0;
}

int main(int argc, char **argv)
{
    if (argc != 3) {
        fprintf(stderr,
                "usage: %s wal-produce|wal-verify|wire-produce|wire-verify <path>\n",
                argv[0]);
        return 2;
    }
    if (strcmp(argv[1], "wal-produce") == 0) return wal_produce(argv[2]);
    if (strcmp(argv[1], "wal-verify") == 0) return wal_verify(argv[2]);
    if (strcmp(argv[1], "wire-produce") == 0) return wire_produce(argv[2]);
    if (strcmp(argv[1], "wire-verify") == 0) return wire_verify(argv[2]);
    fprintf(stderr, "unknown mode: %s\n", argv[1]);
    return 2;
}
