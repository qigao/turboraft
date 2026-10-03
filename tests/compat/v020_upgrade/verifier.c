#include <turboraft/raft_wal_storage.h>
#include <turboraft/raft_wire_codec.h>

#include <salts_error.h>

#include <stdint.h>
#include <stdio.h>
#include <string.h>

static int check(int condition, const char *message)
{
    if (condition) return 0;
    fprintf(stderr, "upgrade verification failed: %s\n", message);
    return 1;
}

static int verify_wire_fixture(const char *prefix)
{
    tr_raft_wire_codec_t *codec = NULL;
    tr_raft_wire_metadata_t metadata;
    tr_raft_message_t message;
    uint8_t frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];
    char path[1024];
    FILE *input;
    long length;
    uint16_t version = 0U;
    int result;
    int failed = 0;

    if (snprintf(path, sizeof(path), "%s.wire.bin", prefix) <= 0) {
        return 1;
    }
    input = fopen(path, "rb");
    if (input == NULL) return 1;
    if (fseek(input, 0L, SEEK_END) != 0) {
        (void)fclose(input);
        return 1;
    }
    length = ftell(input);
    if (length <= 0L || (size_t)length > sizeof(frame) ||
        fseek(input, 0L, SEEK_SET) != 0) {
        (void)fclose(input);
        return 1;
    }
    if (fread(frame, 1U, (size_t)length, input) != (size_t)length) {
        (void)fclose(input);
        return 1;
    }
    if (fclose(input) != 0) return 1;

    result = tr_raft_wire_peek_version(frame, (size_t)length, &version);
    failed |= check(result == SALTS_OK, "wire version parse");
    failed |= check(version == TR_RAFT_WIRE_VERSION, "wire version");

    memset(&metadata, 0, sizeof(metadata));
    memset(&message, 0, sizeof(message));
    result = tr_raft_wire_codec_create(&codec);
    failed |= check(result == SALTS_OK, "wire codec create");
    if (result != SALTS_OK) return 1;
    result = tr_raft_wire_decode(codec, frame, (size_t)length,
                                 &metadata, &message);
    tr_raft_wire_codec_destroy(codec);
    failed |= check(result == SALTS_OK, "wire decode");
    if (result != SALTS_OK) return 1;

    failed |= check(metadata.cluster_id.bytes[0] == 0x42U &&
                        metadata.cluster_id.bytes[15] == 0xa5U,
                    "wire cluster id");
    failed |= check(metadata.group_id == 100U, "wire group id");
    failed |= check(metadata.message_id == 55U, "wire message id");
    failed |= check(message.type == TR_RAFT_MSG_APPEND_REQUEST,
                    "wire message type");
    failed |= check(message.from == 1U && message.to == 2U,
                    "wire endpoints");
    failed |= check(message.term == 3U && message.leader_commit == 2U,
                    "wire term/commit");
    failed |= check(message.previous_log_index == 1U &&
                        message.previous_log_term == 1U,
                    "wire previous log");
    failed |= check(message.entry_count == 1U, "wire entry count");
    if (message.entry_count == 1U) {
        failed |= check(message.entries[0].index == 2U &&
                            message.entries[0].term == 2U &&
                            message.entries[0].command_id == 42U,
                        "wire entry facts");
        failed |= check(message.entries[0].data_length == 4U &&
                            memcmp(message.entries[0].data, "wire", 4U) == 0,
                        "wire entry payload");
    }
    return failed != 0;
}

int main(int argc, char **argv)
{
    static const uint8_t expected_snapshot[] = "v0.2.0-snapshot";
    static const uint8_t expected_suffix[] = "three";
    tr_raft_wal_storage_config_t config;
    tr_raft_wal_storage_t *storage = NULL;
    tr_raft_wal_recovery_t recovery;
    uint8_t snapshot[sizeof(expected_snapshot) - 1U];
    size_t snapshot_read = 0U;
    int result;
    int failed = 0;

    const char *verifier_label;

    if ((argc != 2 && argc != 3) || argv[1][0] == '\0') {
        fprintf(stderr, "usage: %s <wal-prefix> [verifier-label]\n", argv[0]);
        return 2;
    }
    verifier_label = argc == 3 ? argv[2] : "fixture-verifier";

    memset(&config, 0, sizeof(config));
    config.path_prefix = argv[1];
    config.segment_bytes = TR_RAFT_WAL_MIN_SEGMENT_BYTES;
    config.max_transaction_bytes = 16U * 1024U;
    config.max_segments = 4U;
    config.max_log_entries = 16U;
    config.max_snapshot_bytes = 1024U;
    config.create_if_missing = false;

    result = tr_raft_wal_storage_open(&config, &storage);
    if (result != SALTS_OK) {
        fprintf(stderr, "current open of v0.2.0 fixture failed: %d\n", result);
        return 1;
    }
    memset(&recovery, 0, sizeof(recovery));
    result = tr_raft_wal_storage_load(storage, &recovery);
    if (result != SALTS_OK) {
        fprintf(stderr, "current load of v0.2.0 fixture failed: %d\n", result);
        (void)tr_raft_wal_storage_close(storage);
        return 1;
    }

    failed |= check(recovery.term == 2U, "term");
    failed |= check(recovery.voted_for == 1U, "voted_for");
    failed |= check(recovery.commit_index == 3U, "commit_index");
    failed |= check(recovery.snapshot_index == 2U, "snapshot_index");
    failed |= check(recovery.snapshot_term == 2U, "snapshot_term");
    failed |= check(recovery.has_snapshot_configuration,
                    "snapshot configuration presence");
    failed |= check(recovery.snapshot_configuration.phase == TR_RAFT_CONF_FINAL,
                    "snapshot configuration phase");
    failed |= check(recovery.snapshot_configuration.transition_id == 7U,
                    "snapshot transition_id");
    failed |= check(recovery.snapshot_configuration.member_count == 1U,
                    "snapshot member_count");
    failed |= check(recovery.snapshot_configuration.members[0].node_id == 1U,
                    "snapshot voter node_id");
    failed |= check(
        recovery.snapshot_configuration.members[0].roles ==
            (TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER),
        "snapshot voter roles");

    failed |= check(recovery.entry_count == 1U, "retained suffix count");
    if (recovery.entry_count == 1U) {
        failed |= check(recovery.entries[0].index == 3U, "suffix index");
        failed |= check(recovery.entries[0].term == 2U, "suffix term");
        failed |= check(recovery.entries[0].command_id == 3U,
                        "suffix command_id");
        failed |= check(recovery.entries[0].data_length ==
                            sizeof(expected_suffix) - 1U,
                        "suffix payload size");
        if (recovery.entries[0].data_length ==
            sizeof(expected_suffix) - 1U) {
            failed |= check(memcmp(recovery.entries[0].data, expected_suffix,
                                   sizeof(expected_suffix) - 1U) == 0,
                            "suffix payload");
        }
    }

    failed |= check(recovery.snapshot_source.read_at != NULL,
                    "snapshot source available");
    failed |= check(recovery.snapshot_source.size ==
                        sizeof(expected_snapshot) - 1U,
                    "snapshot source size");
    if (recovery.snapshot_source.read_at != NULL) {
        result = recovery.snapshot_source.read_at(
            recovery.snapshot_source.context, 0U, snapshot,
            sizeof(snapshot), &snapshot_read);
        failed |= check(result == SALTS_OK, "snapshot read");
        failed |= check(snapshot_read == sizeof(snapshot),
                        "snapshot read size");
        if (result == SALTS_OK && snapshot_read == sizeof(snapshot)) {
            failed |= check(memcmp(snapshot, expected_snapshot,
                                   sizeof(snapshot)) == 0,
                            "snapshot bytes");
        }
    }

    tr_raft_wal_recovery_destroy(&recovery);
    result = tr_raft_wal_storage_close(storage);
    failed |= check(result == SALTS_OK, "close");
    failed |= verify_wire_fixture(argv[1]);

    if (failed != 0) return 1;
    printf("PASS: %s recovered compatible durable + wire fixtures\n",
           verifier_label);
    return 0;
}
