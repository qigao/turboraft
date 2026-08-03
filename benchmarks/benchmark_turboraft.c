#include <turboraft/raft_wire_codec.h>

#ifdef TURBORAFT_BENCHMARK_SQLITE
#include <turboraft/raft_sqlite_storage.h>
#endif

#include <tinytest.h>
#include <turbo_error.h>

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

enum {
    WIRE_BENCHMARK_SAMPLE_COUNT = 10000,
    WIRE_BENCHMARK_ENTRY_BYTES = 256,
    SQLITE_BENCHMARK_SAMPLE_COUNT = 64
};

static volatile int benchmark_result_sink;

static tr_raft_message_t benchmark_wire_message(void)
{
    tr_raft_message_t message;
    size_t index;

    memset(&message, 0, sizeof(message));
    message.type = TR_RAFT_MSG_APPEND_REQUEST;
    message.from = 1U;
    message.to = 2U;
    message.term = 7U;
    message.previous_log_index = 100U;
    message.previous_log_term = 6U;
    message.leader_commit = 100U;
    message.entry_count = TR_RAFT_MAX_APPEND_ENTRIES;
    for (index = 0U; index < message.entry_count; ++index) {
        tr_raft_entry_t *entry = &message.entries[index];

        entry->index = message.previous_log_index + index + 1U;
        entry->term = message.term;
        entry->command_id = index + 1U;
        entry->data_length = WIRE_BENCHMARK_ENTRY_BYTES;
        memset(entry->data, (int) index, entry->data_length);
    }
    return message;
}

#ifdef TURBORAFT_BENCHMARK_SQLITE
static tr_raft_entry_t benchmark_storage_entry(tr_raft_index_t index)
{
    static const char payload[] = "durable-benchmark";
    tr_raft_entry_t entry;

    memset(&entry, 0, sizeof(entry));
    entry.index = index;
    entry.term = index;
    entry.command_id = index;
    entry.data_length = sizeof(payload) - 1U;
    memcpy(entry.data, payload, entry.data_length);
    return entry;
}
#endif

spec("TurboRaft performance baselines")
{
    bench("wire codec")
    {
        tr_raft_wire_codec_t *codec = NULL;
        tr_raft_wire_metadata_t metadata;
        tr_raft_wire_metadata_t decoded_metadata;
        tr_raft_message_t message = benchmark_wire_message();
        tr_raft_message_t decoded;
        uint8_t frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];
        size_t frame_size = 0U;
        int first_error = TURBO_OK;

        memset(&metadata, 0, sizeof(metadata));
        metadata.cluster_id.bytes[0] = 1U;
        metadata.message_id = 1U;
        check_int_eq(tr_raft_wire_codec_create(&codec), TURBO_OK);
        check_int_eq(tr_raft_wire_encode_version(
                         codec, TR_RAFT_WIRE_VERSION, &metadata, &message,
                         frame, sizeof(frame), &frame_size),
                     TURBO_OK);
        check_int_eq(tr_raft_wire_decode(codec, frame, frame_size,
                                         &decoded_metadata, &decoded),
                     TURBO_OK);
        check_size_eq(decoded.entry_count, message.entry_count);

        benchmark_io("wire v3 encode 8x256B",
                     WIRE_BENCHMARK_SAMPLE_COUNT, 1U, frame_size)
        {
            int result = tr_raft_wire_encode_version(
                codec, TR_RAFT_WIRE_VERSION, &metadata, &message,
                frame, sizeof(frame), &frame_size);
            if (first_error == TURBO_OK && result != TURBO_OK) {
                first_error = result;
            }
            benchmark_result_sink = result;
        }
        check_int_eq(first_error, TURBO_OK);

        first_error = TURBO_OK;
        benchmark_io("wire v3 decode 8x256B",
                     WIRE_BENCHMARK_SAMPLE_COUNT, 1U, frame_size)
        {
            int result = tr_raft_wire_decode(
                codec, frame, frame_size, &decoded_metadata, &decoded);
            if (first_error == TURBO_OK && result != TURBO_OK) {
                first_error = result;
            }
            benchmark_result_sink = result;
        }
        check_int_eq(first_error, TURBO_OK);
        tr_raft_wire_codec_destroy(codec);
    }

#ifdef TURBORAFT_BENCHMARK_SQLITE
    bench("SQLite durable storage")
    {
        char *path = tt_make_temp_file("turboraft-benchmark", ".db");
        tr_raft_sqlite_storage_config_t config;
        tr_raft_sqlite_storage_t *storage = NULL;
        tr_raft_storage_t adapter;
        tr_raft_index_t index = 0U;
        int first_error = TURBO_OK;

        check_not_null(path);
        memset(&config, 0, sizeof(config));
        config.path = path;
        config.busy_timeout_ms = 5000;
        config.create_if_missing = true;
        config.max_snapshot_bytes = 1024U;
        check_int_eq(tr_raft_sqlite_storage_open(&config, &storage), TURBO_OK);
        check_int_eq(tr_raft_sqlite_storage_bind(storage, &adapter), TURBO_OK);

        benchmark_batch("SQLite WAL FULL single-entry commit",
                        SQLITE_BENCHMARK_SAMPLE_COUNT)
        {
            tr_raft_entry_t entry = benchmark_storage_entry(++index);
            int result = adapter.begin(adapter.context);

            if (result == TURBO_OK) {
                result = adapter.write_hard_state(
                    adapter.context, entry.term, 1U);
            }
            if (result == TURBO_OK) {
                result = adapter.append_log(adapter.context, &entry, 1U);
            }
            if (result == TURBO_OK) {
                result = adapter.write_commit_index(
                    adapter.context, entry.index);
            }
            if (result == TURBO_OK) {
                result = adapter.commit(adapter.context);
            } else {
                (void) adapter.rollback(adapter.context);
            }
            if (first_error == TURBO_OK && result != TURBO_OK) {
                first_error = result;
            }
            benchmark_result_sink = result;
        }
        check_int_eq(first_error, TURBO_OK);
        check_long_eq(index, SQLITE_BENCHMARK_SAMPLE_COUNT);
        check_int_eq(tr_raft_sqlite_storage_close(storage), TURBO_OK);
        check_int_eq(tt_remove_file(path), 0);
        free(path);
    }
#endif
}
