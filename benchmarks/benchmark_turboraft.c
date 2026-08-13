#include <turboraft/raft_core.h>
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
    WIRE_BENCHMARK_BURST_ITEMS = 16,
    REPLICATION_MODEL_ENTRY_COUNT = 257,
    REPLICATION_MODEL_MAX_WINDOW = 8,
    SQLITE_BENCHMARK_SAMPLE_COUNT = 64
};

static volatile int benchmark_result_sink;

typedef struct replication_model_result {
    size_t round_trips;
    size_t append_requests;
    tr_raft_index_t match_index;
    tr_raft_index_t commit_index;
} replication_model_result_t;

static tr_raft_ready_t benchmark_ready(tr_raft_message_t *messages,
                                       size_t capacity)
{
    tr_raft_ready_t ready;

    memset(&ready, 0, sizeof(ready));
    ready.messages = messages;
    ready.message_capacity = capacity;
    return ready;
}

static int benchmark_elect_leader(tr_raft_core_t *core,
                                  tr_raft_message_t *messages,
                                  size_t capacity)
{
    tr_raft_tick_t tick = {5U, 7U};
    tr_raft_message_t response;
    tr_raft_ready_t ready = benchmark_ready(messages, capacity);
    int result = tr_raft_core_tick(core, &tick, &ready);

    if (result != TURBO_OK) {
        return result;
    }
    result = tr_raft_core_advance(core);
    if (result != TURBO_OK) {
        return result;
    }

    memset(&response, 0, sizeof(response));
    response.type = TR_RAFT_MSG_PRE_VOTE_RESPONSE;
    response.from = 2U;
    response.to = 1U;
    response.campaign_term = 1U;
    response.granted = true;
    ready = benchmark_ready(messages, capacity);
    result = tr_raft_core_step(core, &response, &ready);
    if (result != TURBO_OK) {
        return result;
    }
    result = tr_raft_core_advance(core);
    if (result != TURBO_OK) {
        return result;
    }

    response.type = TR_RAFT_MSG_VOTE_RESPONSE;
    response.term = 1U;
    ready = benchmark_ready(messages, capacity);
    result = tr_raft_core_step(core, &response, &ready);
    if (result != TURBO_OK || ready.role != TR_RAFT_LEADER) {
        return result == TURBO_OK ? TURBO_EPROTO : result;
    }
    return tr_raft_core_advance(core);
}

/**
 * Drives the real core with a virtual network: responses to one emitted burst
 * become visible in the next round. Time O(entries), scratch O(window).
 */
static int benchmark_replication_round_trips(
    size_t window,
    size_t entry_count,
    replication_model_result_t *out_result)
{
    static const tr_raft_node_id_t voters[] = {1U, 2U};
    tr_raft_core_config_t config;
    tr_raft_core_t *core = NULL;
    tr_raft_message_t *round_messages[2] = {NULL, NULL};
    tr_raft_status_t status;
    tr_raft_progress_view_t progress;
    size_t current_round = 0U;
    size_t current_count = 0U;
    size_t index;
    int result = TURBO_OK;

    if (window == 0U || window > REPLICATION_MODEL_MAX_WINDOW ||
        entry_count == 0U ||
        entry_count > SIZE_MAX - TR_RAFT_MAX_APPEND_ENTRIES ||
        out_result == NULL) {
        return TURBO_EINVAL;
    }
    memset(out_result, 0, sizeof(*out_result));
    memset(&status, 0, sizeof(status));
    memset(&progress, 0, sizeof(progress));
    round_messages[0] = (tr_raft_message_t *) calloc(
        window, sizeof(*round_messages[0]));
    round_messages[1] = (tr_raft_message_t *) calloc(
        window, sizeof(*round_messages[1]));
    if (round_messages[0] == NULL || round_messages[1] == NULL) {
        result = TURBO_ENOMEM;
        goto cleanup;
    }

    memset(&config, 0, sizeof(config));
    config.self_id = 1U;
    config.voters = voters;
    config.voter_count = 2U;
    config.heartbeat_ticks = 2U;
    config.election_min_ticks = 5U;
    config.election_max_ticks = 10U;
    config.initial_election_timeout_ticks = 5U;
    config.max_log_entries = entry_count + TR_RAFT_MAX_APPEND_ENTRIES;
    config.max_inflight_append_requests = window;
    result = tr_raft_core_create(&config, &core);
    if (result != TURBO_OK) {
        goto cleanup;
    }
    result = benchmark_elect_leader(core, round_messages[0], window);
    if (result != TURBO_OK) {
        goto cleanup;
    }

    for (index = 1U; index <= entry_count; ++index) {
        uint8_t payload = (uint8_t) index;
        tr_raft_proposal_t proposal;
        tr_raft_ready_t ready = benchmark_ready(
            index == 1U ? round_messages[0] : round_messages[1], window);

        memset(&proposal, 0, sizeof(proposal));
        proposal.command_id = index;
        proposal.data = &payload;
        proposal.data_length = sizeof(payload);
        result = tr_raft_core_propose(core, &proposal, &ready);
        if (result != TURBO_OK ||
            ready.message_count != (index == 1U ? 1U : 0U)) {
            result = result == TURBO_OK ? TURBO_EPROTO : result;
            goto cleanup;
        }
        if (index == 1U) {
            current_count = ready.message_count;
            out_result->append_requests += ready.message_count;
        }
        result = tr_raft_core_advance(core);
        if (result != TURBO_OK) {
            goto cleanup;
        }
    }

    while (current_count != 0U) {
        size_t next_round = current_round ^ 1U;
        size_t next_count = 0U;

        ++out_result->round_trips;
        for (index = 0U; index < current_count; ++index) {
            const tr_raft_message_t *request =
                &round_messages[current_round][index];
            tr_raft_message_t response;
            tr_raft_ready_t ready;

            if (request->type != TR_RAFT_MSG_APPEND_REQUEST ||
                request->entry_count == 0U || next_count >= window) {
                result = TURBO_EPROTO;
                goto cleanup;
            }
            memset(&response, 0, sizeof(response));
            response.type = TR_RAFT_MSG_APPEND_RESPONSE;
            response.from = 2U;
            response.to = 1U;
            response.term = request->term;
            response.previous_log_index = request->previous_log_index;
            response.match_index =
                request->entries[request->entry_count - 1U].index;
            response.granted = true;
            ready = benchmark_ready(
                &round_messages[next_round][next_count],
                window - next_count);
            result = tr_raft_core_step(core, &response, &ready);
            if (result != TURBO_OK ||
                ready.message_count > window - next_count) {
                result = result == TURBO_OK ? TURBO_EPROTO : result;
                goto cleanup;
            }
            next_count += ready.message_count;
            out_result->append_requests += ready.message_count;
            result = tr_raft_core_advance(core);
            if (result != TURBO_OK) {
                goto cleanup;
            }
        }
        current_round = next_round;
        current_count = next_count;
    }

    result = tr_raft_core_status(core, &status);
    if (result == TURBO_OK) {
        result = tr_raft_core_progress(core, &progress);
    }
    if (result != TURBO_OK || progress.peer_count != 2U ||
        progress.peers[1].node_id != 2U) {
        result = result == TURBO_OK ? TURBO_EPROTO : result;
        goto cleanup;
    }
    out_result->match_index = progress.peers[1].match_index;
    out_result->commit_index = status.commit_index;

cleanup:
    tr_raft_core_destroy(core);
    free(round_messages[1]);
    free(round_messages[0]);
    return result;
}

static void benchmark_wire_message(tr_raft_message_t *message)
{
    size_t index;

    memset(message, 0, sizeof(*message));
    message->type = TR_RAFT_MSG_APPEND_REQUEST;
    message->from = 1U;
    message->to = 2U;
    message->term = 7U;
    message->previous_log_index = 100U;
    message->previous_log_term = 6U;
    message->leader_commit = 100U;
    message->entry_count = TR_RAFT_MAX_APPEND_ENTRIES;
    for (index = 0U; index < message->entry_count; ++index) {
        tr_raft_entry_t *entry = &message->entries[index];

        entry->index = message->previous_log_index + index + 1U;
        entry->term = message->term;
        entry->command_id = index + 1U;
        entry->data_length = WIRE_BENCHMARK_ENTRY_BYTES;
        memset(entry->data, (int) index, entry->data_length);
    }
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
    bench("Raft virtual RTT model")
    {
        replication_model_result_t single;
        replication_model_result_t window_four;
        replication_model_result_t window_eight;

        check_int_eq(benchmark_replication_round_trips(
                         1U, REPLICATION_MODEL_ENTRY_COUNT, &single),
                     TURBO_OK);
        check_int_eq(benchmark_replication_round_trips(
                         4U, REPLICATION_MODEL_ENTRY_COUNT, &window_four),
                     TURBO_OK);
        check_int_eq(benchmark_replication_round_trips(
                         8U, REPLICATION_MODEL_ENTRY_COUNT, &window_eight),
                     TURBO_OK);
        check_size_eq(single.round_trips, 33U);
        check_size_eq(window_four.round_trips, 9U);
        check_size_eq(window_eight.round_trips, 5U);
        check_size_eq(single.append_requests, 33U);
        check_size_eq(window_four.append_requests, single.append_requests);
        check_size_eq(window_eight.append_requests, single.append_requests);
        check_long_eq(single.match_index, REPLICATION_MODEL_ENTRY_COUNT);
        check_long_eq(window_four.match_index,
                      REPLICATION_MODEL_ENTRY_COUNT);
        check_long_eq(window_eight.match_index,
                      REPLICATION_MODEL_ENTRY_COUNT);
        check_long_eq(single.commit_index, REPLICATION_MODEL_ENTRY_COUNT);
        check_long_eq(window_four.commit_index,
                      REPLICATION_MODEL_ENTRY_COUNT);
        check_long_eq(window_eight.commit_index,
                      REPLICATION_MODEL_ENTRY_COUNT);
    }

    bench("wire codec")
    {
        static tr_raft_message_t message;
        static tr_raft_message_t decoded;
        static uint8_t frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];
        tr_raft_wire_codec_t *codec = NULL;
        tr_raft_wire_metadata_t metadata;
        tr_raft_wire_metadata_t decoded_metadata;
        size_t frame_size = 0U;
        int first_error = TURBO_OK;

        benchmark_wire_message(&message);
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

        {
            static uint8_t burst_frames[WIRE_BENCHMARK_BURST_ITEMS]
                                       [TR_RAFT_WIRE_MAX_FRAME_SIZE];
            size_t burst_frame_sizes[WIRE_BENCHMARK_BURST_ITEMS];
            size_t burst_bytes = 0U;
            size_t index;

            for (index = 0U; index < WIRE_BENCHMARK_BURST_ITEMS; ++index) {
                metadata.message_id = index + 1U;
                check_int_eq(tr_raft_wire_encode_version(
                                 codec, TR_RAFT_WIRE_VERSION, &metadata,
                                 &message, burst_frames[index],
                                 sizeof(burst_frames[index]),
                                 &burst_frame_sizes[index]),
                             TURBO_OK);
                burst_bytes += burst_frame_sizes[index];
            }
            first_error = TURBO_OK;
            benchmark_io("wire v3 encode 16-frame replication burst",
                         WIRE_BENCHMARK_SAMPLE_COUNT,
                         WIRE_BENCHMARK_BURST_ITEMS, burst_bytes)
            {
                for (index = 0U; index < WIRE_BENCHMARK_BURST_ITEMS;
                     ++index) {
                    int result;

                    metadata.message_id = index + 1U;
                    result = tr_raft_wire_encode_version(
                        codec, TR_RAFT_WIRE_VERSION, &metadata, &message,
                        burst_frames[index], sizeof(burst_frames[index]),
                        &burst_frame_sizes[index]);
                    if (first_error == TURBO_OK && result != TURBO_OK) {
                        first_error = result;
                    }
                    benchmark_result_sink = result;
                }
            }
            check_int_eq(first_error, TURBO_OK);
        }

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

        {
            static uint8_t snapshot_data[
                TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES];
            static uint8_t snapshot_frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];
            tr_raft_snapshot_chunk_t chunk;
            tr_raft_snapshot_chunk_t decoded_chunk;
            size_t snapshot_frame_size = 0U;

            memset(snapshot_data, 0x5a, sizeof(snapshot_data));
            memset(&chunk, 0, sizeof(chunk));
            chunk.from = 1U;
            chunk.to = 2U;
            chunk.term = 3U;
            chunk.snapshot_index = 4U;
            chunk.snapshot_term = 2U;
            chunk.snapshot_size = sizeof(snapshot_data);
            chunk.has_configuration = true;
            chunk.configuration.phase = TR_RAFT_CONF_FINAL;
            chunk.configuration.member_count = 1U;
            chunk.configuration.members[0].node_id = 2U;
            chunk.configuration.members[0].roles =
                TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER;
            chunk.data = snapshot_data;
            chunk.data_length = sizeof(snapshot_data);
            chunk.done = true;
            memset(chunk.snapshot_digest, 0x3c,
                   sizeof(chunk.snapshot_digest));
            check_int_eq(tr_raft_wire_encode_snapshot_chunk(
                             codec, &metadata, &chunk, snapshot_frame,
                             sizeof(snapshot_frame), &snapshot_frame_size),
                         TURBO_OK);

            first_error = TURBO_OK;
            benchmark_io("snapshot V5 encode 64KiB",
                         WIRE_BENCHMARK_SAMPLE_COUNT, 1U,
                         snapshot_frame_size)
            {
                int result = tr_raft_wire_encode_snapshot_chunk(
                    codec, &metadata, &chunk, snapshot_frame,
                    sizeof(snapshot_frame), &snapshot_frame_size);
                if (first_error == TURBO_OK && result != TURBO_OK) {
                    first_error = result;
                }
                benchmark_result_sink = result;
            }
            check_int_eq(first_error, TURBO_OK);

            first_error = TURBO_OK;
            benchmark_io("snapshot V5 decode 64KiB borrowed",
                         WIRE_BENCHMARK_SAMPLE_COUNT, 1U,
                         snapshot_frame_size)
            {
                int result = tr_raft_wire_decode_snapshot_chunk(
                    codec, snapshot_frame, snapshot_frame_size,
                    &decoded_metadata, &decoded_chunk);
                if (first_error == TURBO_OK && result != TURBO_OK) {
                    first_error = result;
                }
                benchmark_result_sink = result;
            }
            check_int_eq(first_error, TURBO_OK);
        }
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
