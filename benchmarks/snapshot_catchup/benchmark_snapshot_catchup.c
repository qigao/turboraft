#include <turboraft/raft_snapshot_receiver.h>
#include <turboraft/raft_snapshot_sender.h>
#include <turboraft/raft_wire_codec.h>

#include <salts_error.h>

#include <dirent.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <time.h>

enum {
    BENCH_SNAPSHOT_BYTES = 4U * 1024U * 1024U,
    BENCH_CHUNK_BYTES = TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES,
    BENCH_MAX_INFLIGHT = TR_RAFT_SNAPSHOT_RECOMMENDED_INFLIGHT_CHUNKS,
    BENCH_TRANSPORT_PREFIX_BYTES = 4U
};

typedef struct bench_resource_sample {
    uint64_t user_ns;
    uint64_t sys_ns;
    long max_rss_kb;
    size_t fd_count;
} bench_resource_sample_t;

typedef struct bench_sink {
    uint64_t expected_size;
    uint64_t written;
    int begun;
    int committed;
    int aborted;
} bench_sink_t;

static const tr_raft_conf_t bench_configuration = {
    TR_RAFT_CONF_FINAL, 0U, 2U,
    {
        {1U, TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER},
        {2U, TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER}
    }
};

static uint64_t bench_now_ns(void)
{
    struct timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0U;
    }
    return (uint64_t)ts.tv_sec * UINT64_C(1000000000) +
           (uint64_t)ts.tv_nsec;
}

static uint64_t bench_timeval_ns(const struct timeval *value)
{
    return (uint64_t)value->tv_sec * UINT64_C(1000000000) +
           (uint64_t)value->tv_usec * UINT64_C(1000);
}

static size_t bench_fd_count(void)
{
    DIR *directory = opendir("/proc/self/fd");
    struct dirent *entry;
    size_t count = 0U;

    if (directory == NULL) {
        return 0U;
    }
    while ((entry = readdir(directory)) != NULL) {
        if (strcmp(entry->d_name, ".") != 0 &&
            strcmp(entry->d_name, "..") != 0) {
            ++count;
        }
    }
    (void)closedir(directory);
    return count == 0U ? 0U : count - 1U;
}

static int bench_resource_sample(bench_resource_sample_t *out)
{
    struct rusage usage;

    if (out == NULL) {
        return SALTS_EINVAL;
    }
    memset(out, 0, sizeof(*out));
    if (getrusage(RUSAGE_SELF, &usage) != 0) {
        return SALTS_EIO;
    }
    out->user_ns = bench_timeval_ns(&usage.ru_utime);
    out->sys_ns = bench_timeval_ns(&usage.ru_stime);
    out->max_rss_kb = usage.ru_maxrss;
    out->fd_count = bench_fd_count();
    return SALTS_OK;
}

static void bench_cluster_id(tr_raft_cluster_id_t *cluster_id)
{
    size_t index;

    memset(cluster_id, 0, sizeof(*cluster_id));
    for (index = 0U; index < sizeof(cluster_id->bytes); ++index) {
        cluster_id->bytes[index] = (uint8_t)(0x31U + index);
    }
}

static int bench_sink_begin(void *context,
                            tr_raft_term_t leader_term,
                            tr_raft_index_t snapshot_index,
                            tr_raft_term_t snapshot_term,
                            const tr_raft_conf_t *configuration,
                            uint64_t snapshot_size)
{
    bench_sink_t *sink = (bench_sink_t *)context;

    if (sink == NULL || configuration == NULL ||
        leader_term != 7U || snapshot_index != 100U ||
        snapshot_term != 6U || snapshot_size != sink->expected_size) {
        return SALTS_EPROTO;
    }
    sink->written = 0U;
    sink->begun = 1;
    sink->committed = 0;
    sink->aborted = 0;
    return SALTS_OK;
}

static int bench_sink_write(void *context,
                            uint64_t offset,
                            const uint8_t *data,
                            size_t size)
{
    bench_sink_t *sink = (bench_sink_t *)context;

    if (sink == NULL || !sink->begun || sink->committed ||
        offset != sink->written ||
        (size != 0U && data == NULL) ||
        size > sink->expected_size - sink->written) {
        return SALTS_EPROTO;
    }
    sink->written += size;
    return SALTS_OK;
}

static int bench_sink_commit(void *context)
{
    bench_sink_t *sink = (bench_sink_t *)context;

    if (sink == NULL || !sink->begun ||
        sink->written != sink->expected_size) {
        return SALTS_EPROTO;
    }
    sink->committed = 1;
    return SALTS_OK;
}

static void bench_sink_abort(void *context)
{
    bench_sink_t *sink = (bench_sink_t *)context;

    if (sink != NULL) {
        sink->aborted = 1;
    }
}

static int bench_encode_decode_chunk(
    tr_raft_wire_codec_t *leader_codec,
    tr_raft_wire_codec_t *follower_codec,
    uint64_t message_id,
    const tr_raft_snapshot_chunk_t *chunk,
    tr_raft_snapshot_chunk_t *decoded,
    uint64_t *out_wire_bytes)
{
    tr_raft_wire_metadata_t metadata;
    tr_raft_wire_metadata_t decoded_metadata;
    uint8_t frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];
    size_t frame_size = 0U;
    int result;

    memset(&metadata, 0, sizeof(metadata));
    memset(&decoded_metadata, 0, sizeof(decoded_metadata));
    bench_cluster_id(&metadata.cluster_id);
    metadata.group_id = 77U;
    metadata.message_id = message_id;

    result = tr_raft_wire_encode_snapshot_chunk(
        leader_codec, &metadata, chunk,
        frame, sizeof(frame), &frame_size);
    if (result != SALTS_OK) {
        return result;
    }
    result = tr_raft_wire_decode_snapshot_chunk(
        follower_codec, frame, frame_size,
        &decoded_metadata, decoded);
    if (result != SALTS_OK ||
        decoded_metadata.group_id != metadata.group_id ||
        decoded_metadata.message_id != metadata.message_id) {
        return result == SALTS_OK ? SALTS_EPROTO : result;
    }
    *out_wire_bytes += frame_size + BENCH_TRANSPORT_PREFIX_BYTES;
    return SALTS_OK;
}

static int bench_encode_decode_ack(
    tr_raft_wire_codec_t *follower_codec,
    tr_raft_wire_codec_t *leader_codec,
    uint64_t message_id,
    const tr_raft_snapshot_ack_t *ack,
    tr_raft_snapshot_ack_t *decoded,
    uint64_t *out_wire_bytes)
{
    tr_raft_wire_metadata_t metadata;
    tr_raft_wire_metadata_t decoded_metadata;
    uint8_t frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];
    size_t frame_size = 0U;
    int result;

    memset(&metadata, 0, sizeof(metadata));
    memset(&decoded_metadata, 0, sizeof(decoded_metadata));
    bench_cluster_id(&metadata.cluster_id);
    metadata.group_id = 77U;
    metadata.message_id = message_id;

    result = tr_raft_wire_encode_snapshot_ack(
        follower_codec, &metadata, ack,
        frame, sizeof(frame), &frame_size);
    if (result != SALTS_OK) {
        return result;
    }
    result = tr_raft_wire_decode_snapshot_ack(
        leader_codec, frame, frame_size,
        &decoded_metadata, decoded);
    if (result != SALTS_OK ||
        decoded_metadata.group_id != metadata.group_id ||
        decoded_metadata.message_id != metadata.message_id) {
        return result == SALTS_OK ? SALTS_EPROTO : result;
    }
    *out_wire_bytes += frame_size + BENCH_TRANSPORT_PREFIX_BYTES;
    return SALTS_OK;
}

int main(void)
{
    tr_raft_snapshot_sender_config_t sender_config;
    tr_raft_snapshot_receiver_config_t receiver_config;
    tr_raft_snapshot_sender_t *sender = NULL;
    tr_raft_snapshot_receiver_t *receiver = NULL;
    tr_raft_wire_codec_t *leader_codec = NULL;
    tr_raft_wire_codec_t *follower_codec = NULL;
    tr_raft_snapshot_sender_status_t sender_status;
    bench_resource_sample_t before;
    bench_resource_sample_t after;
    bench_sink_t sink;
    uint8_t *snapshot = NULL;
    uint64_t chunk_wire_bytes = 0U;
    uint64_t ack_wire_bytes = 0U;
    uint64_t payload_bytes = 0U;
    uint64_t leader_message_id = 0U;
    uint64_t follower_message_id = 0U;
    uint64_t start_ns;
    uint64_t end_ns;
    size_t chunk_count = 0U;
    int result = SALTS_OK;

    snapshot = (uint8_t *)malloc(BENCH_SNAPSHOT_BYTES);
    if (snapshot == NULL) {
        return 10;
    }
    for (size_t index = 0U; index < BENCH_SNAPSHOT_BYTES; ++index) {
        snapshot[index] = (uint8_t)((index * 31U + 7U) & 0xffU);
    }

    memset(&sender_config, 0, sizeof(sender_config));
    sender_config.self_id = 1U;
    sender_config.peer_id = 2U;
    sender_config.max_snapshot_bytes = BENCH_SNAPSHOT_BYTES;
    sender_config.chunk_size = BENCH_CHUNK_BYTES;
    sender_config.max_inflight_chunks = BENCH_MAX_INFLIGHT;
    result = tr_raft_snapshot_sender_create(&sender_config, &sender);
    if (result == SALTS_OK) {
        result = tr_raft_snapshot_sender_begin(
            sender, 7U, 100U, 6U, &bench_configuration,
            snapshot, BENCH_SNAPSHOT_BYTES);
    }

    memset(&sink, 0, sizeof(sink));
    sink.expected_size = BENCH_SNAPSHOT_BYTES;
    memset(&receiver_config, 0, sizeof(receiver_config));
    receiver_config.self_id = 2U;
    receiver_config.max_snapshot_bytes = BENCH_SNAPSHOT_BYTES;
    receiver_config.stream.begin = bench_sink_begin;
    receiver_config.stream.write = bench_sink_write;
    receiver_config.stream.commit = bench_sink_commit;
    receiver_config.stream.abort = bench_sink_abort;
    receiver_config.stream.context = &sink;
    if (result == SALTS_OK) {
        result = tr_raft_snapshot_receiver_create(
            &receiver_config, &receiver);
    }
    if (result == SALTS_OK) {
        result = tr_raft_wire_codec_create(&leader_codec);
    }
    if (result == SALTS_OK) {
        result = tr_raft_wire_codec_create(&follower_codec);
    }
    if (result == SALTS_OK) {
        result = bench_resource_sample(&before);
    }

    start_ns = bench_now_ns();
    while (result == SALTS_OK) {
        tr_raft_snapshot_ack_t pending_acks[BENCH_MAX_INFLIGHT];
        size_t pending_count = 0U;
        int saw_done = 0;

        memset(pending_acks, 0, sizeof(pending_acks));
        while (pending_count < BENCH_MAX_INFLIGHT && !saw_done) {
            tr_raft_snapshot_chunk_t chunk;
            tr_raft_snapshot_chunk_t decoded_chunk;
            tr_raft_snapshot_receive_result_t receive_result;
            int next_result;

            memset(&chunk, 0, sizeof(chunk));
            memset(&decoded_chunk, 0, sizeof(decoded_chunk));
            memset(&receive_result, 0, sizeof(receive_result));
            next_result = tr_raft_snapshot_sender_next_chunk(
                sender, &chunk);
            if (next_result == SALTS_EBUSY) {
                break;
            }
            if (next_result != SALTS_OK) {
                result = next_result;
                break;
            }

            ++leader_message_id;
            result = bench_encode_decode_chunk(
                leader_codec, follower_codec, leader_message_id,
                &chunk, &decoded_chunk, &chunk_wire_bytes);
            if (result != SALTS_OK) {
                break;
            }
            result = tr_raft_snapshot_receiver_handle(
                receiver, &decoded_chunk, &receive_result);
            if (result != SALTS_OK || !receive_result.ack.accepted) {
                result = result == SALTS_OK ? SALTS_EPROTO : result;
                break;
            }

            payload_bytes += decoded_chunk.data_length;
            ++chunk_count;
            pending_acks[pending_count++] = receive_result.ack;
            saw_done = decoded_chunk.done ? 1 : 0;
        }

        if (result != SALTS_OK) {
            break;
        }
        if (pending_count == 0U) {
            result = SALTS_EPROTO;
            break;
        }

        for (size_t index = 0U;
             result == SALTS_OK && index < pending_count;
             ++index) {
            tr_raft_snapshot_ack_t decoded_ack;

            memset(&decoded_ack, 0, sizeof(decoded_ack));
            ++follower_message_id;
            result = bench_encode_decode_ack(
                follower_codec, leader_codec, follower_message_id,
                &pending_acks[index], &decoded_ack, &ack_wire_bytes);
            if (result == SALTS_OK) {
                result = tr_raft_snapshot_sender_acknowledge(
                    sender, &decoded_ack);
            }
        }
        if (result != SALTS_OK) {
            break;
        }
        result = tr_raft_snapshot_sender_get_status(
            sender, &sender_status);
        if (result == SALTS_OK && sender_status.complete) {
            break;
        }
    }
    end_ns = bench_now_ns();

    if (result == SALTS_OK) {
        result = bench_resource_sample(&after);
    }
    if (result == SALTS_OK &&
        (payload_bytes != BENCH_SNAPSHOT_BYTES ||
         !sink.committed || sink.aborted ||
         chunk_count != BENCH_SNAPSHOT_BYTES / BENCH_CHUNK_BYTES)) {
        result = SALTS_EPROTO;
    }

    if (result == SALTS_OK) {
        const uint64_t duration_ns = end_ns - start_ns;
        const uint64_t total_wire_bytes =
            chunk_wire_bytes + ack_wire_bytes;
        const double throughput_mib_s =
            duration_ns == 0U
                ? 0.0
                : ((double)payload_bytes / (1024.0 * 1024.0)) /
                      ((double)duration_ns / 1000000000.0);
        const double wire_amplification =
            (double)total_wire_bytes / (double)payload_bytes;

        printf("phase,payload_bytes,chunks,chunk_wire_bytes,ack_wire_bytes,"
               "total_wire_bytes,wire_amplification,duration_ns,"
               "throughput_mib_s,user_cpu_ns,sys_cpu_ns,max_rss_kb,"
               "fd_count,max_inflight_chunks\n");
        printf("snapshot_wire_catchup,%" PRIu64 ",%zu,%" PRIu64
               ",%" PRIu64 ",%" PRIu64 ",%.6f,%" PRIu64
               ",%.2f,%" PRIu64 ",%" PRIu64 ",%ld,%zu,%u\n",
               payload_bytes, chunk_count, chunk_wire_bytes,
               ack_wire_bytes, total_wire_bytes, wire_amplification,
               duration_ns, throughput_mib_s,
               after.user_ns - before.user_ns,
               after.sys_ns - before.sys_ns,
               after.max_rss_kb, after.fd_count,
               (unsigned)BENCH_MAX_INFLIGHT);
    } else {
        fprintf(stderr, "snapshot wire catch-up benchmark failed: %d\n",
                result);
    }

    tr_raft_wire_codec_destroy(follower_codec);
    tr_raft_wire_codec_destroy(leader_codec);
    tr_raft_snapshot_receiver_destroy(receiver);
    tr_raft_snapshot_sender_destroy(sender);
    free(snapshot);
    return result == SALTS_OK ? 0 : 1;
}
