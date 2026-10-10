#include <turboraft/raft_snapshot_sender.h>
#include <turboraft/raft_snapshot_receiver.h>
#include <turboraft/raft_wire_codec.h>

#include <cmeta_crypto.h>
#include <cmeta_error.h>

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#if defined(__linux__)
#include <sys/resource.h>
#endif

/*
 * Exact installed SDK conformance, not a throughput microbenchmark or an
 * actual CNet socket test. 64 MiB is provided by generated read_at bytes:
 * no 64 MiB allocation exists in the test, sender, or streaming receiver.
 *
 * The same owned 4-slot, 64-KiB snapshot window is exercised through
 * full Raft wire frame encoding/decoding and cumulative ACKs. We assert
 * bounded credits while the receiver incrementally SHA-256 verifies the
 * complete transfer, then allows one terminal stream.commit().
 */
enum {
    STREAM_CHUNK = TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES,
    STREAM_WINDOW = TR_RAFT_SNAPSHOT_RECOMMENDED_INFLIGHT_CHUNKS,
    STREAM_SIZE = 64U * 1024U * 1024U,
    STREAM_TOTAL_CHUNKS = STREAM_SIZE / STREAM_CHUNK
};

typedef struct source_probe {
    uint64_t size;
    size_t read_calls;
    size_t max_request;
    size_t releases;
} source_probe_t;

typedef struct sink_probe {
    uint64_t expected_size;
    uint64_t written;
    size_t max_write;
    size_t writes;
    size_t starts;
    size_t commits;
    size_t aborts;
} sink_probe_t;

static const tr_raft_conf_t CONFIGURATION = {
    TR_RAFT_CONF_FINAL, 1U, 1U,
    {{2U, TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER}}
};

static uint8_t at(uint64_t offset)
{
    return (uint8_t)((offset * 31U + 7U) & 0xffU);
}

static int source_read(void *context, uint64_t offset, uint8_t *buffer,
                       size_t capacity, size_t *out_size)
{
    source_probe_t *source = (source_probe_t *)context;
    size_t i;
    if (source == NULL || out_size == NULL || offset > source->size ||
        capacity > source->size - offset ||
        (capacity != 0U && buffer == NULL) ||
        capacity > STREAM_CHUNK) return SALTS_EINVAL;
    *out_size = 0U;
    ++source->read_calls;
    if (capacity > source->max_request) source->max_request = capacity;
    for (i = 0U; i < capacity; ++i)
        buffer[i] = at(offset + i);
    *out_size = capacity;
    return SALTS_OK;
}

static void source_release(void *context)
{
    source_probe_t *source = (source_probe_t *)context;
    if (source != NULL) ++source->releases;
}

static int sink_begin(void *context, tr_raft_term_t leader_term,
                      tr_raft_index_t snapshot_index,
                      tr_raft_term_t snapshot_term,
                      const tr_raft_conf_t *config, uint64_t size)
{
    sink_probe_t *sink = (sink_probe_t *)context;
    if (sink == NULL || sink->starts != 0U || config == NULL ||
        leader_term != 7U || snapshot_index != 100U ||
        snapshot_term != 6U || size != sink->expected_size ||
        config->member_count != 1U ||
        config->members[0].node_id != 2U)
        return SALTS_EPROTO;
    ++sink->starts;
    return SALTS_OK;
}

static int sink_write(void *context, uint64_t offset,
                      const uint8_t *data, size_t count)
{
    sink_probe_t *sink = (sink_probe_t *)context;
    size_t i;
    if (sink == NULL || sink->starts != 1U || sink->commits != 0U ||
        sink->aborts != 0U || count > STREAM_CHUNK ||
        offset != sink->written || count > sink->expected_size - offset ||
        (count != 0U && data == NULL))
        return SALTS_EPROTO;
    for (i = 0U; i < count; ++i)
        if (data[i] != at(offset + i))
            return SALTS_EPROTO;
    sink->written += count;
    ++sink->writes;
    if (count > sink->max_write) sink->max_write = count;
    return SALTS_OK;
}

static int sink_commit(void *context)
{
    sink_probe_t *sink = (sink_probe_t *)context;
    if (sink == NULL || sink->starts != 1U ||
        sink->commits != 0U || sink->aborts != 0U ||
        sink->written != sink->expected_size)
        return SALTS_EPROTO;
    ++sink->commits;
    return SALTS_OK;
}

static void sink_abort(void *context)
{
    sink_probe_t *sink = (sink_probe_t *)context;
    if (sink != NULL) ++sink->aborts;
}

static int digest_generated(uint8_t digest[TR_RAFT_WIRE_SNAPSHOT_DIGEST_SIZE])
{
    cmeta_sha256_stream *hash = NULL;
    uint8_t generated[STREAM_CHUNK];
    uint64_t offset;
    size_t i;
    int result = cmeta_sha256_stream_create(&hash);
    if (result != SALTS_OK) return result;
    for (offset = 0U; offset < STREAM_SIZE && result == SALTS_OK;
         offset += STREAM_CHUNK) {
        for (i = 0U; i < sizeof(generated); ++i)
            generated[i] = at(offset + i);
        result = cmeta_sha256_stream_update(hash, generated,
                                            sizeof(generated));
    }
    if (result == SALTS_OK)
        result = cmeta_sha256_stream_finish(hash, digest);
    cmeta_sha256_stream_destroy(hash);
    return result;
}

static int fail(int code, const char *why)
{
    fprintf(stderr, "FAIL large streaming snapshot: %s (rc=%d)\n",
            why, code);
    return code == SALTS_OK ? SALTS_EPROTO : code;
}

static void cluster_id(tr_raft_cluster_id_t *id)
{
    size_t i;
    memset(id, 0, sizeof(*id));
    for (i = 0U; i < sizeof(id->bytes); ++i)
        id->bytes[i] = (uint8_t)(0x30U + i);
}

int main(void)
{
    tr_raft_snapshot_sender_config_t sender_config = {0};
    tr_raft_snapshot_receiver_config_t receiver_config = {0};
    tr_raft_snapshot_source_t source = {0};
    tr_raft_snapshot_sender_t *sender = NULL;
    tr_raft_snapshot_receiver_t *receiver = NULL;
    tr_raft_wire_codec_t *leader_codec = NULL, *follower_codec = NULL;
    tr_raft_snapshot_sender_status_t status = {0};
    tr_raft_wire_metadata_t metadata = {0}, decoded_meta = {0};
    source_probe_t source_probe = {STREAM_SIZE, 0U, 0U, 0U};
    sink_probe_t sink = {STREAM_SIZE, 0U, 0U, 0U, 0U, 0U, 0U};
    uint8_t wire[TR_RAFT_WIRE_MAX_FRAME_SIZE];
    uint64_t sent = 0U, message_id = 0U;
    size_t chunks = 0U, windows = 0U, max_inflight = 0U;
    int result;
#define ENSURE(test, why) do { if (!(test)) { \
    result = fail(SALTS_EPROTO, why); goto done; } } while (0)

    source.context = &source_probe;
    source.read_at = source_read;
    source.release = source_release;
    source.size = STREAM_SIZE;
    result = digest_generated(source.digest);
    if (result != SALTS_OK) goto done;

    sender_config.self_id = 1U;
    sender_config.peer_id = 2U;
    sender_config.max_snapshot_bytes = STREAM_SIZE;
    sender_config.chunk_size = STREAM_CHUNK;
    sender_config.max_inflight_chunks = STREAM_WINDOW;
    receiver_config.self_id = 2U;
    receiver_config.max_snapshot_bytes = STREAM_SIZE;
    /* Streaming path MUST not have a whole-buffer compatibility cap. */
    receiver_config.max_buffered_snapshot_bytes = 0U;
    receiver_config.stream.context = &sink;
    receiver_config.stream.begin = sink_begin;
    receiver_config.stream.write = sink_write;
    receiver_config.stream.commit = sink_commit;
    receiver_config.stream.abort = sink_abort;
    result = tr_raft_snapshot_sender_create(&sender_config, &sender);
    if (result != SALTS_OK) goto done;
    result = tr_raft_snapshot_receiver_create(&receiver_config, &receiver);
    if (result != SALTS_OK) goto done;
    result = tr_raft_wire_codec_create(&leader_codec);
    if (result != SALTS_OK) goto done;
    result = tr_raft_wire_codec_create(&follower_codec);
    if (result != SALTS_OK) goto done;
    result = tr_raft_snapshot_sender_begin_source(
        sender, 7U, 100U, 6U, &CONFIGURATION, &source);
    if (result != SALTS_OK) goto done;

    metadata.group_id = 77U;
    cluster_id(&metadata.cluster_id);
    while (sent < STREAM_SIZE && result == SALTS_OK) {
        tr_raft_snapshot_ack_t acks[STREAM_WINDOW];
        size_t pending = 0U, i;
        memset(acks, 0, sizeof(acks));
        for (i = 0U; i < STREAM_WINDOW && sent < STREAM_SIZE; ++i) {
            tr_raft_snapshot_chunk_t chunk = {0}, decoded = {0};
            tr_raft_snapshot_receive_result_t receipt = {0};
            size_t length = 0U;
            result = tr_raft_snapshot_sender_next_chunk(sender, &chunk);
            if (result != SALTS_OK) goto done;
            ENSURE(chunk.snapshot_offset == sent &&
                   chunk.data_length == STREAM_CHUNK &&
                   chunk.snapshot_size == STREAM_SIZE &&
                   chunk.from == 1U && chunk.to == 2U &&
                   chunk.done == (sent + STREAM_CHUNK == STREAM_SIZE),
                   "bounded sender chunk/identity/offset");
            metadata.message_id = ++message_id;
            result = tr_raft_wire_encode_snapshot_chunk(
                leader_codec, &metadata, &chunk,
                wire, sizeof(wire), &length);
            if (result != SALTS_OK) goto done;
            result = tr_raft_wire_decode_snapshot_chunk(
                follower_codec, wire, length, &decoded_meta, &decoded);
            if (result != SALTS_OK) goto done;
            ENSURE(decoded_meta.group_id == 77U &&
                   decoded_meta.message_id == message_id &&
                   decoded.snapshot_offset == sent,
                   "Raft wire roundtrip envelope identity");
            result = tr_raft_snapshot_receiver_handle(
                receiver, &decoded, &receipt);
            if (result != SALTS_OK) goto done;
            ENSURE(receipt.ack.accepted &&
                   receipt.ack.next_offset == sent + STREAM_CHUNK &&
                   receipt.installed == (sent + STREAM_CHUNK == STREAM_SIZE),
                   "receiver progress is never installed until final chunk");
            acks[pending++] = receipt.ack;
            ++chunks;
            sent += STREAM_CHUNK;
        }
        result = tr_raft_snapshot_sender_get_status(sender, &status);
        if (result != SALTS_OK) goto done;
        ENSURE(status.inflight_chunks == pending &&
               status.inflight_chunks <= STREAM_WINDOW &&
               status.max_inflight_chunks == STREAM_WINDOW,
               "fixed bounded in-flight sender credits");
        if (pending > max_inflight) max_inflight = pending;
        if (sent < STREAM_SIZE) {
            tr_raft_snapshot_chunk_t blocked = {0};
            ENSURE(tr_raft_snapshot_sender_next_chunk(sender, &blocked)
                       == SALTS_EBUSY,
                   "fifth concurrent Snapshot claim is not admitted");
            ++windows;
        }
        for (i = 0U; i < pending; ++i) {
            tr_raft_snapshot_ack_t decoded_ack = {0};
            size_t length = 0U;
            metadata.message_id = ++message_id;
            result = tr_raft_wire_encode_snapshot_ack(
                follower_codec, &metadata, &acks[i],
                wire, sizeof(wire), &length);
            if (result != SALTS_OK) goto done;
            result = tr_raft_wire_decode_snapshot_ack(
                leader_codec, wire, length, &decoded_meta, &decoded_ack);
            if (result != SALTS_OK) goto done;
            ENSURE(decoded_meta.group_id == 77U &&
                   decoded_meta.message_id == message_id &&
                   decoded_ack.next_offset == acks[i].next_offset,
                   "cumulative progress ACK wire identity");
            result = tr_raft_snapshot_sender_acknowledge(
                sender, &decoded_ack);
            if (result != SALTS_OK) goto done;
        }
    }

    result = tr_raft_snapshot_sender_get_status(sender, &status);
    if (result != SALTS_OK) goto done;
    ENSURE(status.complete && status.acknowledged_offset == STREAM_SIZE &&
           status.inflight_chunks == 0U &&
           chunks == STREAM_TOTAL_CHUNKS &&
           sink.starts == 1U && sink.commits == 1U &&
           sink.aborts == 0U && sink.written == STREAM_SIZE &&
           sink.writes == STREAM_TOTAL_CHUNKS &&
           sink.max_write <= STREAM_CHUNK &&
           source_probe.read_calls == STREAM_TOTAL_CHUNKS &&
           source_probe.max_request == STREAM_CHUNK &&
           source_probe.releases == 1U &&
           max_inflight == STREAM_WINDOW && windows > 0U,
           "64 MiB verified SHA-256 stream, exactly once commit and resource caps");

#if defined(__linux__)
    {
        struct rusage use;
        result = getrusage(RUSAGE_SELF, &use) == 0 ? SALTS_OK : SALTS_EIO;
        if (result != SALTS_OK) goto done;
        /* Linux ru_maxrss is KiB. This is a process RSS high-water bound,
         * not a precise heap allocation proof. It is strictly below the
         * 64-MiB logical Snapshot by a large margin. The exact bounded
         * sender slots and streaming read/write limits are asserted above. */
        ENSURE(use.ru_maxrss > 0 &&
               (uint64_t)use.ru_maxrss <= UINT64_C(48) * 1024U,
               "process peak RSS >48 MiB despite 64 MiB streaming source");
        printf("large_snapshot_stream,rss_peak_kib=%ld,", use.ru_maxrss);
    }
#endif
    printf("snapshot_bytes=%u,chunks=%zu,max_inflight=%zu,"
           "windows_blocked=%zu,source_max_read=%zu,"
           "sink_max_write=%zu\n",
           (unsigned)STREAM_SIZE, chunks, max_inflight, windows,
           source_probe.max_request, sink.max_write);
done:
    tr_raft_wire_codec_destroy(follower_codec);
    tr_raft_wire_codec_destroy(leader_codec);
    tr_raft_snapshot_receiver_destroy(receiver);
    tr_raft_snapshot_sender_destroy(sender);
    if (result != SALTS_OK)
        fprintf(stderr, "large Snapshot qualification rc=%d\n", result);
#undef ENSURE
    return result == SALTS_OK ? 0 : 1;
}
