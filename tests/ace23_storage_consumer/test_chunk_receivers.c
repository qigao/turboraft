#if !defined(_WIN32)
#define _POSIX_C_SOURCE 200809L
#endif

#include <turboraft/raft_multicore_chunk_receivers.h>
#include <tinytest.h>
#include <cmeta_error.h>

#include <stdio.h>
#include <string.h>

#if defined(_WIN32)
#include <io.h>
#else
#include <unistd.h>
#endif

/* SHA-256("abcdef"), independently fixed from known digest vectors. */
static const uint8_t SIX_BYTE_DIGEST[32] = {
    0xbeU, 0xf5U, 0x7eU, 0xc7U, 0xf5U, 0x3aU, 0x6dU, 0x40U,
    0xbeU, 0xb6U, 0x40U, 0xa7U, 0x80U, 0xa6U, 0x39U, 0xc8U,
    0x3bU, 0xc2U, 0x9aU, 0xc8U, 0xa9U, 0x81U, 0x6fU, 0x1fU,
    0xc6U, 0xc5U, 0xc6U, 0xdcU, 0xd9U, 0x3cU, 0x47U, 0x21U
};

typedef struct synced_sink {
    FILE *file;
    size_t used;
    unsigned began, wrote, fsynced, aborted;
    bool fail_commit;
} synced_sink;

static int sink_begin_common(synced_sink *sink, uint64_t size)
{
    if (sink == NULL || sink->file == NULL || size != 6U)
        return SALTS_EPROTO;
    sink->used = 0U;
    ++sink->began;
    return SALTS_OK;
}

static int snap_begin(void *context, tr_raft_term_t term,
                      tr_raft_index_t index, tr_raft_term_t snap_term,
                      const tr_raft_conf_t *conf, uint64_t size)
{
    if (term != 5U || index != 9U || snap_term != 4U ||
        conf == NULL || conf->member_count != 1U)
        return SALTS_EPROTO;
    return sink_begin_common((synced_sink *)context, size);
}

static int data_begin(void *context, tr_raft_node_id_t from,
                      tr_raft_term_t term, uint64_t stream_id,
                      uint64_t size, const uint8_t digest[32])
{
    if (from != 1U || term != 5U || stream_id != 9U ||
        digest == NULL || memcmp(digest, SIX_BYTE_DIGEST, 32U) != 0)
        return SALTS_EPROTO;
    return sink_begin_common((synced_sink *)context, size);
}

static int sink_write(void *context, uint64_t offset,
                      const uint8_t *bytes, size_t size)
{
    synced_sink *sink = (synced_sink *)context;
    if (sink == NULL || sink->file == NULL ||
        bytes == NULL || offset != sink->used ||
        size > 6U - sink->used)
        return SALTS_EPROTO;
    if (fwrite(bytes, 1U, size, sink->file) != size)
        return SALTS_EIO;
    sink->used += size;
    ++sink->wrote;
    return SALTS_OK;
}

static int sink_commit(void *context)
{
    synced_sink *sink = (synced_sink *)context;
    int status;
    if (sink == NULL || sink->file == NULL || sink->used != 6U)
        return SALTS_EPROTO;
    if (sink->fail_commit) return SALTS_EIO;
    if (fflush(sink->file) != 0) return SALTS_EIO;
#if defined(_WIN32)
    status = _commit(_fileno(sink->file));
#else
    status = fsync(fileno(sink->file));
#endif
    if (status != 0) return SALTS_EIO;
    ++sink->fsynced;
    return SALTS_OK;
}

static void sink_abort(void *context)
{
    synced_sink *sink = (synced_sink *)context;
    if (sink != NULL) ++sink->aborted;
}

static tr_raft_transport_payload_t snapshot_chunk(
    const uint8_t *data, size_t offset, size_t length, bool done)
{
    tr_raft_transport_payload_t payload = {0};
    tr_raft_snapshot_chunk_t *c = &payload.data.snapshot_chunk;
    payload.group_id = 103U;
    payload.kind = TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK;
    c->from = 1U;
    c->to = 2U;
    c->term = 5U;
    c->snapshot_index = 9U;
    c->snapshot_term = 4U;
    c->snapshot_offset = offset;
    c->snapshot_size = 6U;
    c->data = data;
    c->data_length = length;
    c->done = done;
    c->has_configuration = offset == 0U;
    if (c->has_configuration) {
        c->configuration.phase = TR_RAFT_CONF_FINAL;
        c->configuration.member_count = 1U;
        c->configuration.members[0].node_id = 2U;
        c->configuration.members[0].roles =
            TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER;
    }
    memcpy(c->snapshot_digest, SIX_BYTE_DIGEST, sizeof(SIX_BYTE_DIGEST));
    return payload;
}

static tr_raft_transport_payload_t data_chunk(
    const uint8_t *data, size_t offset, size_t length, bool done)
{
    tr_raft_transport_payload_t payload = {0};
    tr_raft_data_chunk_t *c = &payload.data.data_chunk;
    payload.group_id = 101U;
    payload.kind = TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK;
    c->from = 1U;
    c->to = 2U;
    c->term = 5U;
    c->stream_id = 9U;
    c->stream_offset = offset;
    c->stream_size = 6U;
    c->data = data;
    c->data_length = length;
    c->done = done;
    memcpy(c->stream_digest, SIX_BYTE_DIGEST, sizeof(SIX_BYTE_DIGEST));
    return payload;
}

static tr_raft_snapshot_receiver_config_t snap_config(synced_sink *sink)
{
    tr_raft_snapshot_receiver_config_t config = {0};
    config.self_id = 2U;
    config.max_snapshot_bytes = 32U;
    config.stream.context = sink;
    config.stream.begin = snap_begin;
    config.stream.write = sink_write;
    config.stream.commit = sink_commit;
    config.stream.abort = sink_abort;
    return config;
}

static tr_raft_data_stream_receiver_config_t data_config(synced_sink *sink)
{
    tr_raft_data_stream_receiver_config_t config = {0};
    config.self_id = 2U;
    config.max_stream_bytes = 32U;
    config.sink.context = sink;
    config.sink.begin = data_begin;
    config.sink.write = sink_write;
    config.sink.commit = sink_commit;
    config.sink.abort = sink_abort;
    return config;
}

spec("ACE 2.3 actual owner-local Snapshot/Data receiver ACK semantics")
{
    it("acknowledges SNAPSHOT offsets but reports installed only after sink fsync")
    {
        synced_sink sink = {0};
        tr_raft_snapshot_receiver_t *receiver = NULL;
        tr_raft_snapshot_receiver_config_t config;
        tr_raft_multicore_chunk_receivers_t bind = {0};
        tr_raft_multicore_chunk_result_t result = {0};
        tr_raft_transport_payload_t first, last;
        uint8_t saved[6] = {0};

        sink.file = tmpfile();
        check_not_null(sink.file);
        config = snap_config(&sink);
        check_equal(tr_raft_snapshot_receiver_create(&config, &receiver), SALTS_OK);
        bind.snapshot = receiver;
        first = snapshot_chunk((const uint8_t *)"abc", 0U, 3U, false);
        last = snapshot_chunk((const uint8_t *)"def", 3U, 3U, true);
        check_equal(tr_raft_multicore_chunk_receivers_handle(
            &bind, &first, &result), SALTS_OK);
        check_true(result.ack_valid);
        check_false(result.durable_or_installed);
        check_equal(result.ack.snapshot.next_offset, UINT64_C(3));
        check_equal(sink.fsynced, 0U);
        check_equal(tr_raft_multicore_chunk_receivers_handle(
            &bind, &last, &result), SALTS_OK);
        check_true(result.ack_valid && result.durable_or_installed);
        check_equal(result.ack.snapshot.next_offset, UINT64_C(6));
        check_equal(sink.fsynced, 1U);
        check_equal(sink.wrote, 2U);
        check_equal(fseek(sink.file, 0L, SEEK_SET), 0);
        check_equal(fread(saved, 1U, sizeof(saved), sink.file), sizeof(saved));
        check_equal(memcmp(saved, "abcdef", sizeof(saved)), 0);
        tr_raft_snapshot_receiver_destroy(receiver);
        check_equal(sink.aborted, 0U);
        check_equal(fclose(sink.file), 0);
    }

    it("DATA ACK durable flips only after verified final chunk and sink fsync")
    {
        synced_sink sink = {0};
        tr_raft_data_stream_receiver_t *receiver = NULL;
        tr_raft_data_stream_receiver_config_t config;
        tr_raft_multicore_chunk_receivers_t bind = {0};
        tr_raft_multicore_chunk_result_t result = {0};
        tr_raft_transport_payload_t first, last;

        sink.file = tmpfile();
        check_not_null(sink.file);
        config = data_config(&sink);
        check_equal(tr_raft_data_stream_receiver_create(&config, &receiver),
                    SALTS_OK);
        bind.data = receiver;
        first = data_chunk((const uint8_t *)"abc", 0U, 3U, false);
        last = data_chunk((const uint8_t *)"def", 3U, 3U, true);
        check_equal(tr_raft_multicore_chunk_receivers_handle(
            &bind, &first, &result), SALTS_OK);
        check_true(result.ack_valid && result.ack.data.accepted);
        check_false(result.durable_or_installed);
        check_false(result.ack.data.durable);
        check_equal(result.ack.data.next_offset, UINT64_C(3));
        check_equal(tr_raft_multicore_chunk_receivers_handle(
            &bind, &last, &result), SALTS_OK);
        check_true(result.ack_valid && result.durable_or_installed);
        check_true(result.ack.data.accepted && result.ack.data.durable);
        check_equal(result.ack.data.next_offset, UINT64_C(6));
        check_equal(sink.fsynced, 1U);
        check_equal(sink.wrote, 2U);
        tr_raft_data_stream_receiver_destroy(receiver);
        check_equal(sink.aborted, 0U);
        check_equal(fclose(sink.file), 0);
    }

    it("does not invent a positive ACK on commit error or invalid route")
    {
        synced_sink sink = {0};
        tr_raft_data_stream_receiver_t *receiver = NULL;
        tr_raft_data_stream_receiver_config_t config;
        tr_raft_multicore_chunk_receivers_t bind = {0};
        tr_raft_multicore_chunk_result_t result = {0};
        tr_raft_transport_payload_t invalid, final;

        sink.file = tmpfile();
        check_not_null(sink.file);
        sink.fail_commit = true;
        config = data_config(&sink);
        check_equal(tr_raft_data_stream_receiver_create(&config, &receiver),
                    SALTS_OK);
        bind.data = receiver;
        final = data_chunk((const uint8_t *)"abcdef", 0U, 6U, true);
        check_equal(tr_raft_multicore_chunk_receivers_handle(
            &bind, &final, &result), SALTS_EIO);
        check_false(result.ack_valid);
        check_false(result.durable_or_installed);
        check_equal(sink.fsynced, 0U);
        check_equal(sink.aborted, 1U);
        invalid = snapshot_chunk((const uint8_t *)"abc", 0U, 3U, false);
        check_equal(tr_raft_multicore_chunk_receivers_handle(
            &bind, &invalid, &result), SALTS_ENOTSUP);
        check_false(result.ack_valid);
        tr_raft_data_stream_receiver_destroy(receiver);
        check_equal(fclose(sink.file), 0);
    }
}
