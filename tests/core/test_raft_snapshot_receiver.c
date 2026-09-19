#include <turboraft/raft_snapshot_receiver.h>

#include <tinytest.h>
#include <salts_error.h>

#include <string.h>

typedef struct snapshot_install_capture {
    int calls;
    tr_raft_term_t leader_term;
    tr_raft_index_t snapshot_index;
    tr_raft_term_t snapshot_term;
    uint8_t data[16];
    size_t size;
    tr_raft_conf_t configuration;
} snapshot_install_capture_t;

typedef struct snapshot_stream_capture {
    uint8_t data[16];
    size_t size;
    int begun;
    int committed;
    int aborted;
} snapshot_stream_capture_t;

static int snapshot_stream_begin(void *context, tr_raft_term_t leader_term,
                                 tr_raft_index_t snapshot_index,
                                 tr_raft_term_t snapshot_term,
                                 const tr_raft_conf_t *configuration,
                                 uint64_t snapshot_size)
{
    snapshot_stream_capture_t *capture = context;

    if (capture == NULL || leader_term != 5U || snapshot_index != 9U ||
        snapshot_term != 4U || configuration == NULL || snapshot_size != 6U) {
        return SALTS_EPROTO;
    }
    capture->begun = 1;
    return SALTS_OK;
}

static int snapshot_stream_write(void *context, uint64_t offset,
                                 const uint8_t *data, size_t size)
{
    snapshot_stream_capture_t *capture = context;

    if (capture == NULL || data == NULL || offset != capture->size ||
        size > sizeof(capture->data) - capture->size) {
        return SALTS_EPROTO;
    }
    memcpy(capture->data + capture->size, data, size);
    capture->size += size;
    return SALTS_OK;
}

static int snapshot_stream_commit(void *context)
{
    snapshot_stream_capture_t *capture = context;

    if (capture == NULL || capture->size != 6U) {
        return SALTS_EPROTO;
    }
    capture->committed = 1;
    return SALTS_OK;
}

static void snapshot_stream_abort(void *context)
{
    snapshot_stream_capture_t *capture = context;

    if (capture != NULL) {
        capture->aborted = 1;
    }
}


typedef struct large_snapshot_stream_capture {
    uint64_t expected_size;
    int begun;
    int aborted;
} large_snapshot_stream_capture_t;

static int large_snapshot_stream_begin(
    void *context,
    tr_raft_term_t leader_term,
    tr_raft_index_t snapshot_index,
    tr_raft_term_t snapshot_term,
    const tr_raft_conf_t *configuration,
    uint64_t snapshot_size)
{
    large_snapshot_stream_capture_t *capture =
        (large_snapshot_stream_capture_t *)context;

    if (capture == NULL || leader_term == 0U || snapshot_index == 0U ||
        snapshot_term == 0U || configuration == NULL ||
        snapshot_size != capture->expected_size) {
        return SALTS_EPROTO;
    }
    capture->begun = 1;
    return SALTS_OK;
}

static int large_snapshot_stream_write(
    void *context,
    uint64_t offset,
    const uint8_t *data,
    size_t size)
{
    (void)context;
    return offset == 0U && data != NULL && size == 1U
               ? SALTS_OK
               : SALTS_EPROTO;
}

static int large_snapshot_stream_commit(void *context)
{
    (void)context;
    return SALTS_OK;
}

static void large_snapshot_stream_abort(void *context)
{
    large_snapshot_stream_capture_t *capture =
        (large_snapshot_stream_capture_t *)context;

    if (capture != NULL) {
        capture->aborted = 1;
    }
}

static int snapshot_capture_install(void *context,
                                    tr_raft_term_t leader_term,
                                    tr_raft_index_t snapshot_index,
                                    tr_raft_term_t snapshot_term,
                                    const tr_raft_conf_t *configuration,
                                    const uint8_t *data,
                                    size_t size)
{
    snapshot_install_capture_t *capture =
        (snapshot_install_capture_t *) context;

    if (configuration == NULL || size > sizeof(capture->data)) {
        return SALTS_ERANGE;
    }
    capture->calls++;
    capture->leader_term = leader_term;
    capture->snapshot_index = snapshot_index;
    capture->snapshot_term = snapshot_term;
    capture->size = size;
    capture->configuration = *configuration;
    if (size != 0U) {
        memcpy(capture->data, data, size);
    }
    return SALTS_OK;
}

static tr_raft_snapshot_chunk_t snapshot_final_chunk(void)
{
    static const uint8_t abc_sha256[TR_RAFT_WIRE_SNAPSHOT_DIGEST_SIZE] = {
        0xbaU, 0x78U, 0x16U, 0xbfU, 0x8fU, 0x01U, 0xcfU, 0xeaU,
        0x41U, 0x41U, 0x40U, 0xdeU, 0x5dU, 0xaeU, 0x22U, 0x23U,
        0xb0U, 0x03U, 0x61U, 0xa3U, 0x96U, 0x17U, 0x7aU, 0x9cU,
        0xb4U, 0x10U, 0xffU, 0x61U, 0xf2U, 0x00U, 0x15U, 0xadU
    };
    tr_raft_snapshot_chunk_t chunk;

    memset(&chunk, 0, sizeof(chunk));
    chunk.from = 1U;
    chunk.to = 2U;
    chunk.term = 5U;
    chunk.snapshot_index = 9U;
    chunk.snapshot_term = 4U;
    chunk.snapshot_size = 3U;
    chunk.has_configuration = true;
    chunk.configuration.phase = TR_RAFT_CONF_FINAL;
    chunk.configuration.member_count = 1U;
    chunk.configuration.members[0].node_id = 2U;
    chunk.configuration.members[0].roles =
        TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER;
    chunk.data_length = 3U;
    chunk.data = (const uint8_t *)"abc";
    chunk.done = true;
    memcpy(chunk.snapshot_digest, abc_sha256, sizeof(abc_sha256));
    return chunk;
}

spec("raft snapshot receiver")
{
    it("verifies and installs a complete snapshot")
    {
        snapshot_install_capture_t capture;
        tr_raft_snapshot_receiver_config_t config;
        tr_raft_snapshot_receiver_t *receiver = NULL;
        tr_raft_snapshot_receive_result_t result;
        tr_raft_snapshot_chunk_t chunk = snapshot_final_chunk();

        memset(&capture, 0, sizeof(capture));
        memset(&config, 0, sizeof(config));
        config.self_id = 2U;
        config.max_snapshot_bytes = 1024U;
        config.install = snapshot_capture_install;
        config.install_context = &capture;
        check_equal(tr_raft_snapshot_receiver_create(&config, &receiver),
                     SALTS_OK);
        check_equal(tr_raft_snapshot_receiver_handle(receiver, &chunk,
                                                       &result),
                     SALTS_OK);
        check(result.installed);
        check(result.ack.accepted);
        check_equal(result.ack.next_offset, 3U);
        check_equal(capture.calls, 1);
        check_equal(capture.leader_term, 5U);
        check_equal(capture.snapshot_index, 9U);
        check_equal(capture.configuration.member_count, 1U);
        check_equal(capture.data, "abc", 3U);
        check_equal(tr_raft_snapshot_receiver_handle(receiver, &chunk,
                                                       &result),
                     SALTS_OK);
        check(!result.installed);
        check(result.ack.accepted);
        check_equal(result.ack.next_offset, 3U);
        check_equal(capture.calls, 1);
        tr_raft_snapshot_receiver_destroy(receiver);
    }

    it("rejects a digest mismatch without installation")
    {
        snapshot_install_capture_t capture;
        tr_raft_snapshot_receiver_config_t config;
        tr_raft_snapshot_receiver_t *receiver = NULL;
        tr_raft_snapshot_receive_result_t result;
        tr_raft_snapshot_chunk_t chunk = snapshot_final_chunk();

        memset(&capture, 0, sizeof(capture));
        memset(&config, 0, sizeof(config));
        config.self_id = 2U;
        config.max_snapshot_bytes = 1024U;
        config.install = snapshot_capture_install;
        config.install_context = &capture;
        chunk.snapshot_digest[0] ^= 0xffU;
        check_equal(tr_raft_snapshot_receiver_create(&config, &receiver),
                     SALTS_OK);
        check_equal(tr_raft_snapshot_receiver_handle(receiver, &chunk,
                                                       &result),
                     SALTS_EPROTO);
        check(!result.installed);
        check(!result.ack.accepted);
        check_equal(capture.calls, 0);
        tr_raft_snapshot_receiver_destroy(receiver);
    }

    it("streams chunks through incremental digest verification")
    {
        static const uint8_t digest[TR_RAFT_WIRE_SNAPSHOT_DIGEST_SIZE] = {
            0xbeU, 0xf5U, 0x7eU, 0xc7U, 0xf5U, 0x3aU, 0x6dU, 0x40U,
            0xbeU, 0xb6U, 0x40U, 0xa7U, 0x80U, 0xa6U, 0x39U, 0xc8U,
            0x3bU, 0xc2U, 0x9aU, 0xc8U, 0xa9U, 0x81U, 0x6fU, 0x1fU,
            0xc6U, 0xc5U, 0xc6U, 0xdcU, 0xd9U, 0x3cU, 0x47U, 0x21U};
        tr_raft_snapshot_receiver_config_t config;
        tr_raft_snapshot_receiver_t *receiver = NULL;
        tr_raft_snapshot_receive_result_t result;
        tr_raft_snapshot_chunk_t chunk;
        snapshot_stream_capture_t capture;

        memset(&config, 0, sizeof(config));
        memset(&capture, 0, sizeof(capture));
        config.self_id = 2U;
        config.max_snapshot_bytes = 1024U;
        config.stream.begin = snapshot_stream_begin;
        config.stream.write = snapshot_stream_write;
        config.stream.commit = snapshot_stream_commit;
        config.stream.abort = snapshot_stream_abort;
        config.stream.context = &capture;
        check_equal(tr_raft_snapshot_receiver_create(&config, &receiver),
                     SALTS_OK);

        memset(&chunk, 0, sizeof(chunk));
        chunk.from = 1U;
        chunk.to = 2U;
        chunk.term = 5U;
        chunk.snapshot_index = 9U;
        chunk.snapshot_term = 4U;
        chunk.snapshot_size = 6U;
        chunk.has_configuration = true;
        chunk.configuration.phase = TR_RAFT_CONF_FINAL;
        chunk.configuration.member_count = 1U;
        chunk.configuration.members[0].node_id = 2U;
        chunk.configuration.members[0].roles =
            TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER;
        memcpy(chunk.snapshot_digest, digest, sizeof(digest));
        chunk.data = (const uint8_t *)"abc";
        chunk.data_length = 3U;
        check_equal(tr_raft_snapshot_receiver_handle(receiver, &chunk,
                                                       &result), SALTS_OK);
        check(!result.installed);
        chunk.snapshot_offset = 3U;
        chunk.has_configuration = false;
        chunk.data = (const uint8_t *)"def";
        chunk.done = true;
        check_equal(tr_raft_snapshot_receiver_handle(receiver, &chunk,
                                                       &result), SALTS_OK);
        check(result.installed);
        check(capture.begun);
        check(capture.committed);
        check(!capture.aborted);
        check_equal(capture.data, "abcdef", 6U);
        tr_raft_snapshot_receiver_destroy(receiver);
    }
    it("allows database-scale totals on the streaming sink path")
    {
        const uint64_t large_size = UINT64_C(512) * 1024U * 1024U;
        static const uint8_t one = 0x5aU;
        tr_raft_snapshot_receiver_config_t config;
        tr_raft_snapshot_receiver_t *receiver = NULL;
        tr_raft_snapshot_receive_result_t result;
        tr_raft_snapshot_chunk_t chunk;
        large_snapshot_stream_capture_t capture;

        memset(&config, 0, sizeof(config));
        memset(&chunk, 0, sizeof(chunk));
        memset(&capture, 0, sizeof(capture));
        capture.expected_size = large_size;

        config.self_id = 2U;
        config.max_snapshot_bytes = large_size;
        config.stream.begin = large_snapshot_stream_begin;
        config.stream.write = large_snapshot_stream_write;
        config.stream.commit = large_snapshot_stream_commit;
        config.stream.abort = large_snapshot_stream_abort;
        config.stream.context = &capture;

        check_equal(tr_raft_snapshot_receiver_create(&config, &receiver),
                    SALTS_OK);

        chunk.from = 1U;
        chunk.to = 2U;
        chunk.term = 5U;
        chunk.snapshot_index = 9U;
        chunk.snapshot_term = 4U;
        chunk.snapshot_size = large_size;
        chunk.has_configuration = true;
        chunk.configuration.phase = TR_RAFT_CONF_FINAL;
        chunk.configuration.member_count = 1U;
        chunk.configuration.members[0].node_id = 2U;
        chunk.configuration.members[0].roles =
            TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER;
        chunk.data = &one;
        chunk.data_length = 1U;

        check_equal(tr_raft_snapshot_receiver_handle(
                        receiver, &chunk, &result),
                    SALTS_OK);
        check(capture.begun);
        check(!capture.aborted);
        check_equal(result.ack.next_offset, 1U);

        tr_raft_snapshot_receiver_destroy(receiver);
        check(capture.aborted);
    }

    it("bounds the compatibility whole-buffer install path separately")
    {
        tr_raft_snapshot_receiver_config_t config;
        tr_raft_snapshot_receiver_t *receiver = NULL;
        tr_raft_snapshot_receive_result_t result;
        tr_raft_snapshot_chunk_t chunk = snapshot_final_chunk();
        snapshot_install_capture_t capture;

        memset(&config, 0, sizeof(config));
        memset(&capture, 0, sizeof(capture));
        config.self_id = 2U;
        config.max_snapshot_bytes = 1024U;
        config.max_buffered_snapshot_bytes = 2U;
        config.install = snapshot_capture_install;
        config.install_context = &capture;

        check_equal(tr_raft_snapshot_receiver_create(&config, &receiver),
                    SALTS_OK);
        check_equal(tr_raft_snapshot_receiver_handle(
                        receiver, &chunk, &result),
                    SALTS_EFBIG);
        check_equal(capture.calls, 0);

        tr_raft_snapshot_receiver_destroy(receiver);
    }

}
