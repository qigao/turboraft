#include <turboraft/raft_snapshot_receiver.h>

#include <tinytest.h>
#include <turbo_error.h>

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
        return TURBO_ERANGE;
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
    return TURBO_OK;
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
    chunk.done = true;
    memcpy(chunk.snapshot_digest, abc_sha256, sizeof(abc_sha256));
    memcpy(chunk.data, "abc", 3U);
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
        check_int_eq(tr_raft_snapshot_receiver_create(&config, &receiver),
                     TURBO_OK);
        check_int_eq(tr_raft_snapshot_receiver_handle(receiver, &chunk,
                                                       &result),
                     TURBO_OK);
        check(result.installed);
        check(result.ack.accepted);
        check_long_eq(result.ack.next_offset, 3U);
        check_int_eq(capture.calls, 1);
        check_long_eq(capture.leader_term, 5U);
        check_long_eq(capture.snapshot_index, 9U);
        check_size_eq(capture.configuration.member_count, 1U);
        check_mem_eq(capture.data, "abc", 3U);
        check_int_eq(tr_raft_snapshot_receiver_handle(receiver, &chunk,
                                                       &result),
                     TURBO_OK);
        check(!result.installed);
        check(result.ack.accepted);
        check_long_eq(result.ack.next_offset, 3U);
        check_int_eq(capture.calls, 1);
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
        check_int_eq(tr_raft_snapshot_receiver_create(&config, &receiver),
                     TURBO_OK);
        check_int_eq(tr_raft_snapshot_receiver_handle(receiver, &chunk,
                                                       &result),
                     TURBO_EPROTO);
        check(!result.installed);
        check(!result.ack.accepted);
        check_int_eq(capture.calls, 0);
        tr_raft_snapshot_receiver_destroy(receiver);
    }
}
