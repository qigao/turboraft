#include "raft_snapshot_peer.h"

#include <turboraft/raft_snapshot_receiver.h>

#include <tinytest.h>
#include <turbo_error.h>

#include <string.h>

static const tr_raft_conf_t snapshot_peer_configuration = {
    TR_RAFT_CONF_FINAL, 0U, 1U,
    {{2U, TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER}}
};

typedef struct snapshot_payload_capture {
    tr_raft_coronet_payload_t payloads[2];
    size_t count;
} snapshot_payload_capture_t;

typedef struct snapshot_peer_install_capture {
    uint8_t data[600];
    size_t count;
} snapshot_peer_install_capture_t;

static int snapshot_payload_enqueue(
    void *context,
    const tr_raft_coronet_payload_t *payload)
{
    snapshot_payload_capture_t *capture =
        (snapshot_payload_capture_t *) context;

    if (capture == NULL || payload == NULL ||
        capture->count >= sizeof(capture->payloads) /
                              sizeof(capture->payloads[0])) {
        return TURBO_ENOSPC;
    }
    capture->payloads[capture->count++] = *payload;
    return TURBO_OK;
}

static int snapshot_peer_install(
    void *context,
    tr_raft_term_t leader_term,
    tr_raft_index_t snapshot_index,
    tr_raft_term_t snapshot_term,
    const tr_raft_conf_t *configuration,
    const uint8_t *data,
    size_t size)
{
    snapshot_peer_install_capture_t *capture =
        (snapshot_peer_install_capture_t *) context;

    if (capture == NULL || leader_term != 7U || snapshot_index != 9U ||
        snapshot_term != 6U || data == NULL || size != sizeof(capture->data)) {
        return TURBO_EPROTO;
    }
    memcpy(capture->data, data, size);
    ++capture->count;
    return TURBO_OK;
}

spec("raft snapshot peer")
{
    it("routes tagged chunks and acknowledgements")
    {
        tr_raft_snapshot_peer_config_t peer_config;
        tr_raft_snapshot_receiver_config_t receiver_config;
        tr_raft_snapshot_peer_t *peer = NULL;
        tr_raft_snapshot_receiver_t *receiver = NULL;
        tr_raft_snapshot_receive_result_t receive_result;
        tr_raft_snapshot_sender_status_t status;
        tr_raft_coronet_payload_t ack_payload;
        snapshot_payload_capture_t payloads;
        snapshot_peer_install_capture_t installed;
        uint8_t snapshot[600];
        size_t index;

        memset(&peer_config, 0, sizeof(peer_config));
        memset(&receiver_config, 0, sizeof(receiver_config));
        memset(&payloads, 0, sizeof(payloads));
        memset(&installed, 0, sizeof(installed));
        for (index = 0U; index < sizeof(snapshot); ++index) {
            snapshot[index] = (uint8_t) (index * 29U);
        }

        peer_config.self_id = 1U;
        peer_config.peer_id = 2U;
        peer_config.max_snapshot_bytes = 1024U;
        peer_config.enqueue = snapshot_payload_enqueue;
        peer_config.enqueue_context = &payloads;
        receiver_config.self_id = 2U;
        receiver_config.max_snapshot_bytes = 1024U;
        receiver_config.install = snapshot_peer_install;
        receiver_config.install_context = &installed;

        check_int_eq(tr_raft_snapshot_peer_create(&peer_config, &peer),
                     TURBO_OK);
        check_int_eq(tr_raft_snapshot_receiver_create(
                         &receiver_config, &receiver), TURBO_OK);
        check_int_eq(tr_raft_snapshot_peer_begin(
                         peer, 7U, 9U, 6U, &snapshot_peer_configuration,
                         snapshot, sizeof(snapshot)),
                     TURBO_OK);
        check_size_eq(payloads.count, 1U);
        check_int_eq(payloads.payloads[0].kind,
                     TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK);
        check_long_eq(payloads.payloads[0].data.snapshot_chunk.snapshot_offset,
                      0U);

        check_int_eq(tr_raft_snapshot_receiver_handle(
                         receiver,
                         &payloads.payloads[0].data.snapshot_chunk,
                         &receive_result), TURBO_OK);
        memset(&ack_payload, 0, sizeof(ack_payload));
        ack_payload.kind = TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_ACK;
        ack_payload.data.snapshot_ack = receive_result.ack;
        check_int_eq(tr_raft_snapshot_peer_handle_payload(peer, &ack_payload),
                     TURBO_OK);
        check_size_eq(payloads.count, 2U);
        check_long_eq(payloads.payloads[1].data.snapshot_chunk.snapshot_offset,
                      512U);

        check_int_eq(tr_raft_snapshot_receiver_handle(
                         receiver,
                         &payloads.payloads[1].data.snapshot_chunk,
                         &receive_result), TURBO_OK);
        ack_payload.data.snapshot_ack = receive_result.ack;
        check_int_eq(tr_raft_snapshot_peer_handle_payload(peer, &ack_payload),
                     TURBO_OK);
        check_int_eq(tr_raft_snapshot_peer_get_status(peer, &status), TURBO_OK);
        check(status.complete);
        check_size_eq(installed.count, 1U);
        check_mem_eq(installed.data, snapshot, sizeof(snapshot));

        tr_raft_snapshot_receiver_destroy(receiver);
        tr_raft_snapshot_peer_destroy(peer);
    }
}
