#include <turboraft/raft_transport.h>

#include <salts_error.h>
#include <tinytest.h>

#include <string.h>

typedef struct transport_capture {
    tr_raft_transport_payload_t payload;
    size_t payload_count;
} transport_capture_t;

static int capture_payload(void *context,
                           const tr_raft_transport_payload_t *payload)
{
    transport_capture_t *capture = (transport_capture_t *)context;

    capture->payload = *payload;
    ++capture->payload_count;
    return SALTS_OK;
}

static int reject_unknown_group(
    void *context,
    const tr_raft_transport_payload_t *payload)
{
    transport_capture_t *capture = (transport_capture_t *)context;

    if (payload->group_id == 999U) {
        return SALTS_ENOENT;
    }
    if (payload->group_id == 998U) {
        return SALTS_ESHUTDOWN;
    }
    capture->payload = *payload;
    ++capture->payload_count;
    return SALTS_OK;
}


static tr_raft_transport_session_t *make_session(
    tr_raft_node_id_t local_node_id,
    tr_raft_node_id_t peer_node_id,
    transport_capture_t *capture)
{
    tr_raft_transport_session_config_t config;
    tr_raft_handshake_result_t handshake;
    tr_raft_transport_session_t *session = NULL;
    size_t index;

    memset(&config, 0, sizeof(config));
    memset(&handshake, 0, sizeof(handshake));
    for (index = 0U; index < sizeof(config.cluster_id.bytes); ++index) {
        config.cluster_id.bytes[index] = (uint8_t)(41U + index);
    }
    config.local_node_id = local_node_id;
    config.peer_node_id = peer_node_id;
    config.first_outbound_message_id = 1U;
    handshake.complete = 1;
    handshake.cluster_id = config.cluster_id;
    handshake.local_node_id = local_node_id;
    handshake.peer_node_id = peer_node_id;
    handshake.peer_process_incarnation.bytes[0] = 1U;
    handshake.feature_bits = 0U;
    handshake.wire_major = TR_RAFT_HANDSHAKE_WIRE_MAJOR;
    handshake.wire_minor = TR_RAFT_HANDSHAKE_WIRE_MINOR;
    handshake.max_frame_size = TR_RAFT_WIRE_MAX_FRAME_SIZE;
    handshake.max_snapshot_chunk_size =
        TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES;
    config.handshake = &handshake;
    config.on_payload = capture_payload;
    config.payload_context = capture;
    check_equal(tr_raft_transport_session_create(&config, &session),
                SALTS_OK);
    return session;
}

static tr_raft_message_t make_heartbeat(tr_raft_node_id_t from,
                                        tr_raft_node_id_t to)
{
    tr_raft_message_t message;

    memset(&message, 0, sizeof(message));
    message.type = TR_RAFT_MSG_HEARTBEAT_REQUEST;
    message.from = from;
    message.to = to;
    message.term = 7U;
    return message;
}

spec("Raft transport framing")
{
    it("encodes and incrementally decodes a grouped Raft message")
    {
        transport_capture_t sender_capture;
        transport_capture_t receiver_capture;
        tr_raft_transport_session_t *sender;
        tr_raft_transport_session_t *receiver;
        tr_raft_transport_status_t sender_status;
        tr_raft_transport_status_t receiver_status;
        tr_raft_message_t message = make_heartbeat(1U, 2U);
        uint8_t packet[TR_RAFT_TRANSPORT_MAX_PACKET_SIZE];
        size_t packet_size = 0U;

        memset(&sender_capture, 0, sizeof(sender_capture));
        memset(&receiver_capture, 0, sizeof(receiver_capture));
        sender = make_session(1U, 2U, &sender_capture);
        receiver = make_session(2U, 1U, &receiver_capture);

        check_equal(tr_raft_transport_encode(sender, 100U, &message, packet,
                                             sizeof(packet), &packet_size),
                    SALTS_OK);
        check_equal(tr_raft_transport_feed(receiver, packet, 3U), SALTS_OK);
        check_equal(receiver_capture.payload_count, 0U);
        check_equal(tr_raft_transport_feed(receiver, packet + 3U,
                                            packet_size - 3U),
                    SALTS_OK);
        check_equal(receiver_capture.payload_count, 1U);
        check_equal(receiver_capture.payload.group_id, 100U);
        check_equal(receiver_capture.payload.kind, TR_RAFT_WIRE_PAYLOAD_RAFT);
        check_equal(receiver_capture.payload.data.raft.term, 7U);

        check_equal(tr_raft_transport_get_status(sender, &sender_status),
                    SALTS_OK);
        check_equal(tr_raft_transport_get_status(receiver, &receiver_status),
                    SALTS_OK);
        check_equal(sender_status.frames_encoded, 1U);
        check_equal(sender_status.bytes_encoded, packet_size);
        check_equal(receiver_status.frames_decoded, 1U);
        check_equal(receiver_status.bytes_decoded, packet_size);

        check_equal(tr_raft_transport_session_destroy(receiver), SALTS_OK);
        check_equal(tr_raft_transport_session_destroy(sender), SALTS_OK);
    }

    it("dispatches grouped snapshot payloads and rejects replayed frames")
    {
        transport_capture_t sender_capture;
        transport_capture_t receiver_capture;
        tr_raft_transport_session_t *sender;
        tr_raft_transport_session_t *receiver;
        tr_raft_transport_payload_t payload;
        tr_raft_transport_status_t status;
        uint8_t packet[TR_RAFT_TRANSPORT_MAX_PACKET_SIZE];
        size_t packet_size = 0U;

        memset(&sender_capture, 0, sizeof(sender_capture));
        memset(&receiver_capture, 0, sizeof(receiver_capture));
        memset(&payload, 0, sizeof(payload));
        sender = make_session(1U, 2U, &sender_capture);
        receiver = make_session(2U, 1U, &receiver_capture);
        payload.group_id = 101U;
        payload.kind = TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_ACK;
        payload.data.snapshot_ack.from = 1U;
        payload.data.snapshot_ack.to = 2U;
        payload.data.snapshot_ack.term = 7U;
        payload.data.snapshot_ack.snapshot_index = 23U;
        payload.data.snapshot_ack.snapshot_size = 1024U;
        payload.data.snapshot_ack.next_offset = 1024U;
        payload.data.snapshot_ack.accepted = true;

        check_equal(tr_raft_transport_encode_payload(
                        sender, &payload, packet, sizeof(packet), &packet_size),
                    SALTS_OK);
        check_equal(tr_raft_transport_feed(receiver, packet, packet_size),
                    SALTS_OK);
        check_equal(receiver_capture.payload_count, 1U);
        check_equal(receiver_capture.payload.group_id, 101U);
        check_equal(receiver_capture.payload.data.snapshot_ack.snapshot_index,
                    23U);
        check_equal(tr_raft_transport_feed(receiver, packet, packet_size),
                    SALTS_EPROTO);
        check_equal(tr_raft_transport_get_status(receiver, &status), SALTS_OK);
        check_equal(status.state, TR_RAFT_TRANSPORT_STATE_FAULTED);

        check_equal(tr_raft_transport_session_destroy(receiver), SALTS_OK);
        check_equal(tr_raft_transport_session_destroy(sender), SALTS_OK);
    }
    it("keeps the peer session alive across group routing rejections")
    {
        transport_capture_t sender_capture;
        transport_capture_t receiver_capture;
        tr_raft_transport_session_t *sender;
        tr_raft_transport_session_t *receiver;
        tr_raft_transport_session_config_t receiver_config;
        tr_raft_handshake_result_t handshake;
        tr_raft_transport_status_t status;
        tr_raft_message_t message = make_heartbeat(1U, 2U);
        uint8_t packet[TR_RAFT_TRANSPORT_MAX_PACKET_SIZE];
        size_t packet_size = 0U;
        size_t index;

        memset(&sender_capture, 0, sizeof(sender_capture));
        memset(&receiver_capture, 0, sizeof(receiver_capture));
        memset(&receiver_config, 0, sizeof(receiver_config));
        memset(&handshake, 0, sizeof(handshake));

        sender = make_session(1U, 2U, &sender_capture);
        for (index = 0U; index < sizeof(receiver_config.cluster_id.bytes);
             ++index) {
            receiver_config.cluster_id.bytes[index] =
                (uint8_t)(41U + index);
        }
        receiver_config.local_node_id = 2U;
        receiver_config.peer_node_id = 1U;
        receiver_config.first_outbound_message_id = 1U;
        handshake.complete = 1;
        handshake.cluster_id = receiver_config.cluster_id;
        handshake.local_node_id = 2U;
        handshake.peer_node_id = 1U;
        handshake.peer_process_incarnation.bytes[0] = 1U;
        handshake.feature_bits = 0U;
        handshake.wire_major = TR_RAFT_HANDSHAKE_WIRE_MAJOR;
        handshake.wire_minor = TR_RAFT_HANDSHAKE_WIRE_MINOR;
        handshake.max_frame_size = TR_RAFT_WIRE_MAX_FRAME_SIZE;
        handshake.max_snapshot_chunk_size =
            TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES;
        receiver_config.handshake = &handshake;
        receiver_config.on_payload = reject_unknown_group;
        receiver_config.payload_context = &receiver_capture;
        check_equal(tr_raft_transport_session_create(
                        &receiver_config, &receiver),
                    SALTS_OK);

        check_equal(tr_raft_transport_encode(sender, 999U, &message, packet,
                                             sizeof(packet), &packet_size),
                    SALTS_OK);
        check_equal(tr_raft_transport_feed(receiver, packet, packet_size),
                    SALTS_OK);
        check_equal(tr_raft_transport_get_status(receiver, &status), SALTS_OK);
        check_equal(status.state, TR_RAFT_TRANSPORT_STATE_READY);
        check_equal(status.group_routing_rejections, 1U);
        check_equal(status.last_rejected_group_id, 999U);
        check_equal(status.last_group_routing_error, SALTS_ENOENT);

        check_equal(tr_raft_transport_encode(sender, 998U, &message, packet,
                                             sizeof(packet), &packet_size),
                    SALTS_OK);
        check_equal(tr_raft_transport_feed(receiver, packet, packet_size),
                    SALTS_OK);
        check_equal(tr_raft_transport_get_status(receiver, &status), SALTS_OK);
        check_equal(status.state, TR_RAFT_TRANSPORT_STATE_READY);
        check_equal(status.group_routing_rejections, 2U);
        check_equal(status.last_rejected_group_id, 998U);
        check_equal(status.last_group_routing_error, SALTS_ESHUTDOWN);

        check_equal(tr_raft_transport_encode(sender, 100U, &message, packet,
                                             sizeof(packet), &packet_size),
                    SALTS_OK);
        check_equal(tr_raft_transport_feed(receiver, packet, packet_size),
                    SALTS_OK);
        check_equal(receiver_capture.payload_count, 1U);
        check_equal(receiver_capture.payload.group_id, 100U);
        check_equal(tr_raft_transport_get_status(receiver, &status), SALTS_OK);
        check_equal(status.state, TR_RAFT_TRANSPORT_STATE_READY);

        check_equal(tr_raft_transport_session_destroy(receiver), SALTS_OK);
        check_equal(tr_raft_transport_session_destroy(sender), SALTS_OK);
    }

}
