#include <turboraft/raft_coronet_transport.h>

#include "../../src/transport/raft_coronet_transport_internal.h"

#include <tinytest.h>
#include <turbo_error.h>

#include <string.h>

static int discard_message(void *context, const tr_raft_message_t *message)
{
    (void) context;
    (void) message;
    return TURBO_OK;
}

static void fill_cluster(tr_raft_cluster_id_t *cluster, uint8_t seed)
{
    size_t index;

    for (index = 0U; index < sizeof(cluster->bytes); ++index) {
        cluster->bytes[index] = (uint8_t) (seed + index);
    }
}

static tr_raft_handshake_result_t make_result(uint64_t features)
{
    tr_raft_handshake_result_t result;

    memset(&result, 0, sizeof(result));
    result.complete = 1;
    fill_cluster(&result.cluster_id, 10U);
    result.local_node_id = 1U;
    result.peer_node_id = 2U;
    result.peer_process_incarnation.bytes[0] = 1U;
    result.feature_bits = features;
    result.wire_major = TR_RAFT_HANDSHAKE_WIRE_MAJOR;
    result.wire_minor = TR_RAFT_HANDSHAKE_WIRE_MINOR;
    result.max_frame_size = TR_RAFT_HANDSHAKE_MIN_FRAME_SIZE;
    result.max_snapshot_chunk_size =
        TR_RAFT_HANDSHAKE_MIN_SNAPSHOT_CHUNK_SIZE;
    return result;
}

static tr_raft_coronet_session_t *make_session(
    const tr_raft_handshake_result_t *handshake)
{
    tr_raft_coronet_session_config_t config;
    tr_raft_coronet_session_t *session = NULL;

    memset(&config, 0, sizeof(config));
    config.cluster_id = handshake->cluster_id;
    config.local_node_id = handshake->local_node_id;
    config.peer_node_id = handshake->peer_node_id;
    config.first_outbound_message_id = 1U;
    config.handshake = handshake;
    config.on_message = discard_message;
    check_equal(tr_raft_coronet_session_create(&config, &session), TURBO_OK);
    return session;
}

static tr_raft_message_t make_append(size_t entry_count)
{
    tr_raft_message_t message;
    size_t index;

    memset(&message, 0, sizeof(message));
    message.type = TR_RAFT_MSG_APPEND_REQUEST;
    message.from = 1U;
    message.to = 2U;
    message.term = 4U;
    message.previous_log_index = 7U;
    message.previous_log_term = 3U;
    message.leader_commit = 6U;
    message.entry_count = entry_count;
    for (index = 0U; index < entry_count; ++index) {
        message.entries[index].index = 8U + index;
        message.entries[index].term = 4U;
        message.entries[index].command_id = 50U + index;
    }
    return message;
}

spec("raft rolling wire upgrades")
{
    it("selects legacy v2 and current v3 capabilities")
    {
        tr_raft_handshake_result_t legacy = make_result(
            TR_RAFT_HANDSHAKE_FEATURE_SNAPSHOT_CONF_STATE);
        tr_raft_handshake_result_t current =
            make_result(TR_RAFT_HANDSHAKE_FEATURE_CURRENT);
        uint16_t version = 0U;
        uint32_t chunk_size = 0U;

        check_equal(tr_raft_handshake_select_raft_wire_version(
                         &legacy, 1U, &version),
                     TURBO_OK);
        check_equal(version, TR_RAFT_WIRE_MIN_VERSION);
        check_equal(tr_raft_handshake_select_raft_wire_version(
                         &legacy, 2U, &version),
                     TURBO_EPROTONOSUPPORT);
        check_equal(tr_raft_handshake_require_snapshot_v4(&legacy),
                     TURBO_EPROTONOSUPPORT);
        check_equal(tr_raft_handshake_select_raft_wire_version(
                         &current, TR_RAFT_MAX_APPEND_ENTRIES, &version),
                     TURBO_OK);
        check_equal(version, TR_RAFT_WIRE_VERSION);
        check_equal(tr_raft_handshake_require_snapshot_v4(&current),
                     TURBO_OK);
        check_equal(tr_raft_handshake_select_snapshot_wire_version(
                         &current, &version, &chunk_size), TURBO_OK);
        check_equal(version, TR_RAFT_WIRE_SNAPSHOT_LEGACY_VERSION);
        check_equal(chunk_size,
                      TR_RAFT_WIRE_LEGACY_SNAPSHOT_CHUNK_BYTES);
        current.max_frame_size = TR_RAFT_WIRE_MAX_FRAME_SIZE;
        current.max_snapshot_chunk_size =
            TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES;
        check_equal(tr_raft_handshake_select_snapshot_wire_version(
                         &current, &version, &chunk_size), TURBO_OK);
        check_equal(version, TR_RAFT_WIRE_SNAPSHOT_VERSION);
        check_equal(chunk_size, TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES);
    }

    it("enforces negotiated versions at the transport boundary")
    {
        tr_raft_handshake_result_t legacy = make_result(
            TR_RAFT_HANDSHAKE_FEATURE_SNAPSHOT_CONF_STATE);
        tr_raft_handshake_result_t current =
            make_result(TR_RAFT_HANDSHAKE_FEATURE_CURRENT);
        tr_raft_coronet_session_t *legacy_session = make_session(&legacy);
        tr_raft_coronet_session_t *current_session = make_session(&current);
        tr_raft_coronet_payload_t payload;
        uint8_t packet[TR_RAFT_CORONET_MAX_PACKET_SIZE];
        size_t packet_size = 0U;

        memset(&payload, 0, sizeof(payload));
        payload.kind = TR_RAFT_WIRE_PAYLOAD_RAFT;
        payload.data.raft = make_append(1U);
        check_equal(tr_raft_coronet_encode_payload_packet(
                         legacy_session, &payload, packet, sizeof(packet),
                         &packet_size),
                     TURBO_OK);
        check_equal(packet[9], TR_RAFT_WIRE_MIN_VERSION);
        check_equal(tr_raft_coronet_encode_payload_packet(
                         current_session, &payload, packet, sizeof(packet),
                         &packet_size),
                     TURBO_OK);
        check_equal(packet[9], TR_RAFT_WIRE_VERSION);

        payload.data.raft = make_append(2U);
        check_equal(tr_raft_coronet_encode_payload_packet(
                         legacy_session, &payload, packet, sizeof(packet),
                         &packet_size),
                     TURBO_EPROTONOSUPPORT);
        memset(&payload, 0, sizeof(payload));
        payload.kind = TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_ACK;
        payload.data.snapshot_ack.from = 1U;
        payload.data.snapshot_ack.to = 2U;
        check_equal(tr_raft_coronet_encode_payload_packet(
                         legacy_session, &payload, packet, sizeof(packet),
                         &packet_size),
                     TURBO_EPROTONOSUPPORT);

        tr_raft_coronet_session_destroy(current_session);
        tr_raft_coronet_session_destroy(legacy_session);
    }

    it("splits legacy append batches without breaking log continuity")
    {
        tr_raft_message_t batch = make_append(3U);
        tr_raft_message_t part;

        check_equal(tr_raft_coronet_v2_append_part(&batch, 0U, &part),
                     TURBO_OK);
        check_equal(part.previous_log_index, 7U);
        check_equal(part.previous_log_term, 3U);
        check_equal(part.entries[0].index, 8U);
        check_equal(tr_raft_coronet_v2_append_part(&batch, 1U, &part),
                     TURBO_OK);
        check_equal(part.previous_log_index, 8U);
        check_equal(part.previous_log_term, 4U);
        check_equal(part.entries[0].index, 9U);
        check_equal(tr_raft_coronet_v2_append_part(&batch, 2U, &part),
                     TURBO_OK);
        check_equal(part.previous_log_index, 9U);
        check_equal(part.entries[0].index, 10U);
    }

    it("rejects wire downgrades and keeps both snapshot frames on v4")
    {
        tr_raft_wire_codec_t *codec = NULL;
        tr_raft_wire_metadata_t metadata;
        tr_raft_wire_metadata_t decoded_metadata;
        tr_raft_message_t message = make_append(2U);
        tr_raft_message_t decoded;
        tr_raft_snapshot_ack_t ack;
        tr_raft_snapshot_ack_t decoded_ack;
        uint8_t frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];
        size_t frame_size = 0U;
        uint16_t version = 0U;

        memset(&metadata, 0, sizeof(metadata));
        fill_cluster(&metadata.cluster_id, 10U);
        metadata.message_id = 1U;
        check_equal(tr_raft_wire_codec_create(&codec), TURBO_OK);
        check_equal(tr_raft_wire_encode_version(
                         codec, TR_RAFT_WIRE_VERSION, &metadata, &message,
                         frame, sizeof(frame), &frame_size),
                     TURBO_OK);
        frame[5] = TR_RAFT_WIRE_MIN_VERSION;
        check_equal(tr_raft_wire_decode(codec, frame, frame_size,
                                         &decoded_metadata, &decoded),
                     TURBO_EPROTO);

        memset(&ack, 0, sizeof(ack));
        ack.from = 1U;
        ack.to = 2U;
        ack.term = 4U;
        ack.snapshot_index = 7U;
        ack.snapshot_size = 3U;
        ack.next_offset = 3U;
        ack.accepted = true;
        memset(ack.snapshot_digest, 1, sizeof(ack.snapshot_digest));
        check_equal(tr_raft_wire_encode_snapshot_ack(
                         codec, &metadata, &ack, frame, sizeof(frame),
                         &frame_size),
                     TURBO_OK);
        check_equal(tr_raft_wire_peek_version(frame, frame_size, &version),
                     TURBO_OK);
        check_equal(version, TR_RAFT_WIRE_SNAPSHOT_VERSION);
        frame[5] = TR_RAFT_WIRE_VERSION;
        check_equal(tr_raft_wire_decode_snapshot_ack(
                         codec, frame, frame_size, &decoded_metadata,
                         &decoded_ack),
                     TURBO_EPROTO);
        tr_raft_wire_codec_destroy(codec);
    }
}
