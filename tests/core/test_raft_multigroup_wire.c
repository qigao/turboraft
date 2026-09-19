#include <turboraft/raft_transport.h>

#include <salts_error.h>
#include <tinytest.h>

#include <string.h>

static tr_raft_message_t heartbeat(tr_raft_node_id_t from,
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

static tr_raft_handshake_result_t contract_for(
    tr_raft_node_id_t local_node_id,
    tr_raft_node_id_t peer_node_id,
    bool group_aware)
{
    tr_raft_handshake_result_t result;
    size_t index;

    memset(&result, 0, sizeof(result));
    result.complete = 1;
    for (index = 0U; index < sizeof(result.cluster_id.bytes); ++index) {
        result.cluster_id.bytes[index] = (uint8_t)(30U + index);
    }
    result.local_node_id = local_node_id;
    result.peer_node_id = peer_node_id;
    result.peer_process_incarnation.bytes[0] = 1U;
    result.feature_bits = TR_RAFT_HANDSHAKE_FEATURE_CURRENT;
    if (group_aware) {
        result.feature_bits |=
            TR_RAFT_HANDSHAKE_FEATURE_GROUP_MULTIPLEX_V1;
    }
    result.wire_major = TR_RAFT_HANDSHAKE_WIRE_MAJOR;
    result.wire_minor = TR_RAFT_HANDSHAKE_WIRE_MINOR;
    result.max_frame_size = group_aware
                                ? TR_RAFT_WIRE_MAX_FRAME_SIZE
                                : TR_RAFT_WIRE_LEGACY_MAX_FRAME_SIZE;
    result.max_snapshot_chunk_size =
        TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES;
    return result;
}

static int discard_message(void *context, const tr_raft_message_t *message)
{
    (void)context;
    (void)message;
    return SALTS_OK;
}

static int capture_group_payload(void *context,
                                 const tr_raft_transport_payload_t *payload)
{
    tr_raft_group_id_t *group_id = (tr_raft_group_id_t *)context;

    *group_id = payload->group_id;
    return SALTS_OK;
}

spec("raft multi-group wire contract")
{
    it("round trips group identity in the common envelope")
    {
        tr_raft_wire_codec_t *codec = NULL;
        tr_raft_wire_metadata_t metadata;
        tr_raft_wire_metadata_t decoded_metadata;
        tr_raft_message_t message = heartbeat(1U, 2U);
        tr_raft_message_t decoded;
        uint8_t frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];
        size_t frame_size = 0U;

        memset(&metadata, 0, sizeof(metadata));
        metadata.cluster_id.bytes[0] = 1U;
        metadata.group_id = 100U;
        metadata.message_id = 9U;

        check_equal(tr_raft_wire_codec_create(&codec), SALTS_OK);
        check_equal(tr_raft_wire_encode_version(
                         codec, TR_RAFT_WIRE_GROUP_VERSION, &metadata,
                         &message, frame, sizeof(frame), &frame_size),
                    SALTS_OK);
        check_equal(tr_raft_wire_decode(codec, frame, frame_size,
                                       &decoded_metadata, &decoded),
                    SALTS_OK);
        check_equal(decoded_metadata.group_id, 100U);
        check_equal(decoded_metadata.message_id, 9U);
        check_equal(decoded.type, message.type);
        check_equal(decoded.term, message.term);
        tr_raft_wire_codec_destroy(codec);
    }

    it("rejects zero group identity only on group-aware wire")
    {
        tr_raft_wire_codec_t *codec = NULL;
        tr_raft_wire_metadata_t metadata;
        tr_raft_wire_metadata_t decoded_metadata;
        tr_raft_message_t message = heartbeat(1U, 2U);
        tr_raft_message_t decoded;
        uint8_t frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];
        size_t frame_size = 0U;

        memset(&metadata, 0, sizeof(metadata));
        metadata.cluster_id.bytes[0] = 1U;
        metadata.message_id = 10U;

        check_equal(tr_raft_wire_codec_create(&codec), SALTS_OK);
        check_equal(tr_raft_wire_encode_version(
                         codec, TR_RAFT_WIRE_GROUP_VERSION, &metadata,
                         &message, frame, sizeof(frame), &frame_size),
                    SALTS_EINVAL);

        check_equal(tr_raft_wire_encode_version(
                         codec, TR_RAFT_WIRE_VERSION, &metadata,
                         &message, frame, sizeof(frame), &frame_size),
                    SALTS_OK);
        check_equal(tr_raft_wire_decode(codec, frame, frame_size,
                                       &decoded_metadata, &decoded),
                    SALTS_OK);
        check_equal(decoded_metadata.group_id, 0U);
        tr_raft_wire_codec_destroy(codec);
    }

    it("selects group wire versions only when capability is negotiated")
    {
        tr_raft_handshake_result_t grouped = contract_for(1U, 2U, true);
        tr_raft_handshake_result_t legacy = contract_for(1U, 2U, false);
        uint16_t wire_version = 0U;
        uint32_t chunk_size = 0U;

        check_equal(tr_raft_handshake_select_raft_wire_version(
                         &grouped, 0U, &wire_version), SALTS_OK);
        check_equal(wire_version, TR_RAFT_WIRE_GROUP_VERSION);
        check_equal(tr_raft_handshake_select_snapshot_wire_version(
                         &grouped, &wire_version, &chunk_size), SALTS_OK);
        check_equal(wire_version, TR_RAFT_WIRE_GROUP_VERSION);
        check_equal(tr_raft_handshake_select_data_wire_version(
                         &grouped, &wire_version), SALTS_OK);
        check_equal(wire_version, TR_RAFT_WIRE_GROUP_VERSION);

        check_equal(tr_raft_handshake_select_raft_wire_version(
                         &legacy, 0U, &wire_version), SALTS_OK);
        check_equal(wire_version, TR_RAFT_WIRE_VERSION);
        check_equal(tr_raft_handshake_select_snapshot_wire_version(
                         &legacy, &wire_version, &chunk_size), SALTS_OK);
        check_equal(wire_version, TR_RAFT_WIRE_SNAPSHOT_VERSION);
        check_equal(tr_raft_handshake_select_data_wire_version(
                         &legacy, &wire_version), SALTS_OK);
        check_equal(wire_version, TR_RAFT_WIRE_SNAPSHOT_VERSION);
    }

    it("keeps legacy sessions while requiring explicit group encode")
    {
        tr_raft_handshake_result_t grouped = contract_for(1U, 2U, true);
        tr_raft_handshake_result_t legacy = contract_for(1U, 2U, false);
        tr_raft_transport_session_config_t config;
        tr_raft_transport_session_t *session = NULL;
        tr_raft_message_t message = heartbeat(1U, 2U);
        uint8_t packet[TR_RAFT_TRANSPORT_MAX_PACKET_SIZE];
        size_t packet_size = 0U;
        tr_raft_group_id_t observed_group = 0U;

        memset(&config, 0, sizeof(config));
        config.cluster_id = grouped.cluster_id;
        config.local_node_id = 1U;
        config.peer_node_id = 2U;
        config.first_outbound_message_id = 1U;
        config.handshake = &grouped;
        config.on_payload = capture_group_payload;
        config.payload_context = &observed_group;

        check_equal(tr_raft_transport_session_create(&config, &session),
                    SALTS_OK);
        check_equal(tr_raft_transport_encode(session, &message, packet,
                                             sizeof(packet), &packet_size),
                    SALTS_EPROTONOSUPPORT);
        check_equal(tr_raft_transport_encode_group(
                         session, 100U, &message, packet, sizeof(packet),
                         &packet_size), SALTS_OK);
        check_equal(packet[TR_RAFT_TRANSPORT_LENGTH_PREFIX_SIZE + 5U],
                    TR_RAFT_WIRE_GROUP_VERSION);
        check_equal(tr_raft_transport_session_destroy(session), SALTS_OK);

        session = NULL;
        memset(&config, 0, sizeof(config));
        config.cluster_id = legacy.cluster_id;
        config.local_node_id = 1U;
        config.peer_node_id = 2U;
        config.first_outbound_message_id = 1U;
        config.handshake = &legacy;
        config.on_message = discard_message;
        check_equal(tr_raft_transport_session_create(&config, &session),
                    SALTS_OK);
        check_equal(tr_raft_transport_encode(session, &message, packet,
                                             sizeof(packet), &packet_size),
                    SALTS_OK);
        check_equal(packet[TR_RAFT_TRANSPORT_LENGTH_PREFIX_SIZE + 5U],
                    TR_RAFT_WIRE_VERSION);
        check_equal(tr_raft_transport_session_destroy(session), SALTS_OK);
    }
}
