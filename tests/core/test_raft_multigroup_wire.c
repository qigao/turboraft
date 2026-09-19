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

    it("round trips group identity for snapshot and data payloads")
    {
        tr_raft_wire_codec_t *codec = NULL;
        tr_raft_wire_metadata_t metadata;
        tr_raft_wire_metadata_t decoded_metadata;
        tr_raft_snapshot_chunk_t snapshot;
        tr_raft_snapshot_chunk_t decoded_snapshot;
        tr_raft_data_chunk_t data_chunk;
        tr_raft_data_chunk_t decoded_data;
        uint8_t frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];
        static const uint8_t snapshot_data[1] = {0x41U};
        static const uint8_t stream_data[1] = {0x42U};
        size_t frame_size = 0U;

        memset(&metadata, 0, sizeof(metadata));
        metadata.cluster_id.bytes[0] = 1U;
        metadata.group_id = 321U;
        metadata.message_id = 11U;

        memset(&snapshot, 0, sizeof(snapshot));
        snapshot.from = 1U;
        snapshot.to = 2U;
        snapshot.term = 7U;
        snapshot.snapshot_index = 10U;
        snapshot.snapshot_term = 6U;
        snapshot.snapshot_size = sizeof(snapshot_data);
        snapshot.data = snapshot_data;
        snapshot.data_length = sizeof(snapshot_data);
        snapshot.done = true;
        snapshot.has_configuration = true;
        snapshot.configuration.phase = TR_RAFT_CONF_FINAL;
        snapshot.configuration.member_count = 1U;
        snapshot.configuration.members[0].node_id = 1U;
        snapshot.configuration.members[0].roles =
            TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER;
        memset(snapshot.snapshot_digest, 0x5a,
               sizeof(snapshot.snapshot_digest));

        check_equal(tr_raft_wire_codec_create(&codec), SALTS_OK);
        check_equal(tr_raft_wire_encode_snapshot_chunk_version(
                         codec, TR_RAFT_WIRE_GROUP_VERSION, &metadata,
                         &snapshot, frame, sizeof(frame), &frame_size),
                    SALTS_OK);
        check_equal(tr_raft_wire_decode_snapshot_chunk(
                         codec, frame, frame_size, &decoded_metadata,
                         &decoded_snapshot), SALTS_OK);
        check_equal(decoded_metadata.group_id, 321U);
        check_equal(decoded_snapshot.snapshot_index, 10U);

        metadata.message_id++;
        memset(&data_chunk, 0, sizeof(data_chunk));
        data_chunk.from = 1U;
        data_chunk.to = 2U;
        data_chunk.term = 7U;
        data_chunk.stream_id = 9U;
        data_chunk.stream_size = sizeof(stream_data);
        data_chunk.data = stream_data;
        data_chunk.data_length = sizeof(stream_data);
        data_chunk.done = true;
        memset(data_chunk.stream_digest, 0x6b,
               sizeof(data_chunk.stream_digest));
        check_equal(tr_raft_wire_encode_data_chunk_version(
                         codec, TR_RAFT_WIRE_GROUP_VERSION, &metadata,
                         &data_chunk, frame, sizeof(frame), &frame_size),
                    SALTS_OK);
        check_equal(tr_raft_wire_decode_data_chunk(
                         codec, frame, frame_size, &decoded_metadata,
                         &decoded_data), SALTS_OK);
        check_equal(decoded_metadata.group_id, 321U);
        check_equal(decoded_data.stream_id, 9U);
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
    }    it("dispatches a group-aware raft frame with its group identity")
    {
        tr_raft_handshake_result_t outbound_contract =
            contract_for(1U, 2U, true);
        tr_raft_handshake_result_t inbound_contract =
            contract_for(2U, 1U, true);
        tr_raft_transport_session_config_t config;
        tr_raft_transport_session_t *outbound = NULL;
        tr_raft_transport_session_t *inbound = NULL;
        tr_raft_message_t message = heartbeat(1U, 2U);
        uint8_t packet[TR_RAFT_TRANSPORT_MAX_PACKET_SIZE];
        size_t packet_size = 0U;
        tr_raft_group_id_t observed_group = 0U;

        memset(&config, 0, sizeof(config));
        config.cluster_id = outbound_contract.cluster_id;
        config.local_node_id = 1U;
        config.peer_node_id = 2U;
        config.first_outbound_message_id = 1U;
        config.handshake = &outbound_contract;
        config.on_payload = capture_group_payload;
        config.payload_context = &observed_group;
        check_equal(tr_raft_transport_session_create(&config, &outbound),
                    SALTS_OK);

        memset(&config, 0, sizeof(config));
        config.cluster_id = inbound_contract.cluster_id;
        config.local_node_id = 2U;
        config.peer_node_id = 1U;
        config.first_outbound_message_id = 1U;
        config.handshake = &inbound_contract;
        config.on_payload = capture_group_payload;
        config.payload_context = &observed_group;
        check_equal(tr_raft_transport_session_create(&config, &inbound),
                    SALTS_OK);

        check_equal(tr_raft_transport_encode_group(
                         outbound, 777U, &message, packet, sizeof(packet),
                         &packet_size), SALTS_OK);
        check_equal(tr_raft_transport_feed(inbound, packet, packet_size),
                    SALTS_OK);
        check_equal(observed_group, 777U);

        check_equal(tr_raft_transport_session_destroy(inbound), SALTS_OK);
        check_equal(tr_raft_transport_session_destroy(outbound), SALTS_OK);
    }

}
