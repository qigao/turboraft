#include <turboraft/raft_transport.h>

#include <salts_error.h>
#include <tinytest.h>

#include <string.h>

#if defined(TR_RAFT_WIRE_MIN_VERSION)
#error "legacy minimum wire version must be removed"
#endif

#if defined(TR_RAFT_WIRE_SNAPSHOT_LEGACY_VERSION)
#error "legacy snapshot wire version must be removed"
#endif

#if defined(TR_RAFT_WIRE_SNAPSHOT_VERSION)
#error "separate snapshot wire version must be removed"
#endif

#if defined(TR_RAFT_WIRE_GROUP_VERSION)
#error "group-aware transport is the baseline; separate group version must be removed"
#endif

#if defined(TR_RAFT_WIRE_LEGACY_HEADER_SIZE)
#error "legacy header size must be removed"
#endif

#if defined(TR_RAFT_HANDSHAKE_FEATURE_RAFT_BATCH_V3)
#error "historical raft payload feature bit must be removed"
#endif

#if defined(TR_RAFT_HANDSHAKE_FEATURE_SNAPSHOT_V4)
#error "historical snapshot v4 feature bit must be removed"
#endif

#if defined(TR_RAFT_HANDSHAKE_FEATURE_SNAPSHOT_V5)
#error "historical snapshot v5 feature bit must be removed"
#endif

#if defined(TR_RAFT_HANDSHAKE_FEATURE_DATA_STREAM_V5)
#error "historical data-stream v5 feature bit must be removed"
#endif

#if defined(TR_RAFT_HANDSHAKE_FEATURE_GROUP_MULTIPLEX_V1)
#error "group multiplex is baseline and must not be negotiated as an option"
#endif

_Static_assert(TR_RAFT_WIRE_VERSION == 6U,
               "current peer wire version must be 6");
_Static_assert(TR_RAFT_WIRE_HEADER_SIZE == 48U,
               "current envelope must include group_id");

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

static tr_raft_handshake_result_t current_contract(
    tr_raft_node_id_t local_node_id,
    tr_raft_node_id_t peer_node_id)
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
    result.feature_bits = 0U;
    result.wire_major = TR_RAFT_HANDSHAKE_WIRE_MAJOR;
    result.wire_minor = TR_RAFT_HANDSHAKE_WIRE_MINOR;
    result.max_frame_size = TR_RAFT_WIRE_MAX_FRAME_SIZE;
    result.max_snapshot_chunk_size =
        TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES;
    return result;
}

static tr_raft_handshake_config_t current_config(
    uint8_t seed,
    tr_raft_node_id_t node_id)
{
    tr_raft_handshake_config_t config;
    size_t index;

    memset(&config, 0, sizeof(config));
    for (index = 0U; index < sizeof(config.cluster_id.bytes); ++index) {
        config.cluster_id.bytes[index] = (uint8_t)(30U + index);
        config.process_incarnation.bytes[index] =
            (uint8_t)(seed + index);
    }
    config.local_node_id = node_id;
    config.config_epoch = 1U;
    config.feature_bits = 0U;
    config.wire_major_min = TR_RAFT_HANDSHAKE_WIRE_MAJOR;
    config.wire_major_max = TR_RAFT_HANDSHAKE_WIRE_MAJOR;
    config.wire_minor_min = TR_RAFT_HANDSHAKE_WIRE_MINOR;
    config.wire_minor_max = TR_RAFT_HANDSHAKE_WIRE_MINOR;
    config.max_frame_size = TR_RAFT_WIRE_MAX_FRAME_SIZE;
    config.max_snapshot_chunk_size =
        TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES;
    return config;
}

static int capture_payload(void *context,
                           const tr_raft_transport_payload_t *payload)
{
    tr_raft_group_id_t *group_id = (tr_raft_group_id_t *)context;

    *group_id = payload->group_id;
    return SALTS_OK;
}

spec("raft mandatory group wire contract")
{
    it("round trips group identity in the only raft envelope")
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
        check_equal(tr_raft_wire_encode(codec, &metadata, &message, frame,
                                        sizeof(frame), &frame_size),
                    SALTS_OK);
        check_equal(frame[5], TR_RAFT_WIRE_VERSION);
        check_equal(frame[7], TR_RAFT_WIRE_HEADER_SIZE);
        check_equal(tr_raft_wire_decode(codec, frame, frame_size,
                                        &decoded_metadata, &decoded),
                    SALTS_OK);
        check_equal(decoded_metadata.group_id, 100U);
        check_equal(decoded_metadata.message_id, 9U);
        check_equal(decoded.type, message.type);
        check_equal(decoded.term, message.term);
        tr_raft_wire_codec_destroy(codec);
    }

    it("rejects zero group identity")
    {
        tr_raft_wire_codec_t *codec = NULL;
        tr_raft_wire_metadata_t metadata;
        tr_raft_message_t message = heartbeat(1U, 2U);
        uint8_t frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];
        size_t frame_size = 0U;

        memset(&metadata, 0, sizeof(metadata));
        metadata.cluster_id.bytes[0] = 1U;
        metadata.message_id = 10U;

        check_equal(tr_raft_wire_codec_create(&codec), SALTS_OK);
        check_equal(tr_raft_wire_encode(codec, &metadata, &message, frame,
                                        sizeof(frame), &frame_size),
                    SALTS_EINVAL);
        tr_raft_wire_codec_destroy(codec);
    }

    it("rejects pre-r0 legacy frame versions")
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
        metadata.message_id = 11U;

        check_equal(tr_raft_wire_codec_create(&codec), SALTS_OK);
        check_equal(tr_raft_wire_encode(codec, &metadata, &message, frame,
                                        sizeof(frame), &frame_size),
                    SALTS_OK);

        frame[4] = 0U;
        frame[5] = 3U;
        check_equal(tr_raft_wire_decode(codec, frame, frame_size,
                                        &decoded_metadata, &decoded),
                    SALTS_EPROTO);
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
        metadata.message_id = 12U;

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
        check_equal(tr_raft_wire_encode_snapshot_chunk(
                         codec, &metadata, &snapshot, frame, sizeof(frame),
                         &frame_size),
                    SALTS_OK);
        check_equal(tr_raft_wire_decode_snapshot_chunk(
                         codec, frame, frame_size, &decoded_metadata,
                         &decoded_snapshot),
                    SALTS_OK);
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

        check_equal(tr_raft_wire_encode_data_chunk(
                         codec, &metadata, &data_chunk, frame, sizeof(frame),
                         &frame_size),
                    SALTS_OK);
        check_equal(tr_raft_wire_decode_data_chunk(
                         codec, frame, frame_size, &decoded_metadata,
                         &decoded_data),
                    SALTS_OK);
        check_equal(decoded_metadata.group_id, 321U);
        check_equal(decoded_data.stream_id, 9U);
        tr_raft_wire_codec_destroy(codec);
    }

    it("accepts a baseline handshake with no historical feature bits")
    {
        tr_raft_handshake_config_t first = current_config(20U, 1U);
        tr_raft_handshake_config_t second = current_config(40U, 2U);
        tr_raft_handshake_message_t first_hello;
        tr_raft_handshake_message_t second_hello;
        tr_raft_handshake_message_t first_ack;
        tr_raft_handshake_message_t second_ack;
        tr_raft_handshake_result_t first_result;
        tr_raft_handshake_result_t second_result;

        check_equal(tr_raft_handshake_make_hello(&first, &first_hello),
                    SALTS_OK);
        check_equal(tr_raft_handshake_make_hello(&second, &second_hello),
                    SALTS_OK);
        check_equal(tr_raft_handshake_negotiate(
                         &first, 2U, &second_hello, &first_ack,
                         &first_result),
                    SALTS_OK);
        check_equal(tr_raft_handshake_negotiate(
                         &second, 1U, &first_hello, &second_ack,
                         &second_result),
                    SALTS_OK);
        check_equal(tr_raft_handshake_validate_ack(
                         &first_result, &second_ack),
                    SALTS_OK);
        check_equal(tr_raft_handshake_validate_ack(
                         &second_result, &first_ack),
                    SALTS_OK);
        check_equal(first_result.feature_bits, 0U);
    }

    it("requires group identity in the unified transport API")
    {
        tr_raft_handshake_result_t outbound_contract =
            current_contract(1U, 2U);
        tr_raft_handshake_result_t inbound_contract =
            current_contract(2U, 1U);
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
        config.on_payload = capture_payload;
        config.payload_context = &observed_group;
        check_equal(tr_raft_transport_session_create(&config, &outbound),
                    SALTS_OK);

        memset(&config, 0, sizeof(config));
        config.cluster_id = inbound_contract.cluster_id;
        config.local_node_id = 2U;
        config.peer_node_id = 1U;
        config.first_outbound_message_id = 1U;
        config.handshake = &inbound_contract;
        config.on_payload = capture_payload;
        config.payload_context = &observed_group;
        check_equal(tr_raft_transport_session_create(&config, &inbound),
                    SALTS_OK);

        check_equal(tr_raft_transport_encode(
                         outbound, 777U, &message, packet, sizeof(packet),
                         &packet_size),
                    SALTS_OK);
        check_equal(tr_raft_transport_feed(inbound, packet, packet_size),
                    SALTS_OK);
        check_equal(observed_group, 777U);

        check_equal(tr_raft_transport_session_destroy(inbound), SALTS_OK);
        check_equal(tr_raft_transport_session_destroy(outbound), SALTS_OK);
    }
}
