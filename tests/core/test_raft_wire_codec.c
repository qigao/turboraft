#include <turboraft/raft_wire_codec.h>

#include <tinytest.h>
#include <turbo_error.h>

#include <string.h>

spec("raft wire codec")
{
    it("round trips an append request with exact metadata")
    {
        tr_raft_wire_codec_t *codec = NULL;
        tr_raft_wire_metadata_t metadata;
        tr_raft_wire_metadata_t decoded_metadata;
        tr_raft_message_t message;
        tr_raft_message_t decoded;
        uint8_t frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];
        size_t frame_length = 0U;
        size_t index;

        memset(&metadata, 0, sizeof(metadata));
        for (index = 0U; index < sizeof(metadata.cluster_id.bytes); ++index) {
            metadata.cluster_id.bytes[index] = (uint8_t) index;
        }
        metadata.message_id = 99U;
        memset(&message, 0, sizeof(message));
        message.type = TR_RAFT_MSG_APPEND_REQUEST;
        message.from = 1U;
        message.to = 2U;
        message.term = 4U;
        message.previous_log_index = 7U;
        message.previous_log_term = 3U;
        message.leader_commit = 6U;
        message.entry_count = 1U;
        message.entry.index = 8U;
        message.entry.term = 4U;
        message.entry.command_id = 42U;
        message.entry.data_length = 3U;
        memcpy(message.entry.data, "run", 3U);

        check_int_eq(tr_raft_wire_codec_create(&codec), TURBO_OK);
        check_int_eq(tr_raft_wire_encode(codec, &metadata, &message, frame,
                                         sizeof(frame), &frame_length),
                     TURBO_OK);
        check_int_eq(tr_raft_wire_decode(codec, frame, frame_length,
                                         &decoded_metadata, &decoded),
                     TURBO_OK);
        check_mem_eq(decoded_metadata.cluster_id.bytes,
                     metadata.cluster_id.bytes,
                     sizeof(metadata.cluster_id.bytes));
        check_long_eq(decoded_metadata.message_id, 99U);
        check_int_eq(decoded.type, TR_RAFT_MSG_APPEND_REQUEST);
        check_long_eq(decoded.from, 1U);
        check_long_eq(decoded.to, 2U);
        check_long_eq(decoded.entry.index, 8U);
        check_size_eq(decoded.entry.data_length, 3U);
        check_mem_eq(decoded.entry.data, "run", 3U);
        tr_raft_wire_codec_destroy(codec);
    }

    it("rejects an unsupported envelope version")
    {
        tr_raft_wire_codec_t *codec = NULL;
        tr_raft_wire_metadata_t metadata;
        tr_raft_message_t message;
        uint8_t frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];
        size_t frame_length = 0U;

        memset(&metadata, 0, sizeof(metadata));
        memset(&message, 0, sizeof(message));
        message.type = TR_RAFT_MSG_HEARTBEAT_REQUEST;
        message.from = 1U;
        message.to = 2U;
        check_int_eq(tr_raft_wire_codec_create(&codec), TURBO_OK);
        check_int_eq(tr_raft_wire_encode(codec, &metadata, &message, frame,
                                         sizeof(frame), &frame_length),
                     TURBO_OK);
        frame[5] = 4U;
        check_int_eq(tr_raft_wire_decode(codec, frame, frame_length, &metadata,
                                         &message),
                     TURBO_EPROTO);
        tr_raft_wire_codec_destroy(codec);
    }

    it("round trips TimeoutNow without changing the wire envelope")
    {
        tr_raft_wire_codec_t *codec = NULL;
        tr_raft_wire_metadata_t metadata;
        tr_raft_wire_metadata_t decoded_metadata;
        tr_raft_message_t message;
        tr_raft_message_t decoded;
        uint8_t frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];
        size_t frame_length = 0U;

        memset(&metadata, 0, sizeof(metadata));
        metadata.message_id = 100U;
        memset(&message, 0, sizeof(message));
        message.type = TR_RAFT_MSG_TIMEOUT_NOW;
        message.from = 1U;
        message.to = 2U;
        message.term = 9U;
        check_int_eq(tr_raft_wire_codec_create(&codec), TURBO_OK);
        check_int_eq(tr_raft_wire_encode(codec, &metadata, &message, frame,
                                         sizeof(frame), &frame_length),
                     TURBO_OK);
        check_int_eq(tr_raft_wire_decode(codec, frame, frame_length,
                                         &decoded_metadata, &decoded),
                     TURBO_OK);
        check_int_eq(decoded.type, TR_RAFT_MSG_TIMEOUT_NOW);
        check_long_eq(decoded.from, 1U);
        check_long_eq(decoded.to, 2U);
        check_long_eq(decoded.term, 9U);
        tr_raft_wire_codec_destroy(codec);
    }

    it("round trips a ReadIndex context in the existing campaign slot")
    {
        tr_raft_wire_codec_t *codec = NULL;
        tr_raft_wire_metadata_t metadata;
        tr_raft_wire_metadata_t decoded_metadata;
        tr_raft_message_t message;
        tr_raft_message_t decoded;
        uint8_t frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];
        size_t frame_length = 0U;

        memset(&metadata, 0, sizeof(metadata));
        metadata.message_id = 101U;
        memset(&message, 0, sizeof(message));
        message.type = TR_RAFT_MSG_READ_INDEX_REQUEST;
        message.from = 1U;
        message.to = 2U;
        message.term = 9U;
        message.context_id = 88U;
        check_int_eq(tr_raft_wire_codec_create(&codec), TURBO_OK);
        check_int_eq(tr_raft_wire_encode(codec, &metadata, &message, frame,
                                         sizeof(frame), &frame_length),
                     TURBO_OK);
        check_int_eq(tr_raft_wire_decode(codec, frame, frame_length,
                                         &decoded_metadata, &decoded),
                     TURBO_OK);
        check_int_eq(decoded.type, TR_RAFT_MSG_READ_INDEX_REQUEST);
        check_long_eq(decoded.context_id, 88U);
        check_long_eq(decoded.term, 9U);
        tr_raft_wire_codec_destroy(codec);
    }

    it("round trips a v3 bounded entry batch")
    {
        tr_raft_wire_codec_t *codec = NULL;
        tr_raft_wire_metadata_t metadata;
        tr_raft_wire_metadata_t decoded_metadata;
        tr_raft_message_t message;
        tr_raft_message_t decoded;
        uint8_t frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];
        size_t frame_length = 0U;
        size_t index;

        memset(&metadata, 0, sizeof(metadata));
        metadata.message_id = 102U;
        memset(&message, 0, sizeof(message));
        message.type = TR_RAFT_MSG_APPEND_REQUEST;
        message.from = 1U;
        message.to = 2U;
        message.term = 4U;
        message.previous_log_index = 7U;
        message.previous_log_term = 3U;
        message.entry_count = 3U;
        for (index = 0U; index < message.entry_count; ++index) {
            message.entries[index].index = 8U + index;
            message.entries[index].term = 4U;
            message.entries[index].command_id = 50U + index;
            message.entries[index].data_length = index + 1U;
            memset(message.entries[index].data, (int) ('a' + index),
                   index + 1U);
        }
        check_int_eq(tr_raft_wire_codec_create(&codec), TURBO_OK);
        check_int_eq(tr_raft_wire_encode(codec, &metadata, &message, frame,
                                         sizeof(frame), &frame_length),
                     TURBO_OK);
        check_int_eq(frame[5], TR_RAFT_WIRE_VERSION);
        check_int_eq(tr_raft_wire_decode(codec, frame, frame_length,
                                         &decoded_metadata, &decoded),
                     TURBO_OK);
        check_size_eq(decoded.entry_count, 3U);
        check_long_eq(decoded.entries[2].index, 10U);
        check_size_eq(decoded.entries[2].data_length, 3U);
        check_mem_eq(decoded.entries[2].data, "ccc", 3U);
        tr_raft_wire_codec_destroy(codec);
    }

    it("rejects trailing bytes in a v3 payload")
    {
        tr_raft_wire_codec_t *codec = NULL;
        tr_raft_wire_metadata_t metadata;
        tr_raft_message_t message;
        uint8_t frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];
        size_t frame_length = 0U;
        size_t payload_length;

        memset(&metadata, 0, sizeof(metadata));
        memset(&message, 0, sizeof(message));
        message.type = TR_RAFT_MSG_HEARTBEAT_REQUEST;
        message.from = 1U;
        message.to = 2U;
        check_int_eq(tr_raft_wire_codec_create(&codec), TURBO_OK);
        check_int_eq(tr_raft_wire_encode(codec, &metadata, &message, frame,
                                         sizeof(frame), &frame_length),
                     TURBO_OK);

        payload_length = frame_length - TR_RAFT_WIRE_HEADER_SIZE + 1U;
        frame[frame_length++] = 0U;
        frame[8] = (uint8_t) (payload_length >> 24U);
        frame[9] = (uint8_t) (payload_length >> 16U);
        frame[10] = (uint8_t) (payload_length >> 8U);
        frame[11] = (uint8_t) payload_length;
        check_int_eq(tr_raft_wire_decode(codec, frame, frame_length, &metadata,
                                         &message),
                     TURBO_EPROTO);
        tr_raft_wire_codec_destroy(codec);
    }

    it("decodes explicitly negotiated v2 single-entry frames")
    {
        tr_raft_wire_codec_t *codec = NULL;
        tr_raft_wire_metadata_t metadata;
        tr_raft_wire_metadata_t decoded_metadata;
        tr_raft_message_t message;
        tr_raft_message_t decoded;
        uint8_t frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];
        size_t frame_length = 0U;

        memset(&metadata, 0, sizeof(metadata));
        metadata.message_id = 103U;
        memset(&message, 0, sizeof(message));
        message.type = TR_RAFT_MSG_APPEND_REQUEST;
        message.from = 1U;
        message.to = 2U;
        message.term = 4U;
        message.entry_count = 1U;
        message.entries[0].index = 1U;
        message.entries[0].term = 4U;
        message.entries[0].command_id = 60U;
        message.entries[0].data_length = 2U;
        memcpy(message.entries[0].data, "v2", 2U);
        check_int_eq(tr_raft_wire_codec_create(&codec), TURBO_OK);
        check_int_eq(tr_raft_wire_encode_version(
                         codec, 2U, &metadata, &message, frame, sizeof(frame),
                         &frame_length),
                     TURBO_OK);
        check_int_eq(frame[5], 2U);
        check_int_eq(tr_raft_wire_decode(codec, frame, frame_length,
                                         &decoded_metadata, &decoded),
                     TURBO_OK);
        check_size_eq(decoded.entry_count, 1U);
        check_mem_eq(decoded.entries[0].data, "v2", 2U);
        tr_raft_wire_codec_destroy(codec);
    }

    it("round trips bounded snapshot chunks and acknowledgements")
    {
        tr_raft_wire_codec_t *codec = NULL;
        tr_raft_wire_metadata_t metadata;
        tr_raft_wire_metadata_t decoded_metadata;
        tr_raft_snapshot_chunk_t chunk;
        tr_raft_snapshot_chunk_t decoded_chunk;
        tr_raft_snapshot_ack_t ack;
        tr_raft_snapshot_ack_t decoded_ack;
        uint8_t frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];
        uint8_t chunk_data[88];
        size_t frame_length = 0U;
        size_t index;

        memset(&metadata, 0, sizeof(metadata));
        metadata.message_id = 101U;
        memset(&chunk, 0, sizeof(chunk));
        chunk.from = 1U;
        chunk.to = 2U;
        chunk.term = 7U;
        chunk.snapshot_index = 50U;
        chunk.snapshot_term = 6U;
        chunk.snapshot_offset = 0U;
        chunk.snapshot_size = 88U;
        chunk.data_length = 88U;
        chunk.done = true;
        chunk.has_configuration = true;
        chunk.configuration.phase = TR_RAFT_CONF_FINAL;
        chunk.configuration.member_count = 1U;
        chunk.configuration.members[0].node_id = 2U;
        chunk.configuration.members[0].roles =
            TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER;
        for (index = 0U; index < sizeof(chunk.snapshot_digest); ++index) {
            chunk.snapshot_digest[index] = (uint8_t) (index + 1U);
        }
        memset(chunk_data, 0x5a, sizeof(chunk_data));
        chunk.data = chunk_data;

        check_int_eq(tr_raft_wire_codec_create(&codec), TURBO_OK);
        check_int_eq(tr_raft_wire_encode_snapshot_chunk(
                         codec, &metadata, &chunk, frame, sizeof(frame),
                         &frame_length),
                     TURBO_OK);
        check_int_eq(tr_raft_wire_decode_snapshot_chunk(
                         codec, frame, frame_length, &decoded_metadata,
                         &decoded_chunk),
                     TURBO_OK);
        check_long_eq(decoded_metadata.message_id, 101U);
        check_long_eq(decoded_chunk.snapshot_index, 50U);
        check_long_eq(decoded_chunk.snapshot_offset, 0U);
        check(decoded_chunk.has_configuration);
        check_size_eq(decoded_chunk.configuration.member_count, 1U);
        check_size_eq(decoded_chunk.data_length, 88U);
        check(decoded_chunk.done);
        check_mem_eq(decoded_chunk.snapshot_digest, chunk.snapshot_digest,
                     sizeof(chunk.snapshot_digest));
        check_mem_eq(decoded_chunk.data, chunk.data, chunk.data_length);

        memset(&ack, 0, sizeof(ack));
        ack.from = 2U;
        ack.to = 1U;
        ack.term = 7U;
        ack.snapshot_index = 50U;
        ack.snapshot_size = 600U;
        ack.next_offset = 600U;
        ack.accepted = true;
        memcpy(ack.snapshot_digest, chunk.snapshot_digest,
               sizeof(ack.snapshot_digest));
        check_int_eq(tr_raft_wire_encode_snapshot_ack(
                         codec, &metadata, &ack, frame, sizeof(frame),
                         &frame_length),
                     TURBO_OK);
        check_int_eq(tr_raft_wire_decode_snapshot_ack(
                         codec, frame, frame_length, &decoded_metadata,
                         &decoded_ack),
                     TURBO_OK);
        check(decoded_ack.accepted);
        check_long_eq(decoded_ack.next_offset, 600U);
        check_mem_eq(decoded_ack.snapshot_digest, ack.snapshot_digest,
                     sizeof(ack.snapshot_digest));
        tr_raft_wire_codec_destroy(codec);
    }

    it("rejects invalid snapshot geometry and payload kinds")
    {
        tr_raft_wire_codec_t *codec = NULL;
        tr_raft_wire_metadata_t metadata;
        tr_raft_snapshot_chunk_t chunk;
        tr_raft_snapshot_chunk_t decoded;
        uint8_t frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];
        uint8_t chunk_data[512] = {0};
        size_t frame_length = 0U;

        memset(&metadata, 0, sizeof(metadata));
        memset(&chunk, 0, sizeof(chunk));
        chunk.from = 1U;
        chunk.to = 2U;
        chunk.term = 1U;
        chunk.snapshot_index = 1U;
        chunk.snapshot_term = 1U;
        chunk.snapshot_size = 513U;
        chunk.has_configuration = true;
        chunk.configuration.phase = TR_RAFT_CONF_FINAL;
        chunk.configuration.member_count = 1U;
        chunk.configuration.members[0].node_id = 2U;
        chunk.configuration.members[0].roles =
            TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER;
        chunk.data_length = 513U;
        chunk.data = chunk_data;
        check_int_eq(tr_raft_wire_codec_create(&codec), TURBO_OK);
        check_int_eq(tr_raft_wire_encode_snapshot_chunk(
                         codec, &metadata, &chunk, frame, sizeof(frame),
                         &frame_length),
                     TURBO_EINVAL);

        chunk.data_length = 512U;
        check_int_eq(tr_raft_wire_encode_snapshot_chunk(
                         codec, &metadata, &chunk, frame, sizeof(frame),
                         &frame_length),
                     TURBO_OK);
        frame[15] = TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_ACK;
        check_int_eq(tr_raft_wire_decode_snapshot_chunk(
                         codec, frame, frame_length, &metadata, &decoded),
                     TURBO_EPROTO);
        tr_raft_wire_codec_destroy(codec);
    }

    it("encodes a zero-allocation 64 KiB V5 chunk and preserves V4 limit")
    {
        tr_raft_wire_codec_t *codec = NULL;
        tr_raft_wire_metadata_t metadata;
        tr_raft_wire_metadata_t decoded_metadata;
        tr_raft_snapshot_chunk_t chunk;
        tr_raft_snapshot_chunk_t decoded;
        uint8_t data[TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES];
        uint8_t frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];
        size_t frame_length = 0U;
        uint16_t version = 0U;

        memset(data, 0xa5, sizeof(data));
        memset(&metadata, 0, sizeof(metadata));
        metadata.cluster_id.bytes[0] = 1U;
        metadata.message_id = 2U;
        memset(&chunk, 0, sizeof(chunk));
        chunk.from = 1U;
        chunk.to = 2U;
        chunk.term = 3U;
        chunk.snapshot_index = 4U;
        chunk.snapshot_term = 2U;
        chunk.snapshot_size = sizeof(data);
        chunk.has_configuration = true;
        chunk.configuration.phase = TR_RAFT_CONF_FINAL;
        chunk.configuration.member_count = 1U;
        chunk.configuration.members[0].node_id = 2U;
        chunk.configuration.members[0].roles =
            TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER;
        chunk.data = data;
        chunk.data_length = sizeof(data);
        chunk.done = true;
        memset(chunk.snapshot_digest, 0x3c, sizeof(chunk.snapshot_digest));

        check_int_eq(tr_raft_wire_codec_create(&codec), TURBO_OK);
        check_int_eq(tr_raft_wire_encode_snapshot_chunk(
                         codec, &metadata, &chunk, frame, sizeof(frame),
                         &frame_length), TURBO_OK);
        check_int_eq(tr_raft_wire_peek_version(frame, frame_length, &version),
                     TURBO_OK);
        check_int_eq(version, TR_RAFT_WIRE_SNAPSHOT_VERSION);
        check_int_eq(tr_raft_wire_decode_snapshot_chunk(
                         codec, frame, frame_length, &decoded_metadata,
                         &decoded), TURBO_OK);
        check_size_eq(decoded.data_length, sizeof(data));
        check_mem_eq(decoded.data, data, sizeof(data));
        check_int_eq(tr_raft_wire_encode_snapshot_chunk_version(
                         codec, TR_RAFT_WIRE_SNAPSHOT_LEGACY_VERSION,
                         &metadata, &chunk, frame, sizeof(frame),
                         &frame_length), TURBO_EPROTO);
        tr_raft_wire_codec_destroy(codec);
    }
}
