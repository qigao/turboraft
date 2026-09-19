#include <turboraft/raft_wire_codec.h>

#include <tinytest.h>
#include <salts_error.h>

#include <string.h>

static tr_raft_wire_metadata_t metadata_for(uint64_t group_id,
                                             uint64_t message_id)
{
    tr_raft_wire_metadata_t metadata;

    memset(&metadata, 0, sizeof(metadata));
    metadata.cluster_id.bytes[0] = 1U;
    metadata.group_id = group_id;
    metadata.message_id = message_id;
    return metadata;
}

spec("raft wire codec")
{
    it("round trips an append request with exact group metadata")
    {
        tr_raft_wire_codec_t *codec = NULL;
        tr_raft_wire_metadata_t metadata = metadata_for(100U, 99U);
        tr_raft_wire_metadata_t decoded_metadata;
        tr_raft_message_t message;
        tr_raft_message_t decoded;
        uint8_t frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];
        size_t frame_length = 0U;

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

        check_equal(tr_raft_wire_codec_create(&codec), SALTS_OK);
        check_equal(tr_raft_wire_encode(codec, &metadata, &message, frame,
                                        sizeof(frame), &frame_length),
                    SALTS_OK);
        check_equal(frame[5], TR_RAFT_WIRE_VERSION);
        check_equal(frame[7], TR_RAFT_WIRE_HEADER_SIZE);
        check_equal(tr_raft_wire_decode(codec, frame, frame_length,
                                        &decoded_metadata, &decoded),
                    SALTS_OK);
        check_equal(decoded_metadata.group_id, 100U);
        check_equal(decoded_metadata.message_id, 99U);
        check_equal(decoded.type, TR_RAFT_MSG_APPEND_REQUEST);
        check_equal(decoded.from, 1U);
        check_equal(decoded.to, 2U);
        check_equal(decoded.entry.index, 8U);
        check_equal(decoded.entry.data_length, 3U);
        check_equal(decoded.entry.data, "run", 3U);
        tr_raft_wire_codec_destroy(codec);
    }

    it("rejects zero group identity and pre-r0 envelope versions")
    {
        tr_raft_wire_codec_t *codec = NULL;
        tr_raft_wire_metadata_t metadata = metadata_for(0U, 1U);
        tr_raft_message_t message;
        uint8_t frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];
        size_t frame_length = 0U;

        memset(&message, 0, sizeof(message));
        message.type = TR_RAFT_MSG_HEARTBEAT_REQUEST;
        message.from = 1U;
        message.to = 2U;
        message.term = 1U;

        check_equal(tr_raft_wire_codec_create(&codec), SALTS_OK);
        check_equal(tr_raft_wire_encode(codec, &metadata, &message, frame,
                                        sizeof(frame), &frame_length),
                    SALTS_EINVAL);

        metadata.group_id = 1U;
        check_equal(tr_raft_wire_encode(codec, &metadata, &message, frame,
                                        sizeof(frame), &frame_length),
                    SALTS_OK);
        frame[4] = 0U;
        frame[5] = 3U;
        check_equal(tr_raft_wire_decode(codec, frame, frame_length, &metadata,
                                        &message),
                    SALTS_EPROTO);
        tr_raft_wire_codec_destroy(codec);
    }

    it("round trips TimeoutNow in the current envelope")
    {
        tr_raft_wire_codec_t *codec = NULL;
        tr_raft_wire_metadata_t metadata = metadata_for(1U, 100U);
        tr_raft_wire_metadata_t decoded_metadata;
        tr_raft_message_t message;
        tr_raft_message_t decoded;
        uint8_t frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];
        size_t frame_length = 0U;

        memset(&message, 0, sizeof(message));
        message.type = TR_RAFT_MSG_TIMEOUT_NOW;
        message.from = 1U;
        message.to = 2U;
        message.term = 9U;
        check_equal(tr_raft_wire_codec_create(&codec), SALTS_OK);
        check_equal(tr_raft_wire_encode(codec, &metadata, &message, frame,
                                        sizeof(frame), &frame_length),
                    SALTS_OK);
        check_equal(tr_raft_wire_decode(codec, frame, frame_length,
                                        &decoded_metadata, &decoded),
                    SALTS_OK);
        check_equal(decoded_metadata.group_id, 1U);
        check_equal(decoded.type, TR_RAFT_MSG_TIMEOUT_NOW);
        check_equal(decoded.term, 9U);
        tr_raft_wire_codec_destroy(codec);
    }

    it("round trips a ReadIndex context")
    {
        tr_raft_wire_codec_t *codec = NULL;
        tr_raft_wire_metadata_t metadata = metadata_for(2U, 101U);
        tr_raft_wire_metadata_t decoded_metadata;
        tr_raft_message_t message;
        tr_raft_message_t decoded;
        uint8_t frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];
        size_t frame_length = 0U;

        memset(&message, 0, sizeof(message));
        message.type = TR_RAFT_MSG_READ_INDEX_REQUEST;
        message.from = 1U;
        message.to = 2U;
        message.term = 9U;
        message.context_id = 88U;
        check_equal(tr_raft_wire_codec_create(&codec), SALTS_OK);
        check_equal(tr_raft_wire_encode(codec, &metadata, &message, frame,
                                        sizeof(frame), &frame_length),
                    SALTS_OK);
        check_equal(tr_raft_wire_decode(codec, frame, frame_length,
                                        &decoded_metadata, &decoded),
                    SALTS_OK);
        check_equal(decoded_metadata.group_id, 2U);
        check_equal(decoded.type, TR_RAFT_MSG_READ_INDEX_REQUEST);
        check_equal(decoded.context_id, 88U);
        tr_raft_wire_codec_destroy(codec);
    }

    it("round trips a bounded entry batch")
    {
        tr_raft_wire_codec_t *codec = NULL;
        tr_raft_wire_metadata_t metadata = metadata_for(3U, 102U);
        tr_raft_wire_metadata_t decoded_metadata;
        tr_raft_message_t message;
        tr_raft_message_t decoded;
        uint8_t frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];
        size_t frame_length = 0U;
        size_t index;

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
            memset(message.entries[index].data, (int)('a' + index),
                   index + 1U);
        }

        check_equal(tr_raft_wire_codec_create(&codec), SALTS_OK);
        check_equal(tr_raft_wire_encode(codec, &metadata, &message, frame,
                                        sizeof(frame), &frame_length),
                    SALTS_OK);
        check_equal(tr_raft_wire_decode(codec, frame, frame_length,
                                        &decoded_metadata, &decoded),
                    SALTS_OK);
        check_equal(decoded.entry_count, 3U);
        check_equal(decoded.entries[2].index, 10U);
        check_equal(decoded.entries[2].data, "ccc", 3U);
        tr_raft_wire_codec_destroy(codec);
    }

    it("rejects trailing bytes in the current payload")
    {
        tr_raft_wire_codec_t *codec = NULL;
        tr_raft_wire_metadata_t metadata = metadata_for(4U, 103U);
        tr_raft_message_t message;
        uint8_t frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];
        size_t frame_length = 0U;
        size_t payload_length;

        memset(&message, 0, sizeof(message));
        message.type = TR_RAFT_MSG_HEARTBEAT_REQUEST;
        message.from = 1U;
        message.to = 2U;
        message.term = 1U;
        check_equal(tr_raft_wire_codec_create(&codec), SALTS_OK);
        check_equal(tr_raft_wire_encode(codec, &metadata, &message, frame,
                                        sizeof(frame), &frame_length),
                    SALTS_OK);

        payload_length = frame_length - TR_RAFT_WIRE_HEADER_SIZE + 1U;
        frame[frame_length++] = 0U;
        frame[8] = (uint8_t)(payload_length >> 24U);
        frame[9] = (uint8_t)(payload_length >> 16U);
        frame[10] = (uint8_t)(payload_length >> 8U);
        frame[11] = (uint8_t)payload_length;
        check_equal(tr_raft_wire_decode(codec, frame, frame_length, &metadata,
                                        &message),
                    SALTS_EPROTO);
        tr_raft_wire_codec_destroy(codec);
    }

    it("round trips bounded snapshot chunks and acknowledgements")
    {
        tr_raft_wire_codec_t *codec = NULL;
        tr_raft_wire_metadata_t metadata = metadata_for(5U, 104U);
        tr_raft_wire_metadata_t decoded_metadata;
        tr_raft_snapshot_chunk_t chunk;
        tr_raft_snapshot_chunk_t decoded_chunk;
        tr_raft_snapshot_ack_t ack;
        tr_raft_snapshot_ack_t decoded_ack;
        uint8_t frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];
        uint8_t chunk_data[88];
        size_t frame_length = 0U;
        size_t index;

        memset(&chunk, 0, sizeof(chunk));
        chunk.from = 1U;
        chunk.to = 2U;
        chunk.term = 7U;
        chunk.snapshot_index = 50U;
        chunk.snapshot_term = 6U;
        chunk.snapshot_size = sizeof(chunk_data);
        chunk.data_length = sizeof(chunk_data);
        chunk.done = true;
        chunk.has_configuration = true;
        chunk.configuration.phase = TR_RAFT_CONF_FINAL;
        chunk.configuration.member_count = 1U;
        chunk.configuration.members[0].node_id = 2U;
        chunk.configuration.members[0].roles =
            TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER;
        for (index = 0U; index < sizeof(chunk.snapshot_digest); ++index) {
            chunk.snapshot_digest[index] = (uint8_t)(index + 1U);
        }
        memset(chunk_data, 0x5a, sizeof(chunk_data));
        chunk.data = chunk_data;

        check_equal(tr_raft_wire_codec_create(&codec), SALTS_OK);
        check_equal(tr_raft_wire_encode_snapshot_chunk(
                         codec, &metadata, &chunk, frame, sizeof(frame),
                         &frame_length),
                    SALTS_OK);
        check_equal(tr_raft_wire_decode_snapshot_chunk(
                         codec, frame, frame_length, &decoded_metadata,
                         &decoded_chunk),
                    SALTS_OK);
        check_equal(decoded_metadata.group_id, 5U);
        check_equal(decoded_chunk.snapshot_index, 50U);
        check_equal(decoded_chunk.data, chunk.data, chunk.data_length);

        metadata.message_id++;
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
        check_equal(tr_raft_wire_encode_snapshot_ack(
                         codec, &metadata, &ack, frame, sizeof(frame),
                         &frame_length),
                    SALTS_OK);
        check_equal(tr_raft_wire_decode_snapshot_ack(
                         codec, frame, frame_length, &decoded_metadata,
                         &decoded_ack),
                    SALTS_OK);
        check_equal(decoded_metadata.group_id, 5U);
        check_true(decoded_ack.accepted);
        tr_raft_wire_codec_destroy(codec);
    }

    it("rejects invalid snapshot geometry and payload kinds")
    {
        tr_raft_wire_codec_t *codec = NULL;
        tr_raft_wire_metadata_t metadata = metadata_for(6U, 106U);
        tr_raft_snapshot_chunk_t chunk;
        tr_raft_snapshot_chunk_t decoded;
        uint8_t frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];
        uint8_t chunk_data[512] = {0};
        size_t frame_length = 0U;

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

        check_equal(tr_raft_wire_codec_create(&codec), SALTS_OK);
        check_equal(tr_raft_wire_encode_snapshot_chunk(
                         codec, &metadata, &chunk, frame, sizeof(frame),
                         &frame_length),
                    SALTS_EINVAL);

        chunk.data_length = 512U;
        check_equal(tr_raft_wire_encode_snapshot_chunk(
                         codec, &metadata, &chunk, frame, sizeof(frame),
                         &frame_length),
                    SALTS_OK);
        frame[15] = TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_ACK;
        check_equal(tr_raft_wire_decode_snapshot_chunk(
                         codec, frame, frame_length, &metadata, &decoded),
                    SALTS_EPROTO);
        tr_raft_wire_codec_destroy(codec);
    }

    it("encodes and decodes a current 64 KiB snapshot chunk")
    {
        tr_raft_wire_codec_t *codec = NULL;
        tr_raft_wire_metadata_t metadata = metadata_for(7U, 107U);
        tr_raft_wire_metadata_t decoded_metadata;
        tr_raft_snapshot_chunk_t chunk;
        tr_raft_snapshot_chunk_t decoded;
        uint8_t data[TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES];
        uint8_t frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];
        size_t frame_length = 0U;
        uint16_t version = 0U;

        memset(data, 0xa5, sizeof(data));
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

        check_equal(tr_raft_wire_codec_create(&codec), SALTS_OK);
        check_equal(tr_raft_wire_encode_snapshot_chunk(
                         codec, &metadata, &chunk, frame, sizeof(frame),
                         &frame_length),
                    SALTS_OK);
        check_equal(tr_raft_wire_peek_version(frame, frame_length, &version),
                    SALTS_OK);
        check_equal(version, TR_RAFT_WIRE_VERSION);
        check_equal(tr_raft_wire_decode_snapshot_chunk(
                         codec, frame, frame_length, &decoded_metadata,
                         &decoded),
                    SALTS_OK);
        check_equal(decoded_metadata.group_id, 7U);
        check_equal(decoded.data_length, sizeof(data));
        check_equal(decoded.data, data, sizeof(data));
        tr_raft_wire_codec_destroy(codec);
    }

    it("round trips a grouped 64 KiB data stream chunk and durable ack")
    {
        tr_raft_wire_codec_t *codec = NULL;
        tr_raft_wire_metadata_t metadata = metadata_for(8U, 108U);
        tr_raft_wire_metadata_t decoded_metadata;
        tr_raft_data_chunk_t chunk;
        tr_raft_data_chunk_t decoded_chunk;
        tr_raft_data_ack_t ack;
        tr_raft_data_ack_t decoded_ack;
        static uint8_t data[TR_RAFT_WIRE_MAX_DATA_CHUNK_BYTES];
        static uint8_t frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];
        size_t frame_length = 0U;

        memset(data, 0x6d, sizeof(data));
        memset(&chunk, 0, sizeof(chunk));
        chunk.from = 1U;
        chunk.to = 2U;
        chunk.term = 9U;
        chunk.stream_id = 11U;
        chunk.stream_size = sizeof(data);
        chunk.data = data;
        chunk.data_length = sizeof(data);
        chunk.done = true;
        memset(chunk.stream_digest, 0xa7, sizeof(chunk.stream_digest));

        check_equal(tr_raft_wire_codec_create(&codec), SALTS_OK);
        check_equal(tr_raft_wire_encode_data_chunk(
                         codec, &metadata, &chunk, frame, sizeof(frame),
                         &frame_length),
                    SALTS_OK);
        check_equal(tr_raft_wire_decode_data_chunk(
                         codec, frame, frame_length, &decoded_metadata,
                         &decoded_chunk),
                    SALTS_OK);
        check_equal(decoded_metadata.group_id, 8U);
        check_equal(decoded_chunk.stream_id, chunk.stream_id);
        check_equal(decoded_chunk.data_length, sizeof(data));
        check_equal(decoded_chunk.data, data, sizeof(data));

        metadata.message_id++;
        memset(&ack, 0, sizeof(ack));
        ack.from = 2U;
        ack.to = 1U;
        ack.term = chunk.term;
        ack.stream_id = chunk.stream_id;
        ack.stream_size = chunk.stream_size;
        ack.next_offset = chunk.stream_size;
        ack.accepted = true;
        ack.durable = true;
        memcpy(ack.stream_digest, chunk.stream_digest,
               sizeof(ack.stream_digest));
        check_equal(tr_raft_wire_encode_data_ack(
                         codec, &metadata, &ack, frame, sizeof(frame),
                         &frame_length),
                    SALTS_OK);
        check_equal(tr_raft_wire_decode_data_ack(
                         codec, frame, frame_length, &decoded_metadata,
                         &decoded_ack),
                    SALTS_OK);
        check_equal(decoded_metadata.group_id, 8U);
        check_true(decoded_ack.accepted);
        check_true(decoded_ack.durable);
        tr_raft_wire_codec_destroy(codec);
    }
}
