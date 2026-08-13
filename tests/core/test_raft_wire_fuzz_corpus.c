#include "raft_wire_fuzz.h"

#include <turboraft/raft_wire_codec.h>

#include <tinytest.h>
#include <turbo_error.h>

#include <string.h>

#define WIRE_FUZZ_RANDOM_CASES 128U

static void wire_fuzz_exercise_frame(const uint8_t *frame,
                                     size_t frame_length)
{
    uint8_t mutation[TR_RAFT_WIRE_MAX_FRAME_SIZE + 1U];
    size_t index;

    check_int_eq(tr_raft_wire_fuzz_one_input(NULL, 0U), 0);
    for (index = 0U; index <= frame_length; ++index) {
        check_int_eq(tr_raft_wire_fuzz_one_input(frame, index), 0);
    }
    for (index = 0U; index < frame_length; ++index) {
        memcpy(mutation, frame, frame_length);
        mutation[index] ^= (uint8_t)(UINT8_C(0xa5) + index);
        check_int_eq(tr_raft_wire_fuzz_one_input(mutation, frame_length), 0);
    }
    memcpy(mutation, frame, frame_length);
    mutation[frame_length] = UINT8_C(0x5a);
    check_int_eq(tr_raft_wire_fuzz_one_input(mutation, frame_length + 1U), 0);
}

static uint32_t wire_fuzz_random(uint32_t *state)
{
    uint32_t value = *state;

    value ^= value << 13U;
    value ^= value >> 17U;
    value ^= value << 5U;
    *state = value;
    return value;
}

spec("raft wire fuzz corpus")
{
    it("survives valid frames, truncations, mutations, and bounded noise")
    {
        tr_raft_wire_codec_t *codec = NULL;
        tr_raft_wire_metadata_t metadata;
        tr_raft_message_t message;
        tr_raft_snapshot_chunk_t chunk;
        tr_raft_snapshot_ack_t ack;
        uint8_t frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];
        uint8_t noise[TR_RAFT_WIRE_MAX_FRAME_SIZE + 1U];
        size_t frame_length = 0U;
        size_t index;
        uint32_t random_state = UINT32_C(0x6d2b79f5);

        memset(&metadata, 0, sizeof(metadata));
        metadata.message_id = 1U;
        check_int_eq(tr_raft_wire_codec_create(&codec), TURBO_OK);

        memset(&message, 0, sizeof(message));
        message.type = TR_RAFT_MSG_APPEND_REQUEST;
        message.from = 1U;
        message.to = 2U;
        message.term = 3U;
        message.previous_log_index = 4U;
        message.previous_log_term = 2U;
        message.entry_count = 1U;
        message.entries[0].index = 5U;
        message.entries[0].term = 3U;
        message.entries[0].command_id = 7U;
        message.entries[0].data_length = 4U;
        memcpy(message.entries[0].data, "seed", 4U);
        check_int_eq(tr_raft_wire_encode(codec, &metadata, &message, frame,
                                         sizeof(frame), &frame_length),
                     TURBO_OK);
        wire_fuzz_exercise_frame(frame, frame_length);

        memset(&chunk, 0, sizeof(chunk));
        chunk.from = 1U;
        chunk.to = 2U;
        chunk.term = 3U;
        chunk.snapshot_index = 8U;
        chunk.snapshot_term = 2U;
        chunk.snapshot_size = 16U;
        chunk.data_length = 16U;
        chunk.done = true;
        chunk.has_configuration = true;
        chunk.configuration.phase = TR_RAFT_CONF_FINAL;
        chunk.configuration.member_count = 1U;
        chunk.configuration.members[0].node_id = 1U;
        chunk.configuration.members[0].roles =
            TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER;
        memset(chunk.snapshot_digest, 0x3c, sizeof(chunk.snapshot_digest));
        {
            static const uint8_t chunk_data[16] = {
                0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a,
                0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a, 0x5a};
            chunk.data = chunk_data;
        }
        check_int_eq(tr_raft_wire_encode_snapshot_chunk(
                         codec, &metadata, &chunk, frame, sizeof(frame),
                         &frame_length),
                     TURBO_OK);
        wire_fuzz_exercise_frame(frame, frame_length);

        memset(&ack, 0, sizeof(ack));
        ack.from = 2U;
        ack.to = 1U;
        ack.term = 3U;
        ack.snapshot_index = 8U;
        ack.snapshot_size = 16U;
        ack.next_offset = 16U;
        ack.accepted = true;
        memcpy(ack.snapshot_digest, chunk.snapshot_digest,
               sizeof(ack.snapshot_digest));
        check_int_eq(tr_raft_wire_encode_snapshot_ack(
                         codec, &metadata, &ack, frame, sizeof(frame),
                         &frame_length),
                     TURBO_OK);
        wire_fuzz_exercise_frame(frame, frame_length);

        for (index = 0U; index < WIRE_FUZZ_RANDOM_CASES; ++index) {
            size_t length = wire_fuzz_random(&random_state) % sizeof(noise);
            size_t byte_index;

            for (byte_index = 0U; byte_index < length; ++byte_index) {
                noise[byte_index] = (uint8_t)wire_fuzz_random(&random_state);
            }
            check_int_eq(tr_raft_wire_fuzz_one_input(noise, length), 0);
        }

        tr_raft_wire_codec_destroy(codec);
    }
}
