#include "raft_wire_fuzz.h"

#include <turboraft/raft_wire_codec.h>

#include <turbo_error.h>

#include <stdlib.h>

static tr_raft_wire_codec_t *tr_fuzz_codec;
static int tr_fuzz_cleanup_registered;

static void tr_fuzz_destroy_codec(void)
{
    tr_raft_wire_codec_destroy(tr_fuzz_codec);
    tr_fuzz_codec = NULL;
}

static tr_raft_wire_codec_t *tr_fuzz_get_codec(void)
{
    if (tr_fuzz_codec != NULL) {
        return tr_fuzz_codec;
    }
    if (tr_raft_wire_codec_create(&tr_fuzz_codec) != TURBO_OK) {
        return NULL;
    }
    if (!tr_fuzz_cleanup_registered) {
        if (atexit(tr_fuzz_destroy_codec) != 0) {
            tr_raft_wire_codec_destroy(tr_fuzz_codec);
            tr_fuzz_codec = NULL;
            return NULL;
        }
        tr_fuzz_cleanup_registered = 1;
    }
    return tr_fuzz_codec;
}

static void tr_fuzz_require_round_trip(int encode_result,
                                       int decode_result)
{
    if (encode_result != TURBO_OK || decode_result != TURBO_OK) {
        abort();
    }
}

int tr_raft_wire_fuzz_one_input(const uint8_t *data, size_t size)
{
    tr_raft_wire_codec_t *codec = tr_fuzz_get_codec();
    tr_raft_wire_metadata_t metadata;
    tr_raft_wire_metadata_t round_trip_metadata;
    tr_raft_wire_payload_kind_t kind;
    tr_raft_message_t message;
    tr_raft_message_t round_trip_message;
    tr_raft_snapshot_chunk_t chunk;
    tr_raft_snapshot_chunk_t round_trip_chunk;
    tr_raft_snapshot_ack_t ack;
    tr_raft_snapshot_ack_t round_trip_ack;
    uint8_t frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];
    size_t frame_length = 0U;
    int decode_result;
    int encode_result;

    if (codec == NULL) {
        return 0;
    }

    (void)tr_raft_wire_peek_payload_kind(data, size, &kind);

    decode_result = tr_raft_wire_decode(codec, data, size, &metadata,
                                        &message);
    if (decode_result == TURBO_OK) {
        encode_result = tr_raft_wire_encode(
            codec, &metadata, &message, frame, sizeof(frame), &frame_length);
        decode_result = encode_result == TURBO_OK
                            ? tr_raft_wire_decode(
                                  codec, frame, frame_length,
                                  &round_trip_metadata, &round_trip_message)
                            : encode_result;
        tr_fuzz_require_round_trip(encode_result, decode_result);
    }

    decode_result = tr_raft_wire_decode_snapshot_chunk(
        codec, data, size, &metadata, &chunk);
    if (decode_result == TURBO_OK) {
        encode_result = tr_raft_wire_encode_snapshot_chunk(
            codec, &metadata, &chunk, frame, sizeof(frame), &frame_length);
        decode_result = encode_result == TURBO_OK
                            ? tr_raft_wire_decode_snapshot_chunk(
                                  codec, frame, frame_length,
                                  &round_trip_metadata, &round_trip_chunk)
                            : encode_result;
        tr_fuzz_require_round_trip(encode_result, decode_result);
    }

    decode_result = tr_raft_wire_decode_snapshot_ack(
        codec, data, size, &metadata, &ack);
    if (decode_result == TURBO_OK) {
        encode_result = tr_raft_wire_encode_snapshot_ack(
            codec, &metadata, &ack, frame, sizeof(frame), &frame_length);
        decode_result = encode_result == TURBO_OK
                            ? tr_raft_wire_decode_snapshot_ack(
                                  codec, frame, frame_length,
                                  &round_trip_metadata, &round_trip_ack)
                            : encode_result;
        tr_fuzz_require_round_trip(encode_result, decode_result);
    }

    return 0;
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    return tr_raft_wire_fuzz_one_input(data, size);
}
