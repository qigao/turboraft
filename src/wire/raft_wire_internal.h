#ifndef TURBORAFT_RAFT_WIRE_INTERNAL_H
#define TURBORAFT_RAFT_WIRE_INTERNAL_H

#include <turboraft/raft_wire_codec.h>

#include <stddef.h>
#include <stdint.h>

/*
 * Encodes the wire envelope plus the fixed DATA_CHUNK metadata only.
 *
 * out_prefix_length is the number of bytes written to output.
 * out_frame_length is the complete contiguous wire-frame length including the
 * borrowed chunk payload that follows this prefix on the stream.
 */
int tr_raft_wire_encode_data_chunk_prefix(
    tr_raft_wire_codec_t *codec,
    const tr_raft_wire_metadata_t *metadata,
    const tr_raft_data_chunk_t *chunk,
    uint8_t *output,
    size_t output_capacity,
    size_t *out_prefix_length,
    size_t *out_frame_length);

#endif
