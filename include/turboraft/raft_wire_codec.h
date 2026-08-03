#ifndef TURBORAFT_RAFT_WIRE_CODEC_H
#define TURBORAFT_RAFT_WIRE_CODEC_H

#include <turboraft/raft_core.h>

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TR_RAFT_WIRE_MIN_VERSION 2U
#define TR_RAFT_WIRE_VERSION 3U
#define TR_RAFT_WIRE_SNAPSHOT_VERSION 4U
#define TR_RAFT_WIRE_MAX_VERSION TR_RAFT_WIRE_SNAPSHOT_VERSION
#define TR_RAFT_WIRE_HEADER_SIZE 40U
#define TR_RAFT_WIRE_MAX_PAYLOAD_SIZE 8192U
#define TR_RAFT_WIRE_SNAPSHOT_DIGEST_SIZE 32U
#define TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES 512U
#define TR_RAFT_WIRE_MAX_SNAPSHOT_BYTES (64U * 1024U * 1024U)
#define TR_RAFT_WIRE_MAX_FRAME_SIZE \
    (TR_RAFT_WIRE_HEADER_SIZE + TR_RAFT_WIRE_MAX_PAYLOAD_SIZE)

typedef struct tr_raft_wire_codec tr_raft_wire_codec_t;

typedef struct tr_raft_cluster_id {
    uint8_t bytes[16];
} tr_raft_cluster_id_t;

typedef struct tr_raft_wire_metadata {
    tr_raft_cluster_id_t cluster_id;
    uint64_t message_id;
} tr_raft_wire_metadata_t;

typedef enum tr_raft_wire_payload_kind {
    TR_RAFT_WIRE_PAYLOAD_RAFT = 1,
    TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK = 2,
    TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_ACK = 3
} tr_raft_wire_payload_kind_t;

/** Validates the envelope and returns its payload discriminator. */
int tr_raft_wire_peek_payload_kind(
    const uint8_t *frame,
    size_t frame_length,
    tr_raft_wire_payload_kind_t *out_kind);

/** Validates the envelope and returns its wire version. */
int tr_raft_wire_peek_version(
    const uint8_t *frame,
    size_t frame_length,
    uint16_t *out_version);

typedef struct tr_raft_snapshot_chunk {
    tr_raft_node_id_t from;
    tr_raft_node_id_t to;
    tr_raft_term_t term;
    tr_raft_index_t snapshot_index;
    tr_raft_term_t snapshot_term;
    uint64_t snapshot_offset;
    uint64_t snapshot_size;
    uint8_t snapshot_digest[TR_RAFT_WIRE_SNAPSHOT_DIGEST_SIZE];
    bool has_configuration;
    tr_raft_conf_t configuration;
    size_t data_length;
    uint8_t data[TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES];
    bool done;
} tr_raft_snapshot_chunk_t;

typedef struct tr_raft_snapshot_ack {
    tr_raft_node_id_t from;
    tr_raft_node_id_t to;
    tr_raft_term_t term;
    tr_raft_index_t snapshot_index;
    uint64_t snapshot_size;
    uint64_t next_offset;
    uint8_t snapshot_digest[TR_RAFT_WIRE_SNAPSHOT_DIGEST_SIZE];
    bool accepted;
} tr_raft_snapshot_ack_t;

int tr_raft_wire_codec_create(tr_raft_wire_codec_t **out_codec);

void tr_raft_wire_codec_destroy(tr_raft_wire_codec_t *codec);

/** Encodes one complete frame into caller-owned bounded storage. */
int tr_raft_wire_encode(tr_raft_wire_codec_t *codec,
                        const tr_raft_wire_metadata_t *metadata,
                        const tr_raft_message_t *message,
                        uint8_t *output,
                        size_t output_capacity,
                        size_t *output_length);

/** Explicit version encoder for negotiated rolling upgrades. */
int tr_raft_wire_encode_version(tr_raft_wire_codec_t *codec,
                                uint16_t wire_version,
                                const tr_raft_wire_metadata_t *metadata,
                                const tr_raft_message_t *message,
                                uint8_t *output,
                                size_t output_capacity,
                                size_t *output_length);

/** Decodes exactly one frame; trailing bytes are rejected. */
int tr_raft_wire_decode(tr_raft_wire_codec_t *codec,
                        const uint8_t *frame,
                        size_t frame_length,
                        tr_raft_wire_metadata_t *metadata,
                        tr_raft_message_t *message);

int tr_raft_wire_encode_snapshot_chunk(
    tr_raft_wire_codec_t *codec,
    const tr_raft_wire_metadata_t *metadata,
    const tr_raft_snapshot_chunk_t *chunk,
    uint8_t *output,
    size_t output_capacity,
    size_t *output_length);

int tr_raft_wire_decode_snapshot_chunk(
    tr_raft_wire_codec_t *codec,
    const uint8_t *frame,
    size_t frame_length,
    tr_raft_wire_metadata_t *metadata,
    tr_raft_snapshot_chunk_t *chunk);

int tr_raft_wire_encode_snapshot_ack(
    tr_raft_wire_codec_t *codec,
    const tr_raft_wire_metadata_t *metadata,
    const tr_raft_snapshot_ack_t *ack,
    uint8_t *output,
    size_t output_capacity,
    size_t *output_length);

int tr_raft_wire_decode_snapshot_ack(
    tr_raft_wire_codec_t *codec,
    const uint8_t *frame,
    size_t frame_length,
    tr_raft_wire_metadata_t *metadata,
    tr_raft_snapshot_ack_t *ack);

#ifdef __cplusplus
}
#endif

#endif
