#ifndef TURBORAFT_RAFT_SNAPSHOT_SENDER_H
#define TURBORAFT_RAFT_SNAPSHOT_SENDER_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include <turboraft/raft_wire_codec.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct tr_raft_snapshot_sender tr_raft_snapshot_sender_t;

typedef int (*tr_raft_snapshot_source_read_at_fn)(
    void *context,
    uint64_t offset,
    uint8_t *buffer,
    size_t capacity,
    size_t *out_size);

typedef void (*tr_raft_snapshot_source_release_fn)(void *context);

typedef struct tr_raft_snapshot_source {
    void *context;
    uint64_t size;
    uint8_t digest[TR_RAFT_WIRE_SNAPSHOT_DIGEST_SIZE];
    tr_raft_snapshot_source_read_at_fn read_at;
    tr_raft_snapshot_source_release_fn release;
} tr_raft_snapshot_source_t;

#define TR_RAFT_SNAPSHOT_MAX_INFLIGHT_CHUNKS 4U
#define TR_RAFT_SNAPSHOT_RECOMMENDED_INFLIGHT_CHUNKS 4U

typedef struct tr_raft_snapshot_sender_config {
    tr_raft_node_id_t self_id;
    tr_raft_node_id_t peer_id;
    size_t max_snapshot_bytes;
    /* Both limits are required and validated; zero is rejected. */
    size_t chunk_size;
    size_t max_inflight_chunks;
} tr_raft_snapshot_sender_config_t;

typedef struct tr_raft_snapshot_sender_status {
    bool active;
    bool complete;
    tr_raft_term_t leader_term;
    tr_raft_index_t snapshot_index;
    tr_raft_term_t snapshot_term;
    uint64_t snapshot_size;
    uint64_t acknowledged_offset;
    uint64_t next_offset;
    size_t inflight_chunks;
    size_t max_inflight_chunks;
} tr_raft_snapshot_sender_status_t;

/* A sender is single-owner and must not be accessed concurrently. */
int tr_raft_snapshot_sender_create(
    const tr_raft_snapshot_sender_config_t *config,
    tr_raft_snapshot_sender_t **out_sender);
void tr_raft_snapshot_sender_destroy(tr_raft_snapshot_sender_t *sender);
void tr_raft_snapshot_sender_reset(tr_raft_snapshot_sender_t *sender);

/** Convenience helper for small snapshots; copies the complete payload. */
int tr_raft_snapshot_sender_begin(
    tr_raft_snapshot_sender_t *sender,
    tr_raft_term_t leader_term,
    tr_raft_index_t snapshot_index,
    tr_raft_term_t snapshot_term,
    const tr_raft_conf_t *configuration,
    const uint8_t *data,
    size_t size);

/**
 * Starts a database-scale transfer and takes ownership of source on success.
 * read_at is called with bounded chunk-sized buffers. release is invoked at
 * final acknowledgement, reset, or destroy when non-NULL.
 */
int tr_raft_snapshot_sender_begin_source(
    tr_raft_snapshot_sender_t *sender,
    tr_raft_term_t leader_term,
    tr_raft_index_t snapshot_index,
    tr_raft_term_t snapshot_term,
    const tr_raft_conf_t *configuration,
    const tr_raft_snapshot_source_t *source);


/* Claims the next chunk; returns EBUSY while the bounded window is full. */
int tr_raft_snapshot_sender_next_chunk(
    tr_raft_snapshot_sender_t *sender,
    tr_raft_snapshot_chunk_t *out_chunk);

/* Cancels the most recent claim when the transport rejected it synchronously. */
int tr_raft_snapshot_sender_cancel_chunk(
    tr_raft_snapshot_sender_t *sender,
    uint64_t snapshot_offset);

/* Drops speculative claims so resume can retransmit from cumulative ACK. */
int tr_raft_snapshot_sender_prepare_resume(
    tr_raft_snapshot_sender_t *sender);

int tr_raft_snapshot_sender_acknowledge(
    tr_raft_snapshot_sender_t *sender,
    const tr_raft_snapshot_ack_t *ack);

int tr_raft_snapshot_sender_get_status(
    const tr_raft_snapshot_sender_t *sender,
    tr_raft_snapshot_sender_status_t *out_status);

#ifdef __cplusplus
}
#endif

#endif
