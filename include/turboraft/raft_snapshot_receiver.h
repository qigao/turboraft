#ifndef TURBORAFT_RAFT_SNAPSHOT_RECEIVER_H
#define TURBORAFT_RAFT_SNAPSHOT_RECEIVER_H

#include <turboraft/raft_snapshot_stream.h>

#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct tr_raft_snapshot_receiver tr_raft_snapshot_receiver_t;

typedef int (*tr_raft_snapshot_install_fn)(
    void *context,
    tr_raft_term_t leader_term,
    tr_raft_index_t snapshot_index,
    tr_raft_term_t snapshot_term,
    const tr_raft_conf_t *configuration,
    const uint8_t *data,
    size_t size);

typedef struct tr_raft_snapshot_receiver_config {
    tr_raft_node_id_t self_id;
    /** Total logical snapshot limit; applies to both buffered and streaming paths. */
    uint64_t max_snapshot_bytes;
    /**
     * Whole-buffer compatibility cap. Required and non-zero when install is
     * used; must be zero when stream is used.
     */
    uint64_t max_buffered_snapshot_bytes;
    tr_raft_snapshot_install_fn install;
    void *install_context;
    /* Database-scale bounded-memory path; all four callbacks are required. */
    tr_raft_snapshot_stream_sink_t stream;
} tr_raft_snapshot_receiver_config_t;

typedef struct tr_raft_snapshot_receive_result {
    tr_raft_snapshot_ack_t ack;
    bool installed;
} tr_raft_snapshot_receive_result_t;

/** Creates a single-owner receiver with at most one bounded active transfer. */
int tr_raft_snapshot_receiver_create(
    const tr_raft_snapshot_receiver_config_t *config,
    tr_raft_snapshot_receiver_t **out_receiver);

void tr_raft_snapshot_receiver_destroy(
    tr_raft_snapshot_receiver_t *receiver);

/** Drops an incomplete transfer without changing the installed snapshot. */
void tr_raft_snapshot_receiver_reset(
    tr_raft_snapshot_receiver_t *receiver);

/**
 * Consumes one validated chunk and returns the acknowledgement to enqueue.
 * On a protocol, digest, or install failure the acknowledgement is rejected.
 */
int tr_raft_snapshot_receiver_handle(
    tr_raft_snapshot_receiver_t *receiver,
    const tr_raft_snapshot_chunk_t *chunk,
    tr_raft_snapshot_receive_result_t *out_result);

#ifdef __cplusplus
}
#endif

#endif
