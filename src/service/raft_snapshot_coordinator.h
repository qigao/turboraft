#ifndef TURBORAFT_RAFT_SNAPSHOT_COORDINATOR_H
#define TURBORAFT_RAFT_SNAPSHOT_COORDINATOR_H

#include <turboraft/raft_snapshot_sender.h>

typedef struct tr_raft_snapshot_coordinator
    tr_raft_snapshot_coordinator_t;

typedef int (*tr_raft_snapshot_emit_fn)(
    void *context,
    const tr_raft_snapshot_chunk_t *chunk);

typedef struct tr_raft_snapshot_coordinator_config {
    tr_raft_node_id_t self_id;
    tr_raft_node_id_t peer_id;
    uint64_t max_snapshot_bytes;
    size_t chunk_size;
    size_t max_inflight_chunks;
    tr_raft_snapshot_emit_fn emit;
    void *emit_context;
} tr_raft_snapshot_coordinator_config_t;

int tr_raft_snapshot_coordinator_create(
    const tr_raft_snapshot_coordinator_config_t *config,
    tr_raft_snapshot_coordinator_t **out_coordinator);

void tr_raft_snapshot_coordinator_destroy(
    tr_raft_snapshot_coordinator_t *coordinator);

int tr_raft_snapshot_coordinator_begin(
    tr_raft_snapshot_coordinator_t *coordinator,
    tr_raft_term_t leader_term,
    tr_raft_index_t snapshot_index,
    tr_raft_term_t snapshot_term,
    const tr_raft_conf_t *configuration,
    const uint8_t *data,
    size_t size);

int tr_raft_snapshot_coordinator_begin_source(
    tr_raft_snapshot_coordinator_t *coordinator,
    tr_raft_term_t leader_term,
    tr_raft_index_t snapshot_index,
    tr_raft_term_t snapshot_term,
    const tr_raft_conf_t *configuration,
    const tr_raft_snapshot_source_t *source);

int tr_raft_snapshot_coordinator_handle_ack(
    tr_raft_snapshot_coordinator_t *coordinator,
    const tr_raft_snapshot_ack_t *ack);

/* Re-emits the current unacknowledged chunk after transport recovery. */
int tr_raft_snapshot_coordinator_resume(
    tr_raft_snapshot_coordinator_t *coordinator);

int tr_raft_snapshot_coordinator_get_status(
    const tr_raft_snapshot_coordinator_t *coordinator,
    tr_raft_snapshot_sender_status_t *out_status);

#endif
