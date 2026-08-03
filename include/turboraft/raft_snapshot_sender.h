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

typedef struct tr_raft_snapshot_sender_config {
    tr_raft_node_id_t self_id;
    tr_raft_node_id_t peer_id;
    size_t max_snapshot_bytes;
} tr_raft_snapshot_sender_config_t;

typedef struct tr_raft_snapshot_sender_status {
    bool active;
    bool complete;
    tr_raft_term_t leader_term;
    tr_raft_index_t snapshot_index;
    tr_raft_term_t snapshot_term;
    uint64_t snapshot_size;
    uint64_t acknowledged_offset;
} tr_raft_snapshot_sender_status_t;

/* A sender is single-owner and must not be accessed concurrently. */
int tr_raft_snapshot_sender_create(
    const tr_raft_snapshot_sender_config_t *config,
    tr_raft_snapshot_sender_t **out_sender);
void tr_raft_snapshot_sender_destroy(tr_raft_snapshot_sender_t *sender);
void tr_raft_snapshot_sender_reset(tr_raft_snapshot_sender_t *sender);

int tr_raft_snapshot_sender_begin(
    tr_raft_snapshot_sender_t *sender,
    tr_raft_term_t leader_term,
    tr_raft_index_t snapshot_index,
    tr_raft_term_t snapshot_term,
    const tr_raft_conf_t *configuration,
    const uint8_t *data,
    size_t size);

/* Repeated calls return the same chunk until a valid acknowledgement advances it. */
int tr_raft_snapshot_sender_next_chunk(
    tr_raft_snapshot_sender_t *sender,
    tr_raft_snapshot_chunk_t *out_chunk);

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
