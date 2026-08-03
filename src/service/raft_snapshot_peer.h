#ifndef TURBORAFT_RAFT_SNAPSHOT_PEER_H
#define TURBORAFT_RAFT_SNAPSHOT_PEER_H

#include "raft_snapshot_coordinator.h"

#include <turboraft/raft_coronet_transport.h>

typedef struct tr_raft_snapshot_peer tr_raft_snapshot_peer_t;

typedef int (*tr_raft_snapshot_payload_enqueue_fn)(
    void *context,
    const tr_raft_coronet_payload_t *payload);

typedef struct tr_raft_snapshot_peer_config {
    tr_raft_node_id_t self_id;
    tr_raft_node_id_t peer_id;
    size_t max_snapshot_bytes;
    tr_raft_snapshot_payload_enqueue_fn enqueue;
    void *enqueue_context;
} tr_raft_snapshot_peer_config_t;

int tr_raft_snapshot_peer_create(
    const tr_raft_snapshot_peer_config_t *config,
    tr_raft_snapshot_peer_t **out_peer);

void tr_raft_snapshot_peer_destroy(tr_raft_snapshot_peer_t *peer);

int tr_raft_snapshot_peer_begin(
    tr_raft_snapshot_peer_t *peer,
    tr_raft_term_t leader_term,
    tr_raft_index_t snapshot_index,
    tr_raft_term_t snapshot_term,
    const tr_raft_conf_t *configuration,
    const uint8_t *data,
    size_t size);

int tr_raft_snapshot_peer_handle_payload(
    tr_raft_snapshot_peer_t *peer,
    const tr_raft_coronet_payload_t *payload);

int tr_raft_snapshot_peer_resume(tr_raft_snapshot_peer_t *peer);

int tr_raft_snapshot_peer_get_status(
    const tr_raft_snapshot_peer_t *peer,
    tr_raft_snapshot_sender_status_t *out_status);

#endif
