#ifndef TURBORAFT_RAFT_PEER_SET_H
#define TURBORAFT_RAFT_PEER_SET_H

#include "raft_membership.h"

#include <stddef.h>

#define TR_RAFT_PEER_SET_MAX_SOURCES 3U

typedef struct tr_raft_peer_set {
    tr_raft_node_id_t node_ids[TR_RAFT_MAX_MEMBERS];
    size_t count;
} tr_raft_peer_set_t;

int tr_raft_peer_set_build(
    const tr_raft_membership_t *const *memberships,
    size_t membership_count,
    tr_raft_peer_set_t *peers);

int tr_raft_peer_set_index(const tr_raft_peer_set_t *peers,
                           tr_raft_node_id_t node_id);

#endif
