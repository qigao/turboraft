#ifndef TURBORAFT_RAFT_CNET_PEER_DIRECTORY_H
#define TURBORAFT_RAFT_CNET_PEER_DIRECTORY_H

#include <turboraft/raft_cnet_managed_peer.h>
#include <turboraft/raft_multicore.h>
#include <cnet/owner_placement.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TR_RAFT_CNET_PEER_DIRECTORY_VERSION 1U

/*
 * One CNet progress Owner's immutable, bounded Node ID routing table.
 *
 * This is an ACE Strategy/Adapter composition, NOT a new registry, scheduler,
 * connection pool, queue or owner thread. Entries and authorized group IDs are
 * borrowed from the caller. The caller never changes their contents while
 * an active Service or Directory borrows them.
 *
 * A peer's CNet final-owner placement is STRICT_KEY using its exact Node ID
 * and cnet_owner_placement_choose(). That choice is immutable until a planned
 * stop/reconfigure; no fallback to another owner on exhausted credits. Raft
 * group-to-Raft-owner placement is independent of this network Owner. The host
 * must deliver cross-owner messages through an explicitly bounded handoff.
 *
 * Each entry holds a live ManagedPeer for THIS owner, not a native socket or
 * a recycled per-generation Channel. All calls require this owner thread.
 */
typedef struct tr_raft_cnet_peer_directory_entry {
    tr_raft_node_id_t node_id; /* globally unique among policy peers */
    tr_raft_cnet_managed_peer_t *peer; /* borrowed, address-stable */
} tr_raft_cnet_peer_directory_entry_t;

typedef struct tr_raft_cnet_peer_directory_config {
    uint32_t version;
    tr_raft_node_id_t local_node_id;
    uint32_t owner_index;
    uint32_t owner_count;
    const tr_raft_cnet_identity_policy_t *identities; /* immutable borrowed */
    const tr_raft_cnet_peer_directory_entry_t *entries; /* immutable borrowed */
    size_t entry_count;
    const tr_raft_group_id_t *groups; /* explicitly admitted, immutable */
    size_t group_count;
} tr_raft_cnet_peer_directory_config_t;

typedef struct tr_raft_cnet_peer_directory {
    tr_raft_cnet_peer_directory_config_t config;
    const void *owner_thread;
    int active;
} tr_raft_cnet_peer_directory_t;

/* Zero initialize once, then preflight all Node/Owner/Group identities before
 * any message is admitted. No socket is opened or manager credit reserved.
 * Repeated init returns EALREADY; a failed init leaves the object empty.
 */
int tr_raft_cnet_peer_directory_init(
    tr_raft_cnet_peer_directory_t *directory,
    const tr_raft_cnet_peer_directory_config_t *config);

/* All failures clear *out_peer. ENOENT unknown Node, EPERM foreign Owner.
 * Borrow ends on Directory stop or ManagedPeer destruction, whichever first.
 */
int tr_raft_cnet_peer_directory_lookup(
    tr_raft_cnet_peer_directory_t *directory,
    tr_raft_node_id_t node_id,
    tr_raft_cnet_managed_peer_t **out_peer);

/* Reject unknown group or peer, mismatched Raft message from/to, and foreign
 * Owner BEFORE touching the selected ManagedPeer. A successful CNet admission
 * is not remote delivery or a WAL/applied-index commitment. No hidden retry.
 */
int tr_raft_cnet_peer_directory_send(
    tr_raft_cnet_peer_directory_t *directory,
    const tr_raft_transport_payload_t *payload);

/* Accepts a Group-owned receiver completion only if its saved ingress
 * Channel is exactly the current authenticated ManagedPeer generation.
 * Directory lookup by Node ID alone is never sufficient: reconnection
 * changes the Channel instance and old ACKs fail ECANCELED. A direct
 * incoming Channel not managed by this Directory must use the equivalent
 * Channel fence on the original CNet Owner. Never silently reroute ACKs
 * to another transport or retry on ENOSPC/ECANCELED.
 */
int tr_raft_cnet_peer_directory_send_chunk_completion(
    tr_raft_cnet_peer_directory_t *directory,
    const tr_raft_multicore_completion_t *completion);

/*
 * Verified inbound callback adapter for a single CNet Owner. This may be
 * installed directly as tr_raft_cnet_channel_config.on_payload, after the
 * caller creates a Channel and fills this address-stable context.
 *
 * The Channel supplies *CNet-authenticated* Node ID and ACTIVE proof; do NOT
 * pass caller-claimed Node IDs or certificate strings as the authority.
 * Checks source Node == authenticated peer, destination == this local Node,
 * strict final Owner assignment, and the explicitly allowed Group before
 * forwarding the borrowed payload to on_payload.
 *
 * on_payload executes on the CNet progress Owner and MUST either consume it
 * synchronously or submit a bounded owned copy to the group's distinct Raft
 * Owner. Direct cross-thread Service mutation and unbounded queuing are
 * forbidden. No extra TLS parser, Reactor or payload storage is introduced.
 */
typedef struct tr_raft_cnet_directory_ingress {
    tr_raft_cnet_peer_directory_t *directory;
    tr_raft_cnet_channel_t *channel; /* exact live TLS connection, borrowed */
    tr_raft_transport_payload_handler_fn on_payload; /* bounded Owner sink */
    void *context; /* borrowed until final callback */
} tr_raft_cnet_directory_ingress_t;

int tr_raft_cnet_peer_directory_receive(
    void *ingress_context, const tr_raft_transport_payload_t *payload);

/* One long-lived Service Transport self routes each message.to to its exact
 * preauthorized ManagedPeer. No per-generation Channel pointers are retained;
 * bind preserves the caller's Snapshot adapter and refuses a second bind.
 */
typedef struct tr_raft_cnet_directory_group_binding {
    tr_raft_cnet_peer_directory_t *directory;
    tr_raft_group_id_t group_id;
} tr_raft_cnet_directory_group_binding_t;

int tr_raft_cnet_peer_directory_transport_bind(
    tr_raft_cnet_directory_group_binding_t *binding,
    tr_raft_transport_t *transport);

/* Only after Service/Runtime has released all borrowed directory bindings and
 * each ManagedPeer will be stopped on the same owner. No implicit close. */
int tr_raft_cnet_peer_directory_destroy(
    tr_raft_cnet_peer_directory_t *directory);

#ifdef __cplusplus
}
#endif

#endif /* TURBORAFT_RAFT_CNET_PEER_DIRECTORY_H */
