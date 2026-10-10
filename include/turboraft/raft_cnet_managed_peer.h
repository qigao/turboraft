#ifndef TURBORAFT_RAFT_CNET_MANAGED_PEER_H
#define TURBORAFT_RAFT_CNET_MANAGED_PEER_H

#include <turboraft/raft_cnet_channel.h>
#include <cnet/managed_dial.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * One owner-local Raft peer with explicit transport-only reconnect policy.
 *
 * Exactly one Salts::CNetManager owns the physical connection records for
 * all peers sharing a CNet owner; one Salts::CNetManagedDial owns each link's
 * bounded reconnect state. No Raft-owned timer, retry loop, reactor, queue,
 * raw-runtime persistence, or duplicated Plugin/module lease is created.
 *
 * The Channel configuration REQUIRES host_module_generation != 0, issued
 * by the stable Component/Plugin domain outside any reloadable provider DSO.
 * A new module load needs a fresh host generation even when the OS reuses
 * the same DSO mapping/addresses. Failure to provide an epoch fails create.
 *
 * This object remains address-stable on the SAME owner thread until destroy.
 * Its manager/client, TLS credentials and the caller-owned fingerprint lists
 * must remain valid until manager recycle and peer destruction. Never call
 * peer APIs from CNet callbacks or a foreign owner thread.
 */
typedef struct tr_raft_cnet_managed_peer tr_raft_cnet_managed_peer_t;

typedef struct tr_raft_cnet_managed_peer_config {
    cnet_manager *manager; /* already initialized on this CNet owner */
    tr_raft_cnet_channel_config_t channel;
    tr_raft_node_id_t expected_peer_node_id; /* required exact remote ID */
    const char *uri; /* copied by cnet_managed_dial_init; TLS URI only */
    const cnet_tls_client_config *tls; /* borrowed policy/strings for every dial */
    cnet_reconnect_config reconnect; /* finite attempts/deadline/backoff */
    uint64_t recovery_episode_ms; /* absolute budget after READY disconnect */
} tr_raft_cnet_managed_peer_config_t;

typedef struct tr_raft_cnet_managed_peer_status {
    cnet_managed_dial_snapshot dial;
    tr_raft_cnet_channel_status_t channel;
    tr_raft_node_id_t peer_node_id;
    size_t connections_started;
    size_t protocol_ready_count;
    size_t security_rejections;
    int stopped;
} tr_raft_cnet_managed_peer_status_t;

/* Validates the entire identity policy, then binds exactly ONE expected peer
 * without ranking among other nodes; no sockets are opened by create. */
int tr_raft_cnet_managed_peer_create(
    const tr_raft_cnet_managed_peer_config_t *config,
    tr_raft_cnet_managed_peer_t **out_peer);

/* Caller must separately progress CNet and cnet_manager_advance(), then call
 * this on the same owner using actual monotonic milliseconds. An accepted
 * connect is asynchronous; only a fully verified TLS+HELLO+ACK channel is
 * marked protocol READY in Salts reconnect state. EBUSY may carry wait_ms;
 * it never sleeps or automatically replays any Raft operation.
 *
 * A manager record and all its callbacks must recycle before any reconnect.
 * Security/identity failures seal the reconnect episode with no downgrade.
 */
int tr_raft_cnet_managed_peer_advance(
    tr_raft_cnet_managed_peer_t *peer,
    uint64_t now_ms,
    uint64_t *out_wait_ms);

/* Direct bounded send only after protocol READY. EBUSY/ENOBUFS convert to
 * SALTS_ENOSPC for the existing Raft Service's per-peer suffix accounting.
 * Never silently replay the original request after connection loss. */
int tr_raft_cnet_managed_peer_send(
    tr_raft_cnet_managed_peer_t *peer,
    const tr_raft_transport_payload_t *payload);

/* Caller-driven CNet Owner only. Capture the currently authenticated
 * Channel for a specific inbound Group, then embed this VALUE in the existing
 * Multicore request/completion. A reconnect replaces the Channel and
 * invalidates all prior generations without mutating queued completions.
 */
int tr_raft_cnet_managed_peer_capture_reply_origin(
    tr_raft_cnet_managed_peer_t *peer, tr_raft_group_id_t group_id,
    tr_raft_transport_reply_origin_t *out_origin);

/* No ACK enqueue on an old/stopped/unready generation. Host must already
 * have taken the corresponding Group completion and must not replay it.
 */
int tr_raft_cnet_managed_peer_send_chunk_completion(
    tr_raft_cnet_managed_peer_t *peer,
    const tr_raft_multicore_completion_t *completion);

/*
 * Stable Raft Service transport binding: unlike a single Channel binding,
 * its self remains valid across a managed connection generation change.
 * Caller owns this address-stable group record and the ManagedPeer until
 * Runtime/Service teardown. No hidden queue or settlement retry is created.
 * Snapshot callback/context remain caller-owned and are never overwritten.
 */
typedef struct tr_raft_cnet_managed_group_binding {
    tr_raft_cnet_managed_peer_t *peer;
    tr_raft_group_id_t group_id;
} tr_raft_cnet_managed_group_binding_t;

int tr_raft_cnet_managed_group_transport_bind(
    tr_raft_cnet_managed_group_binding_t *binding,
    tr_raft_transport_t *transport);

int tr_raft_cnet_managed_peer_get_status(
    tr_raft_cnet_managed_peer_t *peer,
    tr_raft_cnet_managed_peer_status_t *out_status);

/* Seal only this peer's dial; caller continues normal CNet/Manager progress
 * until the native connection has a terminal callback and Manager recycles. */
int tr_raft_cnet_managed_peer_stop(tr_raft_cnet_managed_peer_t *peer);
/* EBUSY until an explicit stop and a drained Manager record. No force close. */
int tr_raft_cnet_managed_peer_destroy(tr_raft_cnet_managed_peer_t *peer);

#ifdef __cplusplus
}
#endif

#endif /* TURBORAFT_RAFT_CNET_MANAGED_PEER_H */
