#ifndef TURBORAFT_RAFT_CNET_CHANNEL_H
#define TURBORAFT_RAFT_CNET_CHANNEL_H

#include <turboraft/raft_cnet_identity.h>
#include <turboraft/raft_transport.h>
#include <turboraft/raft_runtime.h>
#include <cnet/cnet.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * One owner-local authenticated peer connection. CNet owns I/O, TLS, native
 * handles, send/receive credits and callbacks. The channel owns ONLY its Raft
 * HELLO/ACK exchange and a post-negotiation wire transport session.
 *
 * No additional network worker, Reactor, command queue, automatic reconnect,
 * codec downgrade or Plugin lease is created. All public calls and callbacks
 * belong to the same CNet progress owner; the host keeps client, identity
 * policy and payload callback context alive through the terminal callback.
 */
typedef struct tr_raft_cnet_channel tr_raft_cnet_channel_t;

typedef enum tr_raft_cnet_channel_phase {
    TR_RAFT_CNET_CHANNEL_CREATED = 0,
    TR_RAFT_CNET_CHANNEL_TLS,
    TR_RAFT_CNET_CHANNEL_NEGOTIATING,
    TR_RAFT_CNET_CHANNEL_ACTIVE,
    TR_RAFT_CNET_CHANNEL_CLOSING,
    TR_RAFT_CNET_CHANNEL_CLOSED,
    TR_RAFT_CNET_CHANNEL_FAILED
} tr_raft_cnet_channel_phase_t;

typedef struct tr_raft_cnet_channel_config {
    cnet_client *client; /* borrowed final CNet owner, not a worker */
    const tr_raft_cnet_identity_policy_t *identity; /* immutable borrowed */
    tr_raft_handshake_config_t handshake; /* copied local node+cluster contract */
    uint64_t first_outbound_message_id; /* nonzero; never reset on this channel */
    tr_raft_transport_payload_handler_fn on_payload; /* owner-thread callback */
    void *payload_context; /* borrowed for entire lifetime */
} tr_raft_cnet_channel_config_t;

typedef struct tr_raft_cnet_channel_status {
    tr_raft_cnet_channel_phase_t phase;
    tr_raft_node_id_t authenticated_peer_node_id;
    uint64_t handshake_packets_sent;
    uint64_t payloads_admitted;
    uint64_t payloads_received;
    /* Owner-local logical-write settlement, distinct from remote delivery.
     * CNet retains payload buffers; this is bounded metadata, NOT a queue. */
    uint64_t payloads_completed;
    uint64_t payloads_canceled;
    size_t payload_writes_pending;
    int last_error;
    int terminal;
} tr_raft_cnet_channel_status_t;

/* Validates the immutable policy and local HELLO before side effects. */
int tr_raft_cnet_channel_create(
    const tr_raft_cnet_channel_config_t *config,
    tr_raft_cnet_channel_t **out_channel);

/* Install this observer on exactly one TLS client connect or accepted stream.
 * Never install on plaintext or attach the same channel to multiple sockets.
 */
cnet_observer tr_raft_cnet_channel_observer(tr_raft_cnet_channel_t *channel);

/* Call after successful CNet connect/accept admission. An already delivered
 * CONNECTING/CONNECTED callback may have attached the same handle. */
int tr_raft_cnet_channel_attach(tr_raft_cnet_channel_t *channel,
                                cnet_connection connection);

/* Only ACTIVE after verified TLS peer leaf, exact Node/Cluster HELLO, and
 * reciprocal ACK. The callback receives borrowed payload only during call.
 * Unknown group routing is delegated to the caller's exact on_payload policy.
 */
int tr_raft_cnet_channel_get_status(
    const tr_raft_cnet_channel_t *channel,
    tr_raft_cnet_channel_status_t *out_status);

/* Enqueue one wire payload directly into CNet's bounded owner-local send.
 * On success, exactly one pending logical write is owned by CNet; the matching
 * on_send marks it completed, or the terminal Channel callback marks it
 * canceled. These counters never imply remote delivery/commit/WAL fsync.
 * Rejected admission creates no pending write, no automatic retry, and no
 * second TurboRaft queue. All calls/status access are owner-thread only.
 */
int tr_raft_cnet_channel_send(
    tr_raft_cnet_channel_t *channel,
    const tr_raft_transport_payload_t *payload);

/*
 * ACE typed Adapter to the existing Raft Runtime/Service transport callback.
 * Caller owns the address-stable binding, the active CNet channel and its
 * final CNet owner until the borrowing Runtime/Service has stopped.
 *
 * No new queue, background retry or second transport authority. An unready
 * handshake or CNet-local capacity rejection reports SALTS_ENOSPC: the Raft
 * Service already preserves exactly the unsent peer suffix and may decide
 * when to retry. Other errors pass through unchanged.
 */
typedef struct tr_raft_cnet_channel_group_binding {
    tr_raft_cnet_channel_t *channel;
    tr_raft_group_id_t group_id;
} tr_raft_cnet_channel_group_binding_t;

/* Binds once; a preexisting enqueue remains untouched (SALTS_EALREADY).
 * Only enqueue/context are assigned; any Snapshot adapter is preserved. */
int tr_raft_cnet_channel_group_transport_bind(
    tr_raft_cnet_channel_group_binding_t *binding,
    tr_raft_transport_t *transport);

/* Idempotent request-close. Host must continue progressing CNet until the
 * terminal callback before channel_destroy; this does not stop CNet itself.
 */
int tr_raft_cnet_channel_stop(tr_raft_cnet_channel_t *channel);

/* Refuses to release callbacks/borrowed state while CNet still owns this
 * connection. No implicit wait or force-close. NULL is a successful no-op. */
int tr_raft_cnet_channel_destroy(tr_raft_cnet_channel_t *channel);

#ifdef __cplusplus
}
#endif

#endif /* TURBORAFT_RAFT_CNET_CHANNEL_H */
