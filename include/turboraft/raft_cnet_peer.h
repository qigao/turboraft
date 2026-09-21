#ifndef TURBORAFT_RAFT_CNET_PEER_H
#define TURBORAFT_RAFT_CNET_PEER_H

#include <turboraft/raft_transport.h>

#include <cnet/cnet.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TR_RAFT_CNET_MAX_QUEUE_CAPACITY 65536U

typedef struct tr_raft_cnet_peer tr_raft_cnet_peer_t;

typedef struct tr_raft_cnet_peer_config {
    cnet_client *client;
    tr_raft_transport_session_config_t transport;
    size_t outbound_queue_capacity;
} tr_raft_cnet_peer_config_t;

typedef struct tr_raft_cnet_peer_status {
    cnet_connection_state connection_state;
    size_t outbound_queue_capacity;
    size_t queued_payload_count;
    uint64_t frames_admitted;
    uint64_t bytes_admitted;
    uint64_t completed_writes;
    int write_pending;
    int stopping;
    int last_error;
} tr_raft_cnet_peer_status_t;

/**
 * Creates a bounded adapter around a borrowed caller-driven CNet client.
 * Use the returned observer in cnet_connect() or cnet_listener_accept*().
 */
int tr_raft_cnet_peer_create(const tr_raft_cnet_peer_config_t *config,
                             tr_raft_cnet_peer_t **out_peer);
cnet_observer tr_raft_cnet_peer_observer(tr_raft_cnet_peer_t *peer);

/** Copies a payload into the peer FIFO. */
int tr_raft_cnet_peer_enqueue_payload(
    tr_raft_cnet_peer_t *peer,
    const tr_raft_transport_payload_t *payload);
int tr_raft_cnet_peer_enqueue_group(
    tr_raft_cnet_peer_t *peer,
    tr_raft_group_id_t group_id,
    const tr_raft_message_t *message);

/** Admits at most one copied CNet write; progress remains caller-driven. */
int tr_raft_cnet_peer_step(tr_raft_cnet_peer_t *peer);

/** Begins close; poll the borrowed CNet client through its terminal callback. */
int tr_raft_cnet_peer_stop(tr_raft_cnet_peer_t *peer);
int tr_raft_cnet_peer_destroy(tr_raft_cnet_peer_t *peer);
int tr_raft_cnet_peer_get_status(const tr_raft_cnet_peer_t *peer,
                                 tr_raft_cnet_peer_status_t *out_status);

#ifdef __cplusplus
}
#endif

#endif
