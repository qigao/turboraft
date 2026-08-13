#include "raft_snapshot_peer.h"

#include <turbo_error.h>

#include <stdlib.h>
#include <string.h>

struct tr_raft_snapshot_peer {
    tr_raft_snapshot_coordinator_t *coordinator;
    tr_raft_snapshot_payload_enqueue_fn enqueue;
    void *enqueue_context;
};

static int tr_snapshot_peer_emit(
    void *context,
    const tr_raft_snapshot_chunk_t *chunk)
{
    tr_raft_snapshot_peer_t *peer = (tr_raft_snapshot_peer_t *) context;
    tr_raft_coronet_payload_t payload;

    if (peer == NULL || chunk == NULL) {
        return TURBO_EINVAL;
    }
    memset(&payload, 0, sizeof(payload));
    payload.kind = TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK;
    payload.data.snapshot_chunk = *chunk;
    return peer->enqueue(peer->enqueue_context, &payload);
}

int tr_raft_snapshot_peer_create(
    const tr_raft_snapshot_peer_config_t *config,
    tr_raft_snapshot_peer_t **out_peer)
{
    tr_raft_snapshot_coordinator_config_t coordinator_config;
    tr_raft_snapshot_peer_t *peer;
    int result;

    if (config == NULL || out_peer == NULL || config->enqueue == NULL) {
        return TURBO_EINVAL;
    }
    *out_peer = NULL;
    memset(&coordinator_config, 0, sizeof(coordinator_config));
    peer = (tr_raft_snapshot_peer_t *) calloc(1U, sizeof(*peer));
    if (peer == NULL) {
        return TURBO_ENOMEM;
    }
    peer->enqueue = config->enqueue;
    peer->enqueue_context = config->enqueue_context;

    coordinator_config.self_id = config->self_id;
    coordinator_config.peer_id = config->peer_id;
    coordinator_config.max_snapshot_bytes = config->max_snapshot_bytes;
    coordinator_config.chunk_size = config->chunk_size;
    coordinator_config.max_inflight_chunks = config->max_inflight_chunks;
    coordinator_config.emit = tr_snapshot_peer_emit;
    coordinator_config.emit_context = peer;
    result = tr_raft_snapshot_coordinator_create(&coordinator_config,
                                                 &peer->coordinator);
    if (result != TURBO_OK) {
        free(peer);
        return result;
    }
    *out_peer = peer;
    return TURBO_OK;
}

void tr_raft_snapshot_peer_destroy(tr_raft_snapshot_peer_t *peer)
{
    if (peer == NULL) {
        return;
    }
    tr_raft_snapshot_coordinator_destroy(peer->coordinator);
    free(peer);
}

int tr_raft_snapshot_peer_begin(
    tr_raft_snapshot_peer_t *peer,
    tr_raft_term_t leader_term,
    tr_raft_index_t snapshot_index,
    tr_raft_term_t snapshot_term,
    const tr_raft_conf_t *configuration,
    const uint8_t *data,
    size_t size)
{
    if (peer == NULL) {
        return TURBO_EINVAL;
    }
    return tr_raft_snapshot_coordinator_begin(
        peer->coordinator, leader_term, snapshot_index, snapshot_term,
        configuration, data, size);
}

int tr_raft_snapshot_peer_handle_payload(
    tr_raft_snapshot_peer_t *peer,
    const tr_raft_coronet_payload_t *payload)
{
    if (peer == NULL || payload == NULL ||
        payload->kind != TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_ACK) {
        return TURBO_EPROTO;
    }
    return tr_raft_snapshot_coordinator_handle_ack(
        peer->coordinator, &payload->data.snapshot_ack);
}

int tr_raft_snapshot_peer_resume(tr_raft_snapshot_peer_t *peer)
{
    if (peer == NULL) {
        return TURBO_EINVAL;
    }
    return tr_raft_snapshot_coordinator_resume(peer->coordinator);
}

int tr_raft_snapshot_peer_get_status(
    const tr_raft_snapshot_peer_t *peer,
    tr_raft_snapshot_sender_status_t *out_status)
{
    if (peer == NULL) {
        return TURBO_EINVAL;
    }
    return tr_raft_snapshot_coordinator_get_status(peer->coordinator,
                                                   out_status);
}
