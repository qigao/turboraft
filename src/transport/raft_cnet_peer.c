#include <turboraft/raft_cnet_peer.h>

#include "../turboraft_stl_status.h"
#include "raft_transport_payload_storage.h"

#include <cstl/deque.h>
#include <salts_error.h>

#include <stdlib.h>
#include <string.h>

struct tr_raft_cnet_peer {
    cnet_client *client;
    cnet_connection connection;
    cnet_connection_state connection_state;
    tr_raft_transport_session_t *transport;
    deque_t outbound;
    size_t outbound_queue_capacity;
    uint8_t *packet;
    size_t packet_size;
    uint64_t frames_admitted;
    uint64_t bytes_admitted;
    uint64_t completed_writes;
    int initialized;
    int packet_ready;
    int write_pending;
    int stopping;
    int last_error;
};

static void tr_raft_cnet_state(void *user,
                               cnet_connection connection,
                               cnet_connection_state state,
                               const cnet_error *error)
{
    tr_raft_cnet_peer_t *peer = (tr_raft_cnet_peer_t *)user;

    peer->connection = connection;
    peer->connection_state = state;
    if (state == CNET_CONNECTION_CLOSED ||
        state == CNET_CONNECTION_FAILED) {
        peer->write_pending = 0;
    }
    if (error != NULL && error->status != SALTS_OK) {
        peer->last_error = error->status;
    }
}

static void tr_raft_cnet_receive(void *user,
                                 cnet_connection connection,
                                 const cnet_receive_view *view)
{
    tr_raft_cnet_peer_t *peer = (tr_raft_cnet_peer_t *)user;
    int result;

    (void)connection;
    if (view == NULL || view->kind != CNET_MESSAGE_BYTES) {
        peer->last_error = SALTS_EPROTO;
        return;
    }
    result = tr_raft_transport_feed(peer->transport,
                                    (const uint8_t *)view->data, view->size);
    if (result != SALTS_OK) {
        peer->last_error = result;
        (void)cnet_close(peer->client, peer->connection);
    }
}

static void tr_raft_cnet_send_complete(void *user,
                                       cnet_connection connection,
                                       size_t size)
{
    tr_raft_cnet_peer_t *peer = (tr_raft_cnet_peer_t *)user;

    (void)connection;
    (void)size;
    peer->write_pending = 0;
    ++peer->completed_writes;
}

static void tr_raft_cnet_release(tr_raft_cnet_peer_t *peer)
{
    tr_raft_owned_transport_payload_t owned;

    while (peer->initialized &&
           deque_pop_front(&peer->outbound, &owned) == STL_OK) {
        tr_raft_owned_transport_payload_release(&owned);
    }
    if (peer->initialized) {
        deque_destroy(&peer->outbound);
    }
    tr_raft_transport_session_destroy(peer->transport);
    free(peer->packet);
    free(peer);
}

int tr_raft_cnet_peer_create(const tr_raft_cnet_peer_config_t *config,
                             tr_raft_cnet_peer_t **out_peer)
{
    tr_raft_cnet_peer_t *peer;
    size_t capacity;
    int result;

    if (config == NULL || out_peer == NULL || config->client == NULL) {
        return SALTS_EINVAL;
    }
    *out_peer = NULL;
    capacity = config->outbound_queue_capacity;
    if (capacity == 0U || capacity > TR_RAFT_CNET_MAX_QUEUE_CAPACITY) {
        return SALTS_ERANGE;
    }
    peer = (tr_raft_cnet_peer_t *)calloc(1U, sizeof(*peer));
    if (peer == NULL) {
        return SALTS_ENOMEM;
    }
    peer->client = config->client;
    peer->outbound_queue_capacity = capacity;
    peer->packet = (uint8_t *)malloc(TR_RAFT_TRANSPORT_MAX_PACKET_SIZE);
    if (peer->packet == NULL) {
        free(peer);
        return SALTS_ENOMEM;
    }
    result = tr_raft_stl_status_to_error(deque_init_bytes(
        &peer->outbound, sizeof(tr_raft_owned_transport_payload_t),
        _Alignof(tr_raft_owned_transport_payload_t), capacity));
    if (result == SALTS_OK) {
        peer->initialized = 1;
        result = tr_raft_stl_status_to_error(
            deque_reserve(&peer->outbound, capacity));
    }
    if (result == SALTS_OK) {
        result = tr_raft_transport_session_create(&config->transport,
                                                   &peer->transport);
    }
    if (result != SALTS_OK) {
        tr_raft_cnet_release(peer);
        return result;
    }
    *out_peer = peer;
    return SALTS_OK;
}

cnet_observer tr_raft_cnet_peer_observer(tr_raft_cnet_peer_t *peer)
{
    cnet_observer observer;

    memset(&observer, 0, sizeof(observer));
    if (peer != NULL) {
        observer.on_state = tr_raft_cnet_state;
        observer.on_receive = tr_raft_cnet_receive;
        observer.on_send = tr_raft_cnet_send_complete;
        observer.user = peer;
    }
    return observer;
}

int tr_raft_cnet_peer_enqueue_payload(
    tr_raft_cnet_peer_t *peer,
    const tr_raft_transport_payload_t *payload)
{
    tr_raft_owned_transport_payload_t owned;
    int result;

    if (peer == NULL || payload == NULL) {
        return SALTS_EINVAL;
    }
    if (peer->stopping) {
        return SALTS_EPIPE;
    }
    if (deque_size(&peer->outbound) >= peer->outbound_queue_capacity) {
        return SALTS_ENOSPC;
    }
    result = tr_raft_owned_transport_payload_copy(&owned, payload);
    if (result != SALTS_OK) {
        return result;
    }
    result = tr_raft_stl_status_to_error(
        deque_push_back(&peer->outbound, &owned));
    if (result != SALTS_OK) {
        tr_raft_owned_transport_payload_release(&owned);
    }
    return result;
}

int tr_raft_cnet_peer_enqueue_group(
    tr_raft_cnet_peer_t *peer,
    tr_raft_group_id_t group_id,
    const tr_raft_message_t *message)
{
    tr_raft_transport_payload_t payload;

    if (peer == NULL || message == NULL || group_id == 0U) {
        return SALTS_EINVAL;
    }
    memset(&payload, 0, sizeof(payload));
    payload.group_id = group_id;
    payload.kind = TR_RAFT_WIRE_PAYLOAD_RAFT;
    payload.data.raft = *message;
    return tr_raft_cnet_peer_enqueue_payload(peer, &payload);
}

int tr_raft_cnet_peer_step(tr_raft_cnet_peer_t *peer)
{
    const tr_raft_owned_transport_payload_t *owned;
    tr_raft_owned_transport_payload_t discard;
    int result;

    if (peer == NULL) {
        return SALTS_EINVAL;
    }
    if (peer->stopping) {
        return SALTS_EPIPE;
    }
    if (peer->connection_state != CNET_CONNECTION_CONNECTED ||
        peer->write_pending || deque_size(&peer->outbound) == 0U) {
        return SALTS_EBUSY;
    }
    if (!peer->packet_ready) {
        owned = (const tr_raft_owned_transport_payload_t *)deque_at_const(
            &peer->outbound, 0U);
        if (owned == NULL) {
            return SALTS_EPROTO;
        }
        result = tr_raft_transport_encode_payload(
            peer->transport, &owned->payload, peer->packet,
            TR_RAFT_TRANSPORT_MAX_PACKET_SIZE, &peer->packet_size);
        if (result != SALTS_OK) {
            peer->last_error = result;
            return result;
        }
        peer->packet_ready = 1;
    }
    result = cnet_send(peer->client, peer->connection, peer->packet,
                       peer->packet_size);
    if (result != SALTS_OK) {
        if (result != SALTS_EBUSY) {
            peer->last_error = result;
        }
        return result;
    }
    if (deque_pop_front(&peer->outbound, &discard) != STL_OK) {
        peer->last_error = SALTS_EPROTO;
        return SALTS_EPROTO;
    }
    tr_raft_owned_transport_payload_release(&discard);
    peer->packet_ready = 0;
    peer->write_pending = 1;
    ++peer->frames_admitted;
    peer->bytes_admitted += peer->packet_size;
    return SALTS_OK;
}

int tr_raft_cnet_peer_stop(tr_raft_cnet_peer_t *peer)
{
    int result;

    if (peer == NULL) {
        return SALTS_EINVAL;
    }
    if (peer->stopping) {
        return SALTS_OK;
    }
    peer->stopping = 1;
    if (peer->connection_state != CNET_CONNECTION_CONNECTING &&
        peer->connection_state != CNET_CONNECTION_CONNECTED &&
        peer->connection_state != CNET_CONNECTION_CLOSING) {
        return SALTS_OK;
    }
    result = cnet_close(peer->client, peer->connection);
    if (result != SALTS_OK && result != SALTS_ENOENT) {
        peer->last_error = result;
        return result;
    }
    return SALTS_OK;
}

int tr_raft_cnet_peer_destroy(tr_raft_cnet_peer_t *peer)
{
    if (peer == NULL) {
        return SALTS_OK;
    }
    if (peer->connection_state == CNET_CONNECTION_CONNECTING ||
        peer->connection_state == CNET_CONNECTION_CONNECTED ||
        peer->connection_state == CNET_CONNECTION_CLOSING ||
        peer->write_pending) {
        return SALTS_EBUSY;
    }
    tr_raft_cnet_release(peer);
    return SALTS_OK;
}

int tr_raft_cnet_peer_get_status(const tr_raft_cnet_peer_t *peer,
                                 tr_raft_cnet_peer_status_t *out_status)
{
    if (peer == NULL || out_status == NULL) {
        return SALTS_EINVAL;
    }
    memset(out_status, 0, sizeof(*out_status));
    out_status->connection_state = peer->connection_state;
    out_status->outbound_queue_capacity = peer->outbound_queue_capacity;
    out_status->queued_payload_count = deque_size(&peer->outbound);
    out_status->frames_admitted = peer->frames_admitted;
    out_status->bytes_admitted = peer->bytes_admitted;
    out_status->completed_writes = peer->completed_writes;
    out_status->write_pending = peer->write_pending;
    out_status->stopping = peer->stopping;
    out_status->last_error = peer->last_error;
    return SALTS_OK;
}
