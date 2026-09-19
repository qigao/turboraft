#include <turboraft/raft_cnet_peer.h>

#include "raft_group_queue.h"
#include "raft_transport_payload_storage.h"

#include <salts_error.h>

#include <stdlib.h>
#include <string.h>

struct tr_raft_cnet_peer {
    cnet_client *client;
    cnet_connection connection;
    cnet_connection_state connection_state;
    tr_raft_transport_session_t *transport;
    tr_raft_group_queue_t outbound;
    tr_raft_transport_queue_limits_t outbound_limits;
    tr_raft_group_queue_token_t packet_token;
    uint8_t *packet;
    size_t packet_size;
    uint64_t frames_admitted;
    uint64_t bytes_admitted;
    uint64_t completed_writes;
    int packet_token_valid;
    int packet_ready;
    int write_pending;
    int stopping;
    int last_error;
};

static size_t tr_raft_cnet_payload_bytes(
    const tr_raft_transport_payload_t *payload)
{
    if (payload == NULL) {
        return 0U;
    }
    if (payload->kind == TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK) {
        return payload->data.snapshot_chunk.data_length;
    }
    if (payload->kind == TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK) {
        return payload->data.data_chunk.data_length;
    }
    return 0U;
}

static void tr_raft_cnet_release_owned(
    tr_raft_owned_transport_payload_t *owned)
{
    tr_raft_owned_transport_payload_release(owned);
}

static int tr_raft_cnet_limits_valid(
    const tr_raft_transport_queue_limits_t *limits)
{
    return limits != NULL &&
           limits->total_item_capacity != 0U &&
           limits->total_item_capacity <= TR_RAFT_CNET_MAX_QUEUE_CAPACITY &&
           limits->total_data_bytes != 0U &&
           limits->max_active_groups != 0U &&
           limits->max_active_groups <= limits->total_item_capacity &&
           limits->per_group_item_capacity != 0U &&
           limits->per_group_item_capacity <= limits->total_item_capacity &&
           limits->per_group_data_bytes != 0U &&
           limits->per_group_data_bytes <= limits->total_data_bytes;
}

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
    if (peer == NULL) {
        return;
    }
    tr_raft_group_queue_destroy(&peer->outbound);
    tr_raft_transport_session_destroy(peer->transport);
    free(peer->packet);
    free(peer);
}

int tr_raft_cnet_peer_create(const tr_raft_cnet_peer_config_t *config,
                             tr_raft_cnet_peer_t **out_peer)
{
    tr_raft_group_queue_config_t queue_config;
    tr_raft_cnet_peer_t *peer;
    int result;

    if (config == NULL || out_peer == NULL || config->client == NULL) {
        return SALTS_EINVAL;
    }
    if (!tr_raft_cnet_limits_valid(&config->outbound_limits)) {
        return SALTS_ERANGE;
    }

    *out_peer = NULL;
    peer = (tr_raft_cnet_peer_t *)calloc(1U, sizeof(*peer));
    if (peer == NULL) {
        return SALTS_ENOMEM;
    }
    peer->client = config->client;
    peer->outbound_limits = config->outbound_limits;
    peer->packet = (uint8_t *)malloc(TR_RAFT_TRANSPORT_MAX_PACKET_SIZE);
    if (peer->packet == NULL) {
        tr_raft_cnet_release(peer);
        return SALTS_ENOMEM;
    }

    memset(&queue_config, 0, sizeof(queue_config));
    queue_config.max_groups = config->outbound_limits.max_active_groups;
    queue_config.total_item_capacity =
        config->outbound_limits.total_item_capacity;
    queue_config.total_data_bytes = config->outbound_limits.total_data_bytes;
    queue_config.per_group_item_capacity =
        config->outbound_limits.per_group_item_capacity;
    queue_config.per_group_data_bytes =
        config->outbound_limits.per_group_data_bytes;
    queue_config.release = tr_raft_cnet_release_owned;
    result = tr_raft_group_queue_init(&peer->outbound, &queue_config);
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
    size_t data_bytes;
    int result;

    if (peer == NULL || payload == NULL || payload->group_id == 0U) {
        return SALTS_EINVAL;
    }
    if (peer->stopping) {
        return SALTS_EPIPE;
    }

    data_bytes = tr_raft_cnet_payload_bytes(payload);
    result = tr_raft_owned_transport_payload_copy(&owned, payload);
    if (result != SALTS_OK) {
        return result;
    }
    result = tr_raft_group_queue_enqueue(&peer->outbound, &owned, data_bytes);
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
        peer->write_pending || tr_raft_group_queue_size(&peer->outbound) == 0U) {
        return SALTS_EBUSY;
    }

    if (!peer->packet_ready) {
        result = tr_raft_group_queue_peek_next(
            &peer->outbound, &peer->packet_token, &owned);
        if (result != SALTS_OK) {
            peer->last_error = result;
            return result;
        }
        result = tr_raft_transport_encode_payload(
            peer->transport, &owned->payload, peer->packet,
            TR_RAFT_TRANSPORT_MAX_PACKET_SIZE, &peer->packet_size);
        if (result != SALTS_OK) {
            peer->last_error = result;
            return result;
        }
        peer->packet_token_valid = 1;
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
    if (!peer->packet_token_valid) {
        peer->last_error = SALTS_EPROTO;
        return SALTS_EPROTO;
    }
    memset(&discard, 0, sizeof(discard));
    result = tr_raft_group_queue_pop(
        &peer->outbound, peer->packet_token, &discard, NULL);
    if (result != SALTS_OK) {
        peer->last_error = result;
        return result;
    }
    tr_raft_owned_transport_payload_release(&discard);
    peer->packet_token_valid = 0;
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
    if (tr_raft_transport_get_status(peer->transport,
                                     &out_status->transport) != SALTS_OK) {
        return SALTS_EPROTO;
    }
    out_status->outbound_limits = peer->outbound_limits;
    out_status->active_group_count =
        tr_raft_group_queue_active_groups(&peer->outbound);
    out_status->queued_payload_count = tr_raft_group_queue_size(&peer->outbound);
    out_status->queued_data_bytes =
        tr_raft_group_queue_data_bytes(&peer->outbound);
    out_status->frames_admitted = peer->frames_admitted;
    out_status->bytes_admitted = peer->bytes_admitted;
    out_status->completed_writes = peer->completed_writes;
    out_status->write_pending = peer->write_pending;
    out_status->stopping = peer->stopping;
    out_status->last_error = peer->last_error;
    return SALTS_OK;
}

int tr_raft_cnet_peer_get_group_status(
    const tr_raft_cnet_peer_t *peer,
    tr_raft_group_id_t group_id,
    tr_raft_transport_group_queue_status_t *out_status)
{
    tr_raft_group_queue_group_status_t status;
    int result;

    if (peer == NULL || out_status == NULL || group_id == 0U) {
        return SALTS_EINVAL;
    }
    result = tr_raft_group_queue_get_group_status(
        &peer->outbound, group_id, &status);
    if (result != SALTS_OK) {
        return result;
    }
    memset(out_status, 0, sizeof(*out_status));
    out_status->group_id = status.group_id;
    out_status->queued_payload_count = status.queued_item_count;
    out_status->queued_data_bytes = status.queued_data_bytes;
    return SALTS_OK;
}
