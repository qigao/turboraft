#include <turboraft/raft_cnet_channel.h>

#include <cmeta_error.h>
#include <cmeta_buffer.h>

#include <stdlib.h>
#include <string.h>

struct tr_raft_cnet_channel {
    cnet_client *client;
    const tr_raft_cnet_identity_policy_t *identity;
    tr_raft_handshake_config_t local;
    tr_raft_transport_payload_handler_fn on_payload;
    void *payload_context;
    tr_raft_handshake_exchange_t *exchange;
    tr_raft_transport_session_t *transport;
    cnet_connection connection;
    tr_raft_cnet_channel_status_t status;
    uint64_t first_outbound_message_id;
    unsigned callback_depth;
    size_t handshake_writes_pending; /* on_send accounts HELLO/ACK first */
    int bound;
    int stopping;
};

/* CNet alone retains the bytes on successful send admission. */
static int tr_channel_send_bytes(
    tr_raft_cnet_channel_t *channel,
    const uint8_t *data,
    size_t size)
{
    mem_buffer_t *buffer;
    int result;

    if (size == 0U || data == NULL ||
        size > TR_RAFT_TRANSPORT_MAX_PACKET_SIZE)
        return SALTS_EINVAL;
    buffer = mem_get_buffer(mem_global(), size);
    if (buffer == NULL) return SALTS_ENOMEM;
    memcpy(mem_buffer_data(buffer), data, size);
    mem_set_used(buffer, size);
    result = cnet_send_buffer(channel->client, channel->connection, buffer);
    if (result == SALTS_OK) ++channel->handshake_writes_pending;
    mem_buffer_release(buffer);
    return result;
}

static int tr_channel_same_connection(
    cnet_connection lhs, cnet_connection rhs)
{
    return lhs.slot == rhs.slot && lhs.generation == rhs.generation;
}

/* A failure fences Raft callbacks synchronously; CNet owns the terminal
 * callback and all outstanding TLS/send completions until it reports close. */
static void tr_channel_fault(tr_raft_cnet_channel_t *channel, int reason)
{
    int ignored;

    if (channel->status.last_error == SALTS_OK)
        channel->status.last_error =
            reason == SALTS_OK ? SALTS_EPROTO : reason;
    channel->status.phase = TR_RAFT_CNET_CHANNEL_FAILED;
    channel->stopping = 1;
    if (channel->bound && !channel->status.terminal) {
        ignored = cnet_close(channel->client, channel->connection);
        (void)ignored;
    }
}

/* CNet executes ordered logical send terminals on this same progress Owner.
 * A Channel retains NO buffer references or raw CNet request handles here.
 * TLS/HELLO writes are admitted before any post-HELLO Raft payload. */
static void tr_channel_send_complete(
    void *context, cnet_connection connection, size_t bytes)
{
    tr_raft_cnet_channel_t *channel = (tr_raft_cnet_channel_t *)context;

    (void)bytes;
    ++channel->callback_depth;
    if (!channel->bound ||
        !tr_channel_same_connection(channel->connection, connection) ||
        channel->status.terminal) {
        tr_channel_fault(channel, SALTS_EPROTO);
    } else if (channel->handshake_writes_pending != 0U) {
        --channel->handshake_writes_pending;
    } else if (channel->status.payload_writes_pending != 0U) {
        --channel->status.payload_writes_pending;
        ++channel->status.payloads_completed;
    } else {
        /* No unmatched/duplicate local completion may grant fresh credit. */
        tr_channel_fault(channel, SALTS_EPROTO);
    }
    --channel->callback_depth;
}

static int tr_channel_payload(void *context,
                              const tr_raft_transport_payload_t *payload)
{
    tr_raft_cnet_channel_t *channel = (tr_raft_cnet_channel_t *)context;
    int result;

    if (channel->stopping ||
        channel->status.phase != TR_RAFT_CNET_CHANNEL_ACTIVE)
        return SALTS_ESHUTDOWN;

    result = channel->on_payload(channel->payload_context, payload);
    if (result == SALTS_OK) ++channel->status.payloads_received;
    return result;
}

static int tr_channel_activate_transport(tr_raft_cnet_channel_t *channel)
{
    tr_raft_transport_session_config_t config = {0};
    tr_raft_handshake_result_t handshake = {0};
    int result;

    result = tr_raft_handshake_exchange_get_result(
        channel->exchange, &handshake);
    if (result != SALTS_OK) return result;

    config.cluster_id = channel->local.cluster_id;
    config.local_node_id = channel->local.local_node_id;
    config.peer_node_id = channel->status.authenticated_peer_node_id;
    config.first_outbound_message_id = channel->first_outbound_message_id;
    config.handshake = &handshake;
    config.on_payload = tr_channel_payload;
    config.payload_context = channel;
    result = tr_raft_transport_session_create(
        &config, &channel->transport);
    if (result != SALTS_OK) return result;

    tr_raft_handshake_exchange_destroy(channel->exchange);
    channel->exchange = NULL;
    channel->status.phase = TR_RAFT_CNET_CHANNEL_ACTIVE;
    return SALTS_OK;
}

static int tr_channel_on_bytes(tr_raft_cnet_channel_t *channel,
                               const uint8_t *data,
                               size_t size)
{
    size_t offset = 0U;
    int result;

    if (channel->status.phase == TR_RAFT_CNET_CHANNEL_NEGOTIATING) {
        uint8_t answer[TR_RAFT_HANDSHAKE_PACKET_SIZE];
        size_t consumed = 0U;
        size_t produced = 0U;
        tr_raft_handshake_exchange_state_t state;

        result = tr_raft_handshake_exchange_feed(
            channel->exchange, data, size, &consumed, answer,
            sizeof(answer), &produced);
        if (result != SALTS_OK) return result;
        offset = consumed;
        if (produced != 0U) {
            result = tr_channel_send_bytes(channel, answer, produced);
            if (result != SALTS_OK) return result;
            ++channel->status.handshake_packets_sent;
        }
        result = tr_raft_handshake_exchange_get_state(
            channel->exchange, &state);
        if (result != SALTS_OK) return result;
        if (state == TR_RAFT_HANDSHAKE_EXCHANGE_COMPLETE) {
            result = tr_channel_activate_transport(channel);
            if (result != SALTS_OK) return result;
        }
    }

    if (offset < size) {
        if (channel->status.phase != TR_RAFT_CNET_CHANNEL_ACTIVE ||
            channel->transport == NULL)
            return SALTS_EPROTO;
        result = tr_raft_transport_feed(
            channel->transport, data + offset, size - offset);
        if (result != SALTS_OK) return result;
    }
    return SALTS_OK;
}

static void tr_channel_state(void *context,
                             cnet_connection connection,
                             cnet_connection_state state,
                             const cnet_error *error)
{
    tr_raft_cnet_channel_t *channel = (tr_raft_cnet_channel_t *)context;
    int result;

    ++channel->callback_depth;
    if (channel->bound &&
        !tr_channel_same_connection(channel->connection, connection)) {
        tr_channel_fault(channel, SALTS_EPROTO);
        goto done;
    }
    channel->bound = 1;
    channel->connection = connection;
    /* A CONNECTING callback can precede explicit post-connect attach().
     * Either path must promote the same connection to the TLS gate. */
    if (channel->status.phase == TR_RAFT_CNET_CHANNEL_CREATED)
        channel->status.phase = TR_RAFT_CNET_CHANNEL_TLS;

    if (state == CNET_CONNECTION_CLOSED ||
        state == CNET_CONNECTION_FAILED) {
        if (state == CNET_CONNECTION_FAILED &&
            channel->status.last_error == SALTS_OK)
            channel->status.last_error =
                error != NULL && error->status != SALTS_OK
                    ? error->status : SALTS_EIO;
        if (state == CNET_CONNECTION_FAILED ||
            channel->status.phase == TR_RAFT_CNET_CHANNEL_FAILED)
            channel->status.phase = TR_RAFT_CNET_CHANNEL_FAILED;
        else
            channel->status.phase = TR_RAFT_CNET_CHANNEL_CLOSED;
        /* CNet is terminal: every admitted but unsent logical payload has
         * exactly one local cancellation, never remote delivery/replay. */
        channel->status.payloads_canceled +=
            (uint64_t)channel->status.payload_writes_pending;
        channel->status.payload_writes_pending = 0U;
        channel->handshake_writes_pending = 0U;
        channel->status.terminal = 1;
        channel->stopping = 1;
        tr_raft_handshake_exchange_destroy(channel->exchange);
        channel->exchange = NULL;
        (void)tr_raft_transport_session_destroy(channel->transport);
        channel->transport = NULL;
        goto done;
    }

    if (state == CNET_CONNECTION_CLOSING) {
        channel->stopping = 1;
        if (channel->status.phase != TR_RAFT_CNET_CHANNEL_FAILED)
            channel->status.phase = TR_RAFT_CNET_CHANNEL_CLOSING;
        goto done;
    }

    if (state != CNET_CONNECTION_CONNECTED || channel->stopping ||
        channel->status.phase != TR_RAFT_CNET_CHANNEL_TLS)
        goto done;

    /* No application receive demand is armed until CNet verified this TLS
     * peer leaf and the exact configured Node ID is unambiguous. */
    result = tr_raft_cnet_identity_admit_tls(
        channel->identity, channel->client, connection,
        &channel->status.authenticated_peer_node_id);
    if (result != SALTS_OK) {
        tr_channel_fault(channel, result);
        goto done;
    }

    result = tr_raft_handshake_exchange_create(
        &channel->local,
        channel->status.authenticated_peer_node_id,
        &channel->exchange);
    if (result == SALTS_OK) {
        uint8_t hello[TR_RAFT_HANDSHAKE_PACKET_SIZE];
        size_t hello_size = 0U;
        result = tr_raft_handshake_exchange_start(
            channel->exchange, hello, sizeof(hello), &hello_size);
        if (result == SALTS_OK) {
            result = tr_channel_send_bytes(channel, hello, hello_size);
            if (result == SALTS_OK) ++channel->status.handshake_packets_sent;
        }
    }
    if (result == SALTS_OK) {
        channel->status.phase = TR_RAFT_CNET_CHANNEL_NEGOTIATING;
        result = cnet_receive(channel->client, connection, 1U);
    }
    if (result != SALTS_OK)
        tr_channel_fault(channel, result);

done:
    --channel->callback_depth;
}

static void tr_channel_receive(void *context,
                               cnet_connection connection,
                               const cnet_receive_view *view)
{
    tr_raft_cnet_channel_t *channel = (tr_raft_cnet_channel_t *)context;
    int result;

    ++channel->callback_depth;
    if (!channel->bound ||
        !tr_channel_same_connection(channel->connection, connection) ||
        channel->stopping ||
        (channel->status.phase != TR_RAFT_CNET_CHANNEL_NEGOTIATING &&
         channel->status.phase != TR_RAFT_CNET_CHANNEL_ACTIVE)) {
        tr_channel_fault(channel, SALTS_EPROTO);
        goto done;
    }
    if (view == NULL || view->kind != CNET_MESSAGE_BYTES ||
        view->data == NULL || view->size == 0U) {
        tr_channel_fault(channel, SALTS_EPROTO);
        goto done;
    }

    result = tr_channel_on_bytes(
        channel, (const uint8_t *)view->data, view->size);
    if (result == SALTS_OK && !channel->stopping)
        result = cnet_receive(channel->client, connection, 1U);
    if (result != SALTS_OK)
        tr_channel_fault(channel, result);

done:
    --channel->callback_depth;
}

int tr_raft_cnet_channel_create(
    const tr_raft_cnet_channel_config_t *config,
    tr_raft_cnet_channel_t **out_channel)
{
    tr_raft_cnet_channel_t *channel;
    tr_raft_handshake_message_t hello;

    if (out_channel == NULL) return SALTS_EINVAL;
    *out_channel = NULL;
    if (config == NULL || config->client == NULL ||
        config->identity == NULL || config->on_payload == NULL ||
        config->first_outbound_message_id == 0U ||
        config->handshake.local_node_id != config->identity->local_node_id ||
        tr_raft_cnet_identity_policy_validate(config->identity) != SALTS_OK ||
        tr_raft_handshake_make_hello(&config->handshake, &hello) != SALTS_OK)
        return SALTS_EINVAL;

    channel = (tr_raft_cnet_channel_t *)calloc(1U, sizeof(*channel));
    if (channel == NULL) return SALTS_ENOMEM;
    channel->client = config->client;
    channel->identity = config->identity;
    channel->local = config->handshake;
    channel->on_payload = config->on_payload;
    channel->payload_context = config->payload_context;
    channel->first_outbound_message_id = config->first_outbound_message_id;
    channel->status.phase = TR_RAFT_CNET_CHANNEL_CREATED;
    *out_channel = channel;
    return SALTS_OK;
}

cnet_observer tr_raft_cnet_channel_observer(
    tr_raft_cnet_channel_t *channel)
{
    cnet_observer observer = {0};
    if (channel != NULL) {
        observer.user = channel;
        observer.on_state = tr_channel_state;
        observer.on_receive = tr_channel_receive;
        observer.on_send = tr_channel_send_complete;
    }
    return observer;
}

int tr_raft_cnet_channel_attach(tr_raft_cnet_channel_t *channel,
                                cnet_connection connection)
{
    if (channel == NULL || connection.generation == 0U)
        return SALTS_EINVAL;
    if (channel->bound)
        return tr_channel_same_connection(
            channel->connection, connection) ? SALTS_OK : SALTS_EALREADY;
    if (channel->status.phase != TR_RAFT_CNET_CHANNEL_CREATED)
        return SALTS_EBUSY;

    channel->bound = 1;
    channel->connection = connection;
    channel->status.phase = TR_RAFT_CNET_CHANNEL_TLS;
    return SALTS_OK;
}

int tr_raft_cnet_channel_get_status(
    const tr_raft_cnet_channel_t *channel,
    tr_raft_cnet_channel_status_t *out_status)
{
    if (channel == NULL || out_status == NULL) return SALTS_EINVAL;
    *out_status = channel->status;
    return SALTS_OK;
}

int tr_raft_cnet_channel_send(
    tr_raft_cnet_channel_t *channel,
    const tr_raft_transport_payload_t *payload)
{
    mem_buffer_t *packet;
    size_t size = 0U;
    int result;

    if (channel == NULL || payload == NULL) return SALTS_EINVAL;
    if (channel->stopping ||
        channel->status.phase != TR_RAFT_CNET_CHANNEL_ACTIVE ||
        channel->transport == NULL)
        return SALTS_EBUSY;
    if (channel->status.payloads_admitted == UINT64_MAX ||
        channel->status.payload_writes_pending == SIZE_MAX)
        return SALTS_ERANGE;

    packet = mem_get_buffer(mem_global(), TR_RAFT_TRANSPORT_MAX_PACKET_SIZE);
    if (packet == NULL) return SALTS_ENOMEM;
    result = tr_raft_transport_encode_payload(
        channel->transport, payload,
        (uint8_t *)mem_buffer_data(packet),
        TR_RAFT_TRANSPORT_MAX_PACKET_SIZE, &size);
    if (result == SALTS_OK) {
        mem_set_used(packet, size);
        result = cnet_send_buffer(
            channel->client, channel->connection, packet);
        if (result == SALTS_OK) {
            ++channel->status.payloads_admitted;
            ++channel->status.payload_writes_pending;
        }
    }
    mem_buffer_release(packet);
    return result;
}

/* Bounded peer-local admission uses the existing Service capacity protocol.
 * A not-yet-authorized TLS channel never accepts a Raft Ready, and the
 * Component/Service caller decides when to resume the exact staged suffix. */
static int tr_channel_group_enqueue(void *context,
                                    const tr_raft_message_t *message)
{
    tr_raft_cnet_channel_group_binding_t *binding =
        (tr_raft_cnet_channel_group_binding_t *)context;
    tr_raft_transport_payload_t payload = {0};
    int result;

    if (binding == NULL || binding->channel == NULL ||
        binding->group_id == 0U || message == NULL)
        return SALTS_EINVAL;

    payload.group_id = binding->group_id;
    payload.kind = TR_RAFT_WIRE_PAYLOAD_RAFT;
    payload.data.raft = *message;
    result = tr_raft_cnet_channel_send(binding->channel, &payload);
    if (result == SALTS_EBUSY || result == SALTS_ENOBUFS)
        return SALTS_ENOSPC;
    return result;
}

int tr_raft_cnet_channel_group_transport_bind(
    tr_raft_cnet_channel_group_binding_t *binding,
    tr_raft_transport_t *transport)
{
    if (binding == NULL || binding->channel == NULL ||
        binding->group_id == 0U || transport == NULL)
        return SALTS_EINVAL;
    if (transport->enqueue != NULL)
        return SALTS_EALREADY;

    transport->context = binding;
    transport->enqueue = tr_channel_group_enqueue;
    return SALTS_OK;
}

int tr_raft_cnet_channel_stop(tr_raft_cnet_channel_t *channel)
{
    int result;

    if (channel == NULL) return SALTS_EINVAL;
    if (channel->status.terminal || channel->stopping)
        return SALTS_OK;
    if (!channel->bound) {
        channel->stopping = 1;
        channel->status.terminal = 1;
        channel->status.phase = TR_RAFT_CNET_CHANNEL_CLOSED;
        return SALTS_OK;
    }

    result = cnet_close(channel->client, channel->connection);
    if (result == SALTS_OK || result == SALTS_EALREADY) {
        channel->stopping = 1;
        channel->status.phase = TR_RAFT_CNET_CHANNEL_CLOSING;
        return SALTS_OK;
    }
    return result;
}

int tr_raft_cnet_channel_destroy(tr_raft_cnet_channel_t *channel)
{
    if (channel == NULL) return SALTS_OK;
    if (channel->callback_depth != 0U ||
        (channel->bound && !channel->status.terminal))
        return SALTS_EBUSY;
    tr_raft_handshake_exchange_destroy(channel->exchange);
    (void)tr_raft_transport_session_destroy(channel->transport);
    free(channel);
    return SALTS_OK;
}
