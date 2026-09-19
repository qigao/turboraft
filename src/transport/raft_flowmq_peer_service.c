#include <turboraft/raft_flowmq_peer_service.h>

#include "raft_group_queue.h"
#include "raft_transport_payload_storage.h"

#include <salts_error.h>

#include <limits.h>
#include <stdlib.h>
#include <string.h>

#define TR_RAFT_FLOWMQ_ENDPOINT_CAPACITY 512U

typedef struct tr_raft_flowmq_peer {
    tr_raft_node_id_t node_id;
    char identity[TR_RAFT_FLOWMQ_MAX_IDENTITY_SIZE + 1U];
    char endpoint[TR_RAFT_FLOWMQ_ENDPOINT_CAPACITY];
    flowmq_socket_t *dealer;
    tr_raft_transport_session_t *session;
    tr_raft_group_queue_t outbound;
    tr_raft_group_queue_token_t packet_token;
    uint8_t *packet;
    size_t packet_size;
    int packet_token_valid;
    int packet_ready;
    int faulted;
} tr_raft_flowmq_peer_t;

struct tr_raft_flowmq_peer_service {
    flowmq_ctx_t *context;
    flowmq_socket_t *router;
    tr_raft_flowmq_peer_t peers[TR_RAFT_MAX_VOTERS - 1U];
    size_t peer_count;
    tr_raft_transport_queue_limits_t outbound_limits;
    size_t max_send_batch_items;
    size_t max_receive_batch_items;
    char bind_endpoint[TR_RAFT_FLOWMQ_ENDPOINT_CAPACITY];
    uint8_t *packet;
    uint64_t frames_sent;
    uint64_t frames_received;
    int started;
    int stopping;
    int step_active;
    int last_error;
};

static int tr_raft_flowmq_copy_string(char *output,
                                      size_t capacity,
                                      const char *input)
{
    size_t size;

    if (output == NULL || capacity == 0U || input == NULL) {
        return SALTS_EINVAL;
    }
    size = strlen(input);
    if (size == 0U || size >= capacity) {
        return SALTS_ERANGE;
    }
    memcpy(output, input, size + 1U);
    return SALTS_OK;
}

static int tr_raft_flowmq_is_tls_endpoint(const char *endpoint)
{
    return endpoint != NULL && strncmp(endpoint, "tls://", 6U) == 0;
}

static int tr_raft_flowmq_set_string(flowmq_socket_t *socket,
                                     int option,
                                     const char *value)
{
    if (value == NULL || value[0] == '\0') {
        return SALTS_OK;
    }
    return flowmq_setsockopt(socket, option, value, strlen(value));
}

static int tr_raft_flowmq_set_int(flowmq_socket_t *socket,
                                  int option,
                                  int value)
{
    return flowmq_setsockopt(socket, option, &value, sizeof(value));
}

static int tr_raft_flowmq_set_size(flowmq_socket_t *socket,
                                   int option,
                                   size_t value)
{
    return flowmq_setsockopt(socket, option, &value, sizeof(value));
}

static int tr_raft_flowmq_configure_common(
    flowmq_socket_t *socket,
    const tr_raft_flowmq_peer_service_config_t *config)
{
    size_t send_bytes = config->send_hwm_bytes;
    size_t receive_bytes = config->receive_hwm_bytes;
    int send_messages = (int)config->send_hwm_messages;
    int receive_messages = (int)config->receive_hwm_messages;
    int reconnect = (int)config->reconnect_initial_ms;
    int reconnect_max = (int)config->reconnect_max_ms;
    int heartbeat = (int)config->heartbeat_interval_ms;
    int heartbeat_timeout = (int)config->heartbeat_timeout_ms;
    int result;

    result = tr_raft_flowmq_set_int(socket, FLOWMQ_SNDHWM, send_messages);
    if (result == SALTS_OK) {
        result = tr_raft_flowmq_set_int(socket, FLOWMQ_RCVHWM,
                                        receive_messages);
    }
    if (result == SALTS_OK) {
        result = tr_raft_flowmq_set_size(socket, FLOWMQ_SNDHWM_BYTES,
                                         send_bytes);
    }
    if (result == SALTS_OK) {
        result = tr_raft_flowmq_set_size(socket, FLOWMQ_RCVHWM_BYTES,
                                         receive_bytes);
    }
    if (result == SALTS_OK) {
        result = tr_raft_flowmq_set_int(socket, FLOWMQ_RECONNECT_IVL,
                                        reconnect);
    }
    if (result == SALTS_OK) {
        result = tr_raft_flowmq_set_int(socket, FLOWMQ_RECONNECT_IVL_MAX,
                                        reconnect_max);
    }
    if (result == SALTS_OK && heartbeat != 0) {
        result = tr_raft_flowmq_set_int(socket, FLOWMQ_HEARTBEAT_IVL,
                                        heartbeat);
    }
    if (result == SALTS_OK && heartbeat_timeout != 0) {
        result = tr_raft_flowmq_set_int(socket, FLOWMQ_HEARTBEAT_TIMEOUT,
                                        heartbeat_timeout);
    }
    return result;
}

static int tr_raft_flowmq_configure_tls(flowmq_socket_t *socket,
                                        const tr_raft_flowmq_tls_config_t *tls,
                                        int server)
{
    int result;

    if (tls == NULL || tls->ca_file == NULL || tls->ca_file[0] == '\0') {
        return SALTS_EINVAL;
    }
    result = tr_raft_flowmq_set_string(socket, FLOWMQ_TLS_CA_FILE,
                                       tls->ca_file);
    if (result == SALTS_OK) {
        result = tr_raft_flowmq_set_string(socket, FLOWMQ_TLS_CERT_FILE,
                                           tls->cert_file);
    }
    if (result == SALTS_OK) {
        result = tr_raft_flowmq_set_string(socket, FLOWMQ_TLS_KEY_FILE,
                                           tls->key_file);
    }
    if (result == SALTS_OK) {
        result = tr_raft_flowmq_set_string(socket, FLOWMQ_TLS_KEY_PASSWORD,
                                           tls->key_password);
    }
    if (result == SALTS_OK && server) {
        int required = tls->require_client_certificate != 0;
        result = tr_raft_flowmq_set_int(
            socket, FLOWMQ_TLS_REQUIRE_CLIENT_CERTIFICATE, required);
    }
    if (result == SALTS_OK && !server) {
        if (tls->server_name == NULL || tls->server_name[0] == '\0') {
            return SALTS_EINVAL;
        }
        result = tr_raft_flowmq_set_string(socket, FLOWMQ_TLS_SERVER_NAME,
                                           tls->server_name);
    }
    return result;
}

static int tr_raft_flowmq_transient(int result)
{
    return result == SALTS_EBUSY || result == SALTS_ENOTCONN ||
           result == SALTS_ECONNRESET || result == SALTS_ECONNREFUSED ||
           result == SALTS_EHOSTUNREACH || result == SALTS_ENETDOWN ||
           result == SALTS_ENETUNREACH || result == SALTS_ETIMEDOUT;
}

static size_t tr_raft_flowmq_payload_bytes(
    const tr_raft_transport_payload_t *payload)
{
    if (payload->kind == TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK) {
        return payload->data.snapshot_chunk.data_length;
    }
    if (payload->kind == TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK) {
        return payload->data.data_chunk.data_length;
    }
    return 0U;
}

static void tr_raft_flowmq_release_owned(
    tr_raft_owned_transport_payload_t *owned)
{
    tr_raft_owned_transport_payload_release(owned);
}

static int tr_raft_flowmq_limits_valid(
    const tr_raft_transport_queue_limits_t *limits)
{
    return limits != NULL &&
           limits->total_item_capacity != 0U &&
           limits->total_item_capacity <=
               TR_RAFT_FLOWMQ_MAX_OUTBOUND_QUEUE_CAPACITY &&
           limits->total_data_bytes != 0U &&
           limits->max_active_groups != 0U &&
           limits->max_active_groups <= limits->total_item_capacity &&
           limits->per_group_item_capacity != 0U &&
           limits->per_group_item_capacity <= limits->total_item_capacity &&
           limits->per_group_data_bytes != 0U &&
           limits->per_group_data_bytes <= limits->total_data_bytes;
}

static tr_raft_node_id_t tr_raft_flowmq_payload_to(
    const tr_raft_transport_payload_t *payload)
{
    switch (payload->kind) {
    case TR_RAFT_WIRE_PAYLOAD_RAFT:
        return payload->data.raft.to;
    case TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK:
        return payload->data.snapshot_chunk.to;
    case TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_ACK:
        return payload->data.snapshot_ack.to;
    case TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK:
        return payload->data.data_chunk.to;
    case TR_RAFT_WIRE_PAYLOAD_DATA_ACK:
        return payload->data.data_ack.to;
    default:
        return 0U;
    }
}

static tr_raft_flowmq_peer_t *tr_raft_flowmq_find_node(
    tr_raft_flowmq_peer_service_t *service,
    tr_raft_node_id_t node_id)
{
    size_t index;

    for (index = 0U; index < service->peer_count; ++index) {
        if (service->peers[index].node_id == node_id) {
            return &service->peers[index];
        }
    }
    return NULL;
}

static tr_raft_flowmq_peer_t *tr_raft_flowmq_find_identity(
    tr_raft_flowmq_peer_service_t *service,
    const char *identity,
    size_t size)
{
    size_t index;

    for (index = 0U; index < service->peer_count; ++index) {
        if (strlen(service->peers[index].identity) == size &&
            memcmp(service->peers[index].identity, identity, size) == 0) {
            return &service->peers[index];
        }
    }
    return NULL;
}

static void tr_raft_flowmq_release_peer(tr_raft_flowmq_peer_t *peer)
{
    if (peer == NULL) {
        return;
    }
    free(peer->packet);
    peer->packet = NULL;
    tr_raft_group_queue_destroy(&peer->outbound);
    tr_raft_transport_session_destroy(peer->session);
    peer->session = NULL;
}

static void tr_raft_flowmq_release(tr_raft_flowmq_peer_service_t *service)
{
    size_t index;

    if (service == NULL) {
        return;
    }
    for (index = 0U; index < service->peer_count; ++index) {
        if (service->peers[index].dealer != NULL) {
            (void)flowmq_close(service->peers[index].dealer);
            service->peers[index].dealer = NULL;
        }
        tr_raft_flowmq_release_peer(&service->peers[index]);
    }
    if (service->router != NULL) {
        (void)flowmq_close(service->router);
        service->router = NULL;
    }
    if (service->context != NULL) {
        (void)flowmq_ctx_term(service->context);
        service->context = NULL;
    }
    free(service->packet);
    free(service);
}

int tr_raft_flowmq_peer_service_create(
    const tr_raft_flowmq_peer_service_config_t *config,
    tr_raft_flowmq_peer_service_t **out_service)
{
    tr_raft_flowmq_peer_service_t *service;
    tr_raft_handshake_message_t hello;
    size_t index;
    int result;

    if (config == NULL || out_service == NULL ||
        config->bind_endpoint == NULL || config->local_identity == NULL ||
        config->on_payload == NULL || config->peer_count == 0U ||
        config->peer_count > TR_RAFT_MAX_VOTERS - 1U ||
        config->peers == NULL || config->protocol.local_node_id == 0U) {
        return SALTS_EINVAL;
    }
    result = tr_raft_handshake_make_hello(&config->protocol, &hello);
    if (result != SALTS_OK) {
        return result;
    }
    if (config->protocol.feature_bits != 0U ||
        config->protocol.wire_major_min != TR_RAFT_HANDSHAKE_WIRE_MAJOR ||
        config->protocol.wire_major_max != TR_RAFT_HANDSHAKE_WIRE_MAJOR ||
        config->protocol.wire_minor_min != TR_RAFT_HANDSHAKE_WIRE_MINOR ||
        config->protocol.wire_minor_max != TR_RAFT_HANDSHAKE_WIRE_MINOR ||
        config->protocol.max_frame_size < TR_RAFT_WIRE_MAX_FRAME_SIZE ||
        config->protocol.max_snapshot_chunk_size <
            TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES) {
        return SALTS_EPROTONOSUPPORT;
    }
    *out_service = NULL;
    service = (tr_raft_flowmq_peer_service_t *)calloc(1U, sizeof(*service));
    if (service == NULL) {
        return SALTS_ENOMEM;
    }
    service->peer_count = config->peer_count;
    service->outbound_limits = config->outbound_limits;
    service->max_send_batch_items = config->max_send_batch_items;
    service->max_receive_batch_items = config->max_receive_batch_items;
    if (!tr_raft_flowmq_limits_valid(&service->outbound_limits) ||
        service->max_send_batch_items == 0U ||
        service->max_send_batch_items >
            TR_RAFT_FLOWMQ_MAX_OUTBOUND_QUEUE_CAPACITY ||
        service->max_receive_batch_items == 0U ||
        service->max_receive_batch_items >
            TR_RAFT_FLOWMQ_MAX_OUTBOUND_QUEUE_CAPACITY ||
        config->send_hwm_messages == 0U ||
        config->receive_hwm_messages == 0U ||
        config->send_hwm_bytes == 0U ||
        config->receive_hwm_bytes == 0U ||
        config->reconnect_initial_ms == 0U ||
        config->reconnect_max_ms < config->reconnect_initial_ms ||
        config->send_hwm_messages > INT_MAX ||
        config->receive_hwm_messages > INT_MAX ||
        config->outbound_limits.total_item_capacity > INT_MAX ||
        config->reconnect_initial_ms > INT_MAX ||
        config->reconnect_max_ms > INT_MAX ||
        config->heartbeat_interval_ms > INT_MAX ||
        config->heartbeat_timeout_ms > INT_MAX ||
        strlen(config->local_identity) == 0U ||
        strlen(config->local_identity) > TR_RAFT_FLOWMQ_MAX_IDENTITY_SIZE) {
        free(service);
        return SALTS_ERANGE;
    }
    result = tr_raft_flowmq_copy_string(
        service->bind_endpoint, sizeof(service->bind_endpoint),
        config->bind_endpoint);
    if (result != SALTS_OK) {
        free(service);
        return result;
    }
    service->packet = (uint8_t *)malloc(TR_RAFT_TRANSPORT_MAX_PACKET_SIZE);
    service->context = flowmq_ctx_new();
    if (service->packet == NULL || service->context == NULL) {
        tr_raft_flowmq_release(service);
        return SALTS_ENOMEM;
    }
    service->router = flowmq_socket(service->context, FLOWMQ_ROUTER);
    if (service->router == NULL) {
        tr_raft_flowmq_release(service);
        return SALTS_ENOMEM;
    }
    result = tr_raft_flowmq_configure_common(service->router, config);
    if (result == SALTS_OK &&
        tr_raft_flowmq_is_tls_endpoint(config->bind_endpoint)) {
        result = tr_raft_flowmq_configure_tls(service->router, &config->tls,
                                               1);
    }
    if (result != SALTS_OK) {
        tr_raft_flowmq_release(service);
        return result;
    }

    for (index = 0U; index < service->peer_count; ++index) {
        tr_raft_flowmq_peer_t *peer = &service->peers[index];
        tr_raft_transport_session_config_t session_config;
        size_t prior;

        if (config->peers[index].node_id == 0U ||
            config->peers[index].node_id == config->protocol.local_node_id ||
            config->peers[index].handshake == NULL ||
            config->peers[index].identity == NULL ||
            config->peers[index].endpoint == NULL) {
            tr_raft_flowmq_release(service);
            return SALTS_EINVAL;
        }
        for (prior = 0U; prior < index; ++prior) {
            if (service->peers[prior].node_id == config->peers[index].node_id ||
                strcmp(service->peers[prior].identity,
                       config->peers[index].identity) == 0) {
                tr_raft_flowmq_release(service);
                return SALTS_EINVAL;
            }
        }
        peer->node_id = config->peers[index].node_id;
        result = tr_raft_flowmq_copy_string(
            peer->identity, sizeof(peer->identity),
            config->peers[index].identity);
        if (result == SALTS_OK) {
            result = tr_raft_flowmq_copy_string(
                peer->endpoint, sizeof(peer->endpoint),
                config->peers[index].endpoint);
        }
        if (result != SALTS_OK) {
            tr_raft_flowmq_release(service);
            return result;
        }
        {
            tr_raft_group_queue_config_t queue_config;

            memset(&queue_config, 0, sizeof(queue_config));
            queue_config.max_groups =
                service->outbound_limits.max_active_groups;
            queue_config.total_item_capacity =
                service->outbound_limits.total_item_capacity;
            queue_config.total_data_bytes =
                service->outbound_limits.total_data_bytes;
            queue_config.per_group_item_capacity =
                service->outbound_limits.per_group_item_capacity;
            queue_config.per_group_data_bytes =
                service->outbound_limits.per_group_data_bytes;
            queue_config.release = tr_raft_flowmq_release_owned;
            result = tr_raft_group_queue_init(
                &peer->outbound, &queue_config);
        }
        if (result != SALTS_OK) {
            tr_raft_flowmq_release(service);
            return result;
        }
        peer->packet =
            (uint8_t *)malloc(TR_RAFT_TRANSPORT_MAX_PACKET_SIZE);
        if (peer->packet == NULL) {
            tr_raft_flowmq_release(service);
            return SALTS_ENOMEM;
        }
        memset(&session_config, 0, sizeof(session_config));
        session_config.cluster_id = config->protocol.cluster_id;
        session_config.local_node_id = config->protocol.local_node_id;
        session_config.peer_node_id = peer->node_id;
        session_config.first_outbound_message_id = 1U;
        session_config.handshake = config->peers[index].handshake;
        session_config.on_payload = config->on_payload;
        session_config.payload_context = config->payload_context;
        result = tr_raft_transport_session_create(&session_config,
                                                   &peer->session);
        if (result != SALTS_OK) {
            tr_raft_flowmq_release(service);
            return result;
        }
        peer->dealer = flowmq_socket(service->context, FLOWMQ_DEALER);
        if (peer->dealer == NULL) {
            tr_raft_flowmq_release(service);
            return SALTS_ENOMEM;
        }
        result = tr_raft_flowmq_configure_common(peer->dealer, config);
        if (result == SALTS_OK) {
            result = flowmq_setsockopt(peer->dealer, FLOWMQ_IDENTITY,
                                       config->local_identity,
                                       strlen(config->local_identity));
        }
        if (result == SALTS_OK &&
            tr_raft_flowmq_is_tls_endpoint(peer->endpoint)) {
            result = tr_raft_flowmq_configure_tls(
                peer->dealer, &config->peers[index].tls, 0);
        }
        if (result != SALTS_OK) {
            tr_raft_flowmq_release(service);
            return result;
        }
    }
    *out_service = service;
    return SALTS_OK;
}

int tr_raft_flowmq_peer_service_start(tr_raft_flowmq_peer_service_t *service)
{
    size_t index;
    int result;

    if (service == NULL || service->stopping) {
        return SALTS_EINVAL;
    }
    if (service->started) {
        return SALTS_EALREADY;
    }
    result = flowmq_bind(service->router, service->bind_endpoint);
    if (result != SALTS_OK) {
        service->last_error = result;
        return result;
    }
    for (index = 0U; index < service->peer_count; ++index) {
        result = flowmq_connect(service->peers[index].dealer,
                                service->peers[index].endpoint);
        if (result != SALTS_OK) {
            service->last_error = result;
            return result;
        }
    }
    service->started = 1;
    return SALTS_OK;
}

static int tr_raft_flowmq_receive_one(
    tr_raft_flowmq_peer_service_t *service,
    tr_raft_flowmq_peer_service_step_result_t *step)
{
    char identity[TR_RAFT_FLOWMQ_MAX_IDENTITY_SIZE + 1U];
    tr_raft_flowmq_peer_t *peer;
    size_t identity_size = 0U;
    size_t packet_size = 0U;
    size_t option_size;
    int more = 0;
    int result;

    result = flowmq_recv(service->router, identity,
                         TR_RAFT_FLOWMQ_MAX_IDENTITY_SIZE, &identity_size,
                         FLOWMQ_DONTWAIT);
    if (result != SALTS_OK) {
        return result;
    }
    option_size = sizeof(more);
    result = flowmq_getsockopt(service->router, FLOWMQ_RCVMORE, &more,
                               &option_size);
    if (result != SALTS_OK || !more) {
        return result == SALTS_OK ? SALTS_EPROTO : result;
    }
    result = flowmq_recv(service->router, service->packet,
                         TR_RAFT_TRANSPORT_MAX_PACKET_SIZE, &packet_size,
                         FLOWMQ_DONTWAIT);
    if (result != SALTS_OK) {
        return result;
    }
    option_size = sizeof(more);
    result = flowmq_getsockopt(service->router, FLOWMQ_RCVMORE, &more,
                               &option_size);
    if (result != SALTS_OK || more) {
        return result == SALTS_OK ? SALTS_EPROTO : result;
    }
    peer = tr_raft_flowmq_find_identity(service, identity, identity_size);
    if (peer == NULL) {
        return SALTS_EPERM;
    }
    result = tr_raft_transport_feed(peer->session, service->packet,
                                    packet_size);
    if (result != SALTS_OK) {
        peer->faulted = 1;
        return result;
    }
    ++service->frames_received;
    ++step->received_frames;
    return SALTS_OK;
}

static int tr_raft_flowmq_send_peer(
    tr_raft_flowmq_peer_service_t *service,
    tr_raft_flowmq_peer_t *peer,
    tr_raft_flowmq_peer_service_step_result_t *step)
{
    size_t sent = 0U;

    while (sent < service->max_send_batch_items &&
           deque_size(&peer->outbound) != 0U) {
        const tr_raft_owned_transport_payload_t *owned =
            (const tr_raft_owned_transport_payload_t *)deque_at_const(
                &peer->outbound, 0U);
        tr_raft_owned_transport_payload_t discard;
        size_t data_bytes;
        int result;

        if (owned == NULL) {
            return SALTS_EPROTO;
        }
        if (!peer->packet_ready) {
            result = tr_raft_transport_encode_payload(
                peer->session, &owned->payload, peer->packet,
                TR_RAFT_TRANSPORT_MAX_PACKET_SIZE, &peer->packet_size);
            if (result != SALTS_OK) {
                peer->faulted = 1;
                return result;
            }
            peer->packet_ready = 1;
        }
        result = flowmq_send(peer->dealer, peer->packet, peer->packet_size,
                             FLOWMQ_DONTWAIT);
        if (result != SALTS_OK) {
            return result;
        }
        peer->packet_ready = 0;
        peer->packet_size = 0U;
        data_bytes = tr_raft_flowmq_payload_bytes(&owned->payload);
        if (deque_pop_front(&peer->outbound, &discard) != STL_OK) {
            return SALTS_EPROTO;
        }
        tr_raft_owned_transport_payload_release(&discard);
        peer->queued_data_bytes -= data_bytes;
        ++service->frames_sent;
        ++step->sent_frames;
        ++sent;
    }
    return SALTS_OK;
}

int tr_raft_flowmq_peer_service_step(
    tr_raft_flowmq_peer_service_t *service,
    tr_raft_flowmq_peer_service_step_result_t *out_result)
{
    flowmq_pollitem_t items[TR_RAFT_MAX_VOTERS];
    tr_raft_flowmq_peer_service_step_result_t step;
    size_t ready = 0U;
    size_t index;
    int result;

    if (service == NULL || out_result == NULL || !service->started ||
        service->stopping) {
        return SALTS_EINVAL;
    }
    if (service->step_active) {
        return SALTS_EBUSY;
    }
    service->step_active = 1;
    memset(&step, 0, sizeof(step));
    memset(items, 0, sizeof(items));
    items[0].socket = service->router;
    items[0].events = FLOWMQ_POLLIN;
    for (index = 0U; index < service->peer_count; ++index) {
        items[index + 1U].socket = service->peers[index].dealer;
        items[index + 1U].events = FLOWMQ_POLLOUT | FLOWMQ_POLLERR;
    }
    result = flowmq_poll(items, service->peer_count + 1U, 0U, &ready);
    if (result != SALTS_OK) {
        step.first_error = result;
    } else if ((items[0].revents & FLOWMQ_POLLIN) != 0) {
        for (index = 0U; index < service->max_receive_batch_items; ++index) {
            result = tr_raft_flowmq_receive_one(service, &step);
            if (result == SALTS_EBUSY) {
                break;
            }
            if (result != SALTS_OK) {
                step.first_error = result;
                break;
            }
        }
    }
    for (index = 0U; index < service->peer_count; ++index) {
        tr_raft_flowmq_peer_t *peer = &service->peers[index];

        if (peer->faulted || (items[index + 1U].revents & FLOWMQ_POLLERR) != 0) {
            ++step.failed_peer_count;
            continue;
        }
        if (deque_size(&peer->outbound) == 0U) {
            continue;
        }
        if ((items[index + 1U].revents & FLOWMQ_POLLOUT) == 0) {
            ++step.blocked_peer_count;
            continue;
        }
        result = tr_raft_flowmq_send_peer(service, peer, &step);
        if (result != SALTS_OK) {
            if (tr_raft_flowmq_transient(result)) {
                ++step.blocked_peer_count;
            } else {
                peer->faulted = 1;
                ++step.failed_peer_count;
                if (step.first_error == SALTS_OK) {
                    step.first_error = result;
                }
            }
        }
    }
    service->step_active = 0;
    service->last_error = step.first_error;
    *out_result = step;
    return step.first_error;
}

int tr_raft_flowmq_peer_service_stop(tr_raft_flowmq_peer_service_t *service)
{
    size_t index;
    int first_error = SALTS_OK;
    int result;

    if (service == NULL) {
        return SALTS_EINVAL;
    }
    if (service->step_active) {
        return SALTS_EBUSY;
    }
    if (service->stopping) {
        return SALTS_OK;
    }
    service->stopping = 1;
    for (index = 0U; index < service->peer_count; ++index) {
        if (service->peers[index].dealer != NULL) {
            result = flowmq_close(service->peers[index].dealer);
            if (result != SALTS_OK && first_error == SALTS_OK) {
                first_error = result;
            }
            service->peers[index].dealer = NULL;
        }
    }
    if (service->router != NULL) {
        result = flowmq_close(service->router);
        if (result != SALTS_OK && first_error == SALTS_OK) {
            first_error = result;
        }
        service->router = NULL;
    }
    if (service->context != NULL) {
        result = flowmq_ctx_term(service->context);
        if (result != SALTS_OK && first_error == SALTS_OK) {
            first_error = result;
        }
        if (result == SALTS_OK) {
            service->context = NULL;
        }
    }
    service->last_error = first_error;
    return first_error;
}

int tr_raft_flowmq_peer_service_destroy(tr_raft_flowmq_peer_service_t *service)
{
    if (service == NULL) {
        return SALTS_OK;
    }
    if (service->step_active || (service->started && !service->stopping)) {
        return SALTS_EBUSY;
    }
    tr_raft_flowmq_release(service);
    return SALTS_OK;
}

int tr_raft_flowmq_peer_service_enqueue_group(
    tr_raft_flowmq_peer_service_t *service,
    tr_raft_group_id_t group_id,
    const tr_raft_message_t *message)
{
    tr_raft_transport_payload_t payload;

    if (service == NULL || message == NULL || group_id == 0U) {
        return SALTS_EINVAL;
    }
    memset(&payload, 0, sizeof(payload));
    payload.group_id = group_id;
    payload.kind = TR_RAFT_WIRE_PAYLOAD_RAFT;
    payload.data.raft = *message;
    return tr_raft_flowmq_peer_service_enqueue_payload(service, &payload);
}

int tr_raft_flowmq_peer_service_enqueue_payload(
    tr_raft_flowmq_peer_service_t *service,
    const tr_raft_transport_payload_t *payload)
{
    tr_raft_flowmq_peer_t *peer;
    tr_raft_owned_transport_payload_t owned;
    size_t data_bytes;
    int result;

    if (service == NULL || payload == NULL) {
        return SALTS_EINVAL;
    }
    if (service->stopping) {
        return SALTS_EPIPE;
    }
    peer = tr_raft_flowmq_find_node(service,
                                    tr_raft_flowmq_payload_to(payload));
    if (peer == NULL) {
        return SALTS_ENOENT;
    }
    if (peer->faulted) {
        return SALTS_EPROTO;
    }
    if (deque_size(&peer->outbound) >= service->outbound_queue_capacity) {
        return SALTS_ENOSPC;
    }
    data_bytes = tr_raft_flowmq_payload_bytes(payload);
    if (data_bytes > service->max_inflight_data_bytes -
                         peer->queued_data_bytes) {
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
        return result;
    }
    peer->queued_data_bytes += data_bytes;
    return SALTS_OK;
}

int tr_raft_flowmq_peer_service_get_status(
    const tr_raft_flowmq_peer_service_t *service,
    tr_raft_flowmq_peer_service_status_t *out_status)
{
    size_t index;

    if (service == NULL || out_status == NULL) {
        return SALTS_EINVAL;
    }
    memset(out_status, 0, sizeof(*out_status));
    out_status->peer_count = service->peer_count;
    out_status->outbound_queue_capacity = service->outbound_queue_capacity;
    out_status->max_inflight_data_bytes = service->max_inflight_data_bytes;
    out_status->frames_sent = service->frames_sent;
    out_status->frames_received = service->frames_received;
    out_status->started = service->started;
    out_status->stopping = service->stopping;
    out_status->step_active = service->step_active;
    out_status->last_error = service->last_error;
    for (index = 0U; index < service->peer_count; ++index) {
        out_status->queued_payload_count +=
            deque_size(&service->peers[index].outbound);
        out_status->queued_data_bytes +=
            service->peers[index].queued_data_bytes;
    }
    return SALTS_OK;
}
