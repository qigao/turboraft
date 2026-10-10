#include <turboraft/raft_cnet_managed_peer.h>

#include <cmeta_error.h>

#include <stdlib.h>
#include <string.h>

/* The managed dial already owns its reconnect state and Manager reservation;
 * this outer object only installs a fresh authenticated Raft Channel for each
 * new generation. Never copy a live CNetManager/ManagedDial wrapper. */
struct tr_raft_cnet_managed_peer {
    cnet_manager *manager;
    cnet_client *client;
    cnet_managed_dial dial;
    tr_raft_cnet_channel_t *channel;
    tr_raft_cnet_channel_config_t channel_config;
    tr_raft_cnet_identity_policy_t single_identity;
    tr_raft_cnet_peer_identity_t peer_identity;
    cnet_observer observer;
    tr_raft_node_id_t peer_node_id;
    size_t connections_started;
    size_t protocol_ready_count;
    size_t security_rejections;
    int current_protocol_ready;
    int stopped;
};

static int tr_managed_peer_security_status(int status)
{
    return status == SALTS_EPROTO ||
           status == SALTS_EPROTONOSUPPORT ||
           status == SALTS_EINVAL ||
           status == SALTS_EPERM;
}

/* Called by Salts::CNetManagedDial before forwarding the terminal callback.
 * A known connection/timeout/refusal is transient; everything else is sealed
 * until explicitly reconfigured. In particular a Raft HELLO/CA/identity
 * rejection must not reconnect using a weaker identity or plaintext. */
static cnet_reconnect_failure_kind tr_managed_peer_classify(
    void *context,
    cnet_connection_state state,
    const cnet_error *error)
{
    tr_raft_cnet_managed_peer_t *peer =
        (tr_raft_cnet_managed_peer_t *)context;
    tr_raft_cnet_channel_status_t status = {0};
    int err = SALTS_OK;

    if (peer != NULL && peer->channel != NULL &&
        tr_raft_cnet_channel_get_status(peer->channel, &status) == SALTS_OK)
        err = status.last_error;

    if (tr_managed_peer_security_status(err))
        goto security;
    if (error != NULL) {
        if (tr_managed_peer_security_status(error->status))
            goto security;
        /* CNet TLS trust/identity handshake failure has no permitted
         * plaintext or unverified reconnection fallback. */
        if (error->stage != NULL &&
            (strstr(error->stage, "tls") != NULL ||
             strstr(error->stage, "TLS") != NULL ||
             strstr(error->stage, "certificate") != NULL ||
             strstr(error->stage, "handshake") != NULL))
            goto security;

        if (error->status == SALTS_ECONNREFUSED ||
            error->status == SALTS_ECONNRESET ||
            error->status == SALTS_ETIMEDOUT ||
            error->status == SALTS_EHOSTUNREACH ||
            error->status == SALTS_ENETUNREACH ||
            error->status == SALTS_ECONNABORTED ||
            error->status == SALTS_EPIPE)
            return CNET_RECONNECT_TRANSIENT;
    }

    /* A normal close of a previously fully-authorized transport may retry
     * establishment (never application payload settlement). */
    if (state == CNET_CONNECTION_CLOSED && err == SALTS_OK &&
        peer != NULL && peer->current_protocol_ready)
        return CNET_RECONNECT_TRANSIENT;

security:
    if (peer != NULL) ++peer->security_rejections;
    return CNET_RECONNECT_SECURITY;
}

static void tr_managed_peer_on_state(
    void *context,
    cnet_connection connection,
    cnet_connection_state state,
    const cnet_error *error)
{
    tr_raft_cnet_managed_peer_t *peer =
        (tr_raft_cnet_managed_peer_t *)context;
    if (peer->observer.on_state != NULL)
        peer->observer.on_state(peer->observer.user, connection, state, error);
}

static void tr_managed_peer_on_receive(
    void *context, cnet_connection connection,
    const cnet_receive_view *view)
{
    tr_raft_cnet_managed_peer_t *peer =
        (tr_raft_cnet_managed_peer_t *)context;
    if (peer->observer.on_receive != NULL)
        peer->observer.on_receive(peer->observer.user, connection, view);
}

static void tr_managed_peer_on_send(
    void *context, cnet_connection connection, size_t bytes)
{
    tr_raft_cnet_managed_peer_t *peer =
        (tr_raft_cnet_managed_peer_t *)context;
    /* The ManagedDial owns the connection generation; the Channel owns
     * settlement metadata. Forward only on the same CNet progress Owner. */
    if (peer->observer.on_send != NULL)
        peer->observer.on_send(peer->observer.user, connection, bytes);
}

static int tr_managed_peer_fresh_channel(
    tr_raft_cnet_managed_peer_t *peer)
{
    int result;

    /* A new dial is forbidden while old Manager records or CNet terminal
     * callbacks still borrow the previous Channel. The caller must advance
     * Manager to recycle before reaching this boundary. */
    if (peer->channel != NULL) {
        result = tr_raft_cnet_channel_destroy(peer->channel);
        if (result != SALTS_OK) return result;
        peer->channel = NULL;
    }

    result = tr_raft_cnet_channel_create(&peer->channel_config,
                                          &peer->channel);
    if (result != SALTS_OK) return result;
    peer->observer = tr_raft_cnet_channel_observer(peer->channel);
    peer->current_protocol_ready = 0;
    return SALTS_OK;
}

int tr_raft_cnet_managed_peer_create(
    const tr_raft_cnet_managed_peer_config_t *config,
    tr_raft_cnet_managed_peer_t **out_peer)
{
    tr_raft_cnet_managed_peer_t *peer;
    tr_raft_handshake_message_t hello = {0};
    cnet_managed_dial_config dial = {0};
    size_t i;
    const tr_raft_cnet_peer_identity_t *target = NULL;
    int result;

    if (out_peer == NULL) return SALTS_EINVAL;
    *out_peer = NULL;

    if (config == NULL || config->manager == NULL ||
        config->channel.client == NULL ||
        config->channel.identity == NULL ||
        config->channel.on_payload == NULL ||
        config->channel.first_outbound_message_id == 0U ||
        config->expected_peer_node_id == 0U ||
        config->uri == NULL ||
        strncmp(config->uri, "tls://", 6U) != 0 ||
        config->tls == NULL ||
        config->tls->size != sizeof(*config->tls) ||
        config->recovery_episode_ms == 0U ||
        config->channel.handshake.local_node_id !=
            config->channel.identity->local_node_id ||
        config->expected_peer_node_id ==
            config->channel.handshake.local_node_id ||
        tr_raft_cnet_identity_policy_validate(
            config->channel.identity) != SALTS_OK ||
        tr_raft_handshake_make_hello(
            &config->channel.handshake, &hello) != SALTS_OK)
        return SALTS_EINVAL;

    for (i = 0U; i < config->channel.identity->peer_count; ++i) {
        if (config->channel.identity->peers[i].node_id ==
            config->expected_peer_node_id) {
            target = &config->channel.identity->peers[i];
            break;
        }
    }
    if (target == NULL) return SALTS_EINVAL;

    peer = (tr_raft_cnet_managed_peer_t *)calloc(1U, sizeof(*peer));
    if (peer == NULL) return SALTS_ENOMEM;

    peer->manager = config->manager;
    peer->client = config->channel.client;
    peer->peer_node_id = config->expected_peer_node_id;
    peer->peer_identity = *target;
    peer->single_identity = (tr_raft_cnet_identity_policy_t){
        config->channel.handshake.local_node_id, &peer->peer_identity, 1U
    };
    peer->channel_config = config->channel;
    peer->channel_config.identity = &peer->single_identity;

    result = tr_managed_peer_fresh_channel(peer);
    if (result != SALTS_OK) {
        free(peer);
        return result;
    }

    dial.size = sizeof(dial);
    dial.version = CNET_MANAGED_DIAL_VERSION;
    dial.manager = config->manager;
    dial.client = config->channel.client;
    dial.connection.uri = config->uri;
    dial.connection.tls = config->tls;
    dial.connection.observer = (cnet_observer){
        .on_state = tr_managed_peer_on_state,
        .on_receive = tr_managed_peer_on_receive,
        .on_send = tr_managed_peer_on_send,
        .user = peer
    };
    dial.recovery = config->reconnect;
    dial.recovery_episode_ms = config->recovery_episode_ms;
    dial.classify = tr_managed_peer_classify;
    dial.classify_user = peer;

    result = cnet_managed_dial_init(&peer->dial, &dial);
    if (result != SALTS_OK) {
        (void)tr_raft_cnet_channel_destroy(peer->channel);
        free(peer);
        return result;
    }

    *out_peer = peer;
    return SALTS_OK;
}

int tr_raft_cnet_managed_peer_advance(
    tr_raft_cnet_managed_peer_t *peer,
    uint64_t now_ms,
    uint64_t *out_wait_ms)
{
    cnet_managed_dial_snapshot dial = {0};
    tr_raft_cnet_channel_status_t channel = {0};
    int result;

    if (out_wait_ms == NULL) return SALTS_EINVAL;
    *out_wait_ms = 0U;
    if (peer == NULL) return SALTS_EINVAL;
    if (peer->stopped) return SALTS_ESHUTDOWN;

    result = cnet_managed_dial_get_snapshot(&peer->dial, &dial);
    if (result != SALTS_OK) return result;
    if (peer->channel != NULL) {
        result = tr_raft_cnet_channel_get_status(
            peer->channel, &channel);
        if (result != SALTS_OK) return result;
    }

    if (channel.phase == TR_RAFT_CNET_CHANNEL_ACTIVE &&
        !peer->current_protocol_ready) {
        /* Connected is deliberately insufficient. Exact Raft HELLO/ACK
         * + CNet verified TLS determines protocol_ready, no other path. */
        result = cnet_managed_dial_protocol_ready(
            &peer->dial, dial.recovery_ticket, now_ms);
        if (result != SALTS_OK) return result;
        peer->current_protocol_ready = 1;
        ++peer->protocol_ready_count;
        return SALTS_OK;
    }

    if (peer->current_protocol_ready &&
        channel.phase == TR_RAFT_CNET_CHANNEL_ACTIVE)
        return SALTS_OK;

    /* A recycled physical Manager record frees this one dial for another
     * explicit recovery attempt. Destroy channel only AFTER its last
     * terminal callback; never mutate its lifetime while CNet borrows it. */
    if (dial.managed.slot != 0U) return SALTS_EBUSY;
    if (channel.phase == TR_RAFT_CNET_CHANNEL_CLOSED ||
        channel.phase == TR_RAFT_CNET_CHANNEL_FAILED) {
        result = tr_managed_peer_fresh_channel(peer);
        if (result != SALTS_OK) return result;
    } else if (peer->channel == NULL) {
        result = tr_managed_peer_fresh_channel(peer);
        if (result != SALTS_OK) return result;
    } else if (channel.phase != TR_RAFT_CNET_CHANNEL_CREATED) {
        return SALTS_EBUSY;
    }

    result = cnet_managed_dial_advance(
        &peer->dial, now_ms, out_wait_ms);
    if (result == SALTS_OK) {
        cnet_managed_dial_snapshot latest = {0};
        result = cnet_managed_dial_get_snapshot(&peer->dial, &latest);
        if (result != SALTS_OK) return result;
        result = tr_raft_cnet_channel_attach(
            peer->channel, latest.connection);
        if (result == SALTS_OK) ++peer->connections_started;
    }
    return result;
}

int tr_raft_cnet_managed_peer_send(
    tr_raft_cnet_managed_peer_t *peer,
    const tr_raft_transport_payload_t *payload)
{
    int result;

    if (peer == NULL || payload == NULL) return SALTS_EINVAL;
    if (peer->stopped) return SALTS_ESHUTDOWN;
    if (!peer->current_protocol_ready) return SALTS_ENOSPC;
    if (peer->channel == NULL) return SALTS_ENOSPC;
    result = tr_raft_cnet_channel_send(peer->channel, payload);
    return (result == SALTS_EBUSY || result == SALTS_ENOBUFS)
               ? SALTS_ENOSPC : result;
}

int tr_raft_cnet_managed_peer_capture_reply_origin(
    tr_raft_cnet_managed_peer_t *peer, tr_raft_group_id_t group_id,
    tr_raft_transport_reply_origin_t *out_origin)
{
    if (out_origin == NULL) return SALTS_EINVAL;
    *out_origin = (tr_raft_transport_reply_origin_t){0};
    if (peer == NULL || group_id == 0U) return SALTS_EINVAL;
    if (peer->stopped || !peer->current_protocol_ready ||
        peer->channel == NULL)
        return SALTS_EBUSY;
    return tr_raft_cnet_channel_capture_reply_origin(
        peer->channel, group_id, out_origin);
}

int tr_raft_cnet_managed_peer_send_chunk_completion(
    tr_raft_cnet_managed_peer_t *peer,
    const tr_raft_multicore_completion_t *completion)
{
    if (peer == NULL || completion == NULL) return SALTS_EINVAL;
    if (peer->stopped || !peer->current_protocol_ready ||
        peer->channel == NULL)
        return SALTS_ECANCELED;
    /* Never resolve a stale N callback through the current dial N+1:
     * Channel enforces strict incarnation, authenticated Node/Group and
     * CNet physical slot+generation before consuming a single send credit. */
    return tr_raft_cnet_channel_send_chunk_completion(
        peer->channel, completion);
}

static int tr_managed_group_enqueue(
    void *context,
    const tr_raft_message_t *message)
{
    tr_raft_cnet_managed_group_binding_t *binding =
        (tr_raft_cnet_managed_group_binding_t *)context;
    tr_raft_transport_payload_t payload = {0};

    if (binding == NULL || binding->peer == NULL ||
        binding->group_id == 0U || message == NULL)
        return SALTS_EINVAL;

    payload.group_id = binding->group_id;
    payload.kind = TR_RAFT_WIRE_PAYLOAD_RAFT;
    payload.data.raft = *message;
    return tr_raft_cnet_managed_peer_send(binding->peer, &payload);
}

int tr_raft_cnet_managed_group_transport_bind(
    tr_raft_cnet_managed_group_binding_t *binding,
    tr_raft_transport_t *transport)
{
    if (binding == NULL || binding->peer == NULL ||
        binding->group_id == 0U || transport == NULL)
        return SALTS_EINVAL;
    if (transport->enqueue != NULL)
        return SALTS_EALREADY;

    transport->context = binding;
    transport->enqueue = tr_managed_group_enqueue;
    return SALTS_OK;
}

int tr_raft_cnet_managed_peer_get_status(
    tr_raft_cnet_managed_peer_t *peer,
    tr_raft_cnet_managed_peer_status_t *out_status)
{
    int result;

    if (peer == NULL || out_status == NULL) return SALTS_EINVAL;
    *out_status = (tr_raft_cnet_managed_peer_status_t){0};
    result = cnet_managed_dial_get_snapshot(
        &peer->dial, &out_status->dial);
    if (result != SALTS_OK) return result;
    if (peer->channel != NULL) {
        result = tr_raft_cnet_channel_get_status(
            peer->channel, &out_status->channel);
        if (result != SALTS_OK) return result;
    }
    out_status->peer_node_id = peer->peer_node_id;
    out_status->connections_started = peer->connections_started;
    out_status->protocol_ready_count = peer->protocol_ready_count;
    out_status->security_rejections = peer->security_rejections;
    out_status->stopped = peer->stopped;
    return SALTS_OK;
}

int tr_raft_cnet_managed_peer_stop(
    tr_raft_cnet_managed_peer_t *peer)
{
    int result;

    if (peer == NULL) return SALTS_EINVAL;
    if (peer->stopped) return SALTS_OK;
    result = cnet_managed_dial_seal(&peer->dial);
    if (result != SALTS_OK) return result;
    peer->stopped = 1;
    if (peer->channel != NULL)
        (void)tr_raft_cnet_channel_stop(peer->channel);
    return SALTS_OK;
}

int tr_raft_cnet_managed_peer_destroy(
    tr_raft_cnet_managed_peer_t *peer)
{
    int result;

    if (peer == NULL) return SALTS_OK;
    if (!peer->stopped) return SALTS_EBUSY;
    result = cnet_managed_dial_destroy(&peer->dial);
    if (result != SALTS_OK) return result;
    result = tr_raft_cnet_channel_destroy(peer->channel);
    if (result != SALTS_OK) return result;
    free(peer);
    return SALTS_OK;
}
