#include <turboraft/raft_coronet_transport.h>

#include <turbo_error.h>

static int tr_raft_coronet_host_is_valid(const char *host)
{
    return host != NULL && host[0] != '\0';
}

int tr_raft_coronet_outbound_config_validate(
    const tr_raft_coronet_outbound_config_t *config)
{
    if (config == NULL || config->context == NULL ||
        !tr_raft_coronet_host_is_valid(config->connect_host) ||
        !tr_raft_coronet_host_is_valid(config->request_host) ||
        config->port <= 0 || config->port > 65535 ||
        config->connect_timeout_ms == 0U || config->tls.verify_peer != 1 ||
        config->admission.handshake.handshake == NULL ||
        config->admission.handshake.timeout_ms == 0U ||
        config->admission.handshake.resolve_peer_identity == NULL ||
        config->admission.direction !=
            TR_RAFT_CORONET_CONNECTION_OUTBOUND ||
        config->admission.expected_peer_node_id == 0U ||
        config->admission.first_outbound_message_id == 0U ||
        config->admission.peer_idle_timeout_ms == 0U ||
        config->admission.on_message == NULL) {
        return TURBO_EINVAL;
    }
    return TURBO_OK;
}

int tr_raft_coronet_peer_manager_connect_outbound(
    tr_raft_coronet_peer_manager_t *manager,
    const tr_raft_coronet_outbound_config_t *config,
    tr_raft_node_id_t *out_peer_node_id)
{
    coro_socket_t *socket;
    int result;

    if (manager == NULL || out_peer_node_id == NULL) {
        return TURBO_EINVAL;
    }
    *out_peer_node_id = 0U;
    result = tr_raft_coronet_outbound_config_validate(config);
    if (result != TURBO_OK) {
        return result;
    }

    socket = coro_socket_create(config->context, CORO_SOCKET_TLS);
    if (socket == NULL) {
        return TURBO_ENOMEM;
    }
    coro_socket_set_timeout(socket, config->connect_timeout_ms);
    result = coro_socket_set_tls_client_config(socket, &config->tls);
    if (result != TURBO_OK) {
        coro_socket_destroy(socket);
        return result;
    }
    result = coro_socket_connect_host_ex(socket,
                                         config->connect_host,
                                         config->port,
                                         config->request_host);
    if (result != TURBO_OK) {
        coro_socket_destroy(socket);
        return result;
    }

    return tr_raft_coronet_peer_manager_admit_owned_socket(
        manager, socket, &config->admission, out_peer_node_id);
}
