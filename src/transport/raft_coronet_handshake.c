#include <turboraft/raft_coronet_transport.h>

#include <turbo_error.h>

#include <string.h>

int tr_raft_coronet_handshake_exchange(
    coro_socket_t *socket,
    const tr_raft_coronet_handshake_config_t *config,
    tr_raft_handshake_result_t *out_result,
    tr_raft_coronet_receive_remainder_t *out_remainder)
{
    tr_raft_handshake_exchange_t *exchange = NULL;
    tr_raft_handshake_exchange_state_t state =
        TR_RAFT_HANDSHAKE_EXCHANGE_NEW;
    char certificate_sha256[CORO_TLS_PEER_CERT_SHA256_CAPACITY];
    tr_raft_node_id_t authenticated_peer_node_id = 0U;
    uint8_t outbound[TR_RAFT_HANDSHAKE_PACKET_SIZE];
    size_t outbound_size = 0U;
    int result;

    if (socket == NULL || config == NULL || config->handshake == NULL ||
        config->timeout_ms == 0U || config->resolve_peer_identity == NULL ||
        out_result == NULL || out_remainder == NULL) {
        return TURBO_EINVAL;
    }
    memset(out_result, 0, sizeof(*out_result));
    memset(out_remainder, 0, sizeof(*out_remainder));
    memset(certificate_sha256, 0, sizeof(certificate_sha256));

    coro_socket_set_timeout(socket, config->timeout_ms);
    result = coro_socket_tls_get_verified_peer_certificate_sha256(
        socket, certificate_sha256);
    if (result != TURBO_OK) {
        return result;
    }
    result = config->resolve_peer_identity(config->identity_context,
                                           certificate_sha256,
                                           &authenticated_peer_node_id);
    if (result != TURBO_OK) {
        return result;
    }
    if (authenticated_peer_node_id == 0U) {
        return TURBO_EPROTO;
    }

    result = tr_raft_handshake_exchange_create(
        config->handshake, authenticated_peer_node_id, &exchange);
    if (result != TURBO_OK) {
        return result;
    }
    result = tr_raft_handshake_exchange_start(
        exchange, outbound, sizeof(outbound), &outbound_size);
    if (result == TURBO_OK) {
        result = coro_socket_send(socket, (const char *) outbound,
                                  outbound_size);
    }

    while (result == TURBO_OK) {
        char *received = NULL;
        size_t received_size = 0U;
        size_t consumed_size = 0U;

        result = coro_socket_recv(socket, &received, &received_size);
        if (result != TURBO_OK) {
            break;
        }
        if (received == NULL || received_size == 0U) {
            coro_socket_free_recv(received);
            result = TURBO_EIO;
            break;
        }
        outbound_size = 0U;
        result = tr_raft_handshake_exchange_feed(
            exchange, (const uint8_t *) received, received_size,
            &consumed_size, outbound, sizeof(outbound), &outbound_size);
        if (result == TURBO_OK && outbound_size != 0U) {
            result = coro_socket_send(socket, (const char *) outbound,
                                      outbound_size);
        }
        if (result == TURBO_OK) {
            result = tr_raft_handshake_exchange_get_state(exchange, &state);
        }
        if (result == TURBO_OK &&
            state == TR_RAFT_HANDSHAKE_EXCHANGE_COMPLETE) {
            result = tr_raft_handshake_exchange_get_result(exchange,
                                                            out_result);
            if (result == TURBO_OK && consumed_size < received_size) {
                out_remainder->receive_buffer = received;
                out_remainder->data =
                    (const uint8_t *) received + consumed_size;
                out_remainder->size = received_size - consumed_size;
                received = NULL;
            }
            coro_socket_free_recv(received);
            break;
        }
        coro_socket_free_recv(received);
    }

    tr_raft_handshake_exchange_destroy(exchange);
    if (result != TURBO_OK) {
        memset(out_result, 0, sizeof(*out_result));
        tr_raft_coronet_receive_remainder_release(out_remainder);
    }
    return result;
}

void tr_raft_coronet_receive_remainder_release(
    tr_raft_coronet_receive_remainder_t *remainder)
{
    if (remainder == NULL) {
        return;
    }
    coro_socket_free_recv(remainder->receive_buffer);
    memset(remainder, 0, sizeof(*remainder));
}

int tr_raft_coronet_receive_remainder_apply(
    tr_raft_coronet_session_t *session,
    tr_raft_coronet_receive_remainder_t *remainder)
{
    int result;

    if (session == NULL || remainder == NULL ||
        remainder->receive_buffer == NULL || remainder->data == NULL ||
        remainder->size == 0U) {
        return TURBO_EINVAL;
    }
    result = tr_raft_coronet_feed(session, remainder->data, remainder->size);
    tr_raft_coronet_receive_remainder_release(remainder);
    return result;
}
