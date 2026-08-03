#include <turboraft/raft_coronet_transport.h>

#include "raft_coronet_transport_internal.h"

#include <turbo_error.h>

#include <stdlib.h>

struct tr_raft_coronet_inbound_service {
    tr_raft_coronet_inbound_service_config_t config;
    tr_raft_coronet_inbound_status_t status;
};

int tr_raft_coronet_inbound_service_config_validate(
    const tr_raft_coronet_inbound_service_config_t *config)
{
    if (config == NULL || config->manager == NULL ||
        config->admit_owned_socket == NULL || config->on_result == NULL ||
        config->admission.handshake.handshake == NULL ||
        config->admission.handshake.timeout_ms == 0U ||
        config->admission.handshake.resolve_peer_identity == NULL ||
        config->admission.direction != TR_RAFT_CORONET_CONNECTION_INBOUND ||
        config->admission.expected_peer_node_id != 0U ||
        config->admission.first_outbound_message_id == 0U ||
        config->admission.peer_idle_timeout_ms == 0U ||
        config->admission.on_message == NULL) {
        return TURBO_EINVAL;
    }
    return TURBO_OK;
}

int tr_raft_coronet_inbound_service_create(
    const tr_raft_coronet_inbound_service_config_t *config,
    tr_raft_coronet_inbound_service_t **out_service)
{
    tr_raft_coronet_inbound_service_t *service;
    int result;

    if (out_service == NULL) {
        return TURBO_EINVAL;
    }
    *out_service = NULL;
    result = tr_raft_coronet_inbound_service_config_validate(config);
    if (result != TURBO_OK) {
        return result;
    }
    service = (tr_raft_coronet_inbound_service_t *) calloc(
        1U, sizeof(*service));
    if (service == NULL) {
        return TURBO_ENOMEM;
    }
    service->config = *config;
    *out_service = service;
    return TURBO_OK;
}

int tr_raft_coronet_inbound_service_destroy(
    tr_raft_coronet_inbound_service_t *service)
{
    if (service == NULL) {
        return TURBO_EINVAL;
    }
    if (service->status.active_admission_count != 0U) {
        return TURBO_EBUSY;
    }
    free(service);
    return TURBO_OK;
}

int tr_raft_coronet_inbound_service_admit_internal(
    tr_raft_coronet_inbound_service_t *service,
    coro_socket_t *socket,
    tr_raft_node_id_t *out_peer_node_id)
{
    tr_raft_node_id_t peer_node_id = 0U;
    int result;

    if (service == NULL || out_peer_node_id == NULL) {
        return TURBO_EINVAL;
    }
    *out_peer_node_id = 0U;
    if (socket == NULL) {
        result = TURBO_EINVAL;
    } else {
        ++service->status.active_admission_count;
        ++service->status.accepted_socket_count;
        result = service->config.admit_owned_socket(
            service->config.manager, socket, &service->config.admission,
            &peer_node_id);
        if (result == TURBO_OK) {
            ++service->status.admitted_socket_count;
        } else {
            ++service->status.rejected_socket_count;
            peer_node_id = 0U;
        }
        service->status.last_error = result;
        service->config.on_result(service->config.result_context, result,
                                  peer_node_id);
        --service->status.active_admission_count;
        *out_peer_node_id = peer_node_id;
        return result;
    }

    service->status.last_error = result;
    ++service->status.rejected_socket_count;
    service->config.on_result(service->config.result_context, result, 0U);
    return result;
}

void tr_raft_coronet_inbound_service_handle(coro_socket_t *socket,
                                             void *context)
{
    tr_raft_coronet_inbound_service_t *service =
        (tr_raft_coronet_inbound_service_t *) context;
    tr_raft_node_id_t peer_node_id;

    if (service == NULL) {
        coro_socket_destroy(socket);
        return;
    }
    (void) tr_raft_coronet_inbound_service_admit_internal(
        service, socket, &peer_node_id);
}

int tr_raft_coronet_inbound_service_get_status(
    const tr_raft_coronet_inbound_service_t *service,
    tr_raft_coronet_inbound_status_t *out_status)
{
    if (service == NULL || out_status == NULL) {
        return TURBO_EINVAL;
    }
    *out_status = service->status;
    return TURBO_OK;
}
