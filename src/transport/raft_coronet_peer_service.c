#include <turboraft/raft_coronet_peer_service.h>

#include "raft_coronet_transport_internal.h"

#include <CoroNet/turbo_coro_context.h>
#include <turbo_deque.h>
#include <turbo_error.h>

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

TURBO_DEQUE_DEFINE(tr_raft_coronet_payload_queue_t,
                   tr_raft_coronet_payload_t)

typedef struct tr_raft_coronet_reader_slot {
    tr_raft_coronet_peer_service_t *service;
    tr_raft_node_id_t peer_node_id;
    uint64_t reconnect_at_ms;
    int active;
    int reconnect_requested;
} tr_raft_coronet_reader_slot_t;

struct tr_raft_coronet_peer_service {
    coro_context_t *context;
    tr_raft_coronet_peer_manager_t *manager;
    tr_raft_coronet_identity_registry_t *identity_registry;
    tr_raft_coronet_inbound_service_t *inbound_service;
    tr_raft_coronet_dial_scheduler_t *schedulers[TR_RAFT_MAX_VOTERS - 1U];
    tr_raft_node_id_t scheduler_peer_ids[TR_RAFT_MAX_VOTERS - 1U];
    tr_raft_node_id_t peer_node_ids[TR_RAFT_MAX_VOTERS - 1U];
    tr_raft_coronet_payload_queue_t outbound_queues[
        TR_RAFT_MAX_VOTERS - 1U];
    tr_raft_coronet_reader_slot_t readers[TR_RAFT_MAX_VOTERS - 1U];
    size_t scheduler_count;
    size_t peer_count;
    tr_raft_node_id_t local_node_id;
    uint64_t identity_generation;
    uint32_t active_operation_count;
    size_t outbound_queue_capacity;
    size_t active_reader_count;
    int step_active;
    int writer_active;
    int stopping;
    int last_pump_error;
    tr_raft_coronet_admit_owned_socket_fn admit_owned_socket;
    tr_raft_coronet_connect_outbound_fn connect_outbound;
    tr_raft_snapshot_receiver_t *snapshot_receiver;
    tr_raft_coronet_snapshot_handler_fn on_snapshot_ack;
    void *snapshot_ack_context;
    uint64_t snapshot_install_count;
    uint64_t snapshot_reject_count;
    uint64_t snapshot_ack_count;
};

static int tr_raft_coronet_peer_service_find_peer(
    const tr_raft_coronet_peer_service_t *service,
    tr_raft_node_id_t peer_node_id,
    size_t *out_index)
{
    size_t left = 0U;
    size_t right;

    if (service == NULL || peer_node_id == 0U) {
        return TURBO_EINVAL;
    }
    right = service->peer_count;
    while (left < right) {
        size_t middle = left + (right - left) / 2U;
        if (service->peer_node_ids[middle] == peer_node_id) {
            if (out_index != NULL) {
                *out_index = middle;
            }
            return TURBO_OK;
        }
        if (peer_node_id < service->peer_node_ids[middle]) {
            right = middle;
        } else {
            left = middle + 1U;
        }
    }
    return TURBO_EPROTO;
}

static int tr_raft_coronet_peer_service_identities_validate(
    const tr_raft_coronet_peer_service_t *service,
    const tr_raft_coronet_identity_entry_t *entries,
    size_t entry_count)
{
    size_t peer_index;
    size_t entry_index;

    if (service == NULL || entries == NULL || entry_count == 0U) {
        return TURBO_EINVAL;
    }
    for (entry_index = 0U; entry_index < entry_count; ++entry_index) {
        if (tr_raft_coronet_peer_service_find_peer(
                service, entries[entry_index].node_id, NULL) != TURBO_OK) {
            return TURBO_EINVAL;
        }
    }
    for (peer_index = 0U; peer_index < service->peer_count; ++peer_index) {
        int found = 0;
        for (entry_index = 0U; entry_index < entry_count; ++entry_index) {
            if (entries[entry_index].node_id ==
                service->peer_node_ids[peer_index]) {
                found = 1;
                break;
            }
        }
        if (!found) {
            return TURBO_EINVAL;
        }
    }
    return TURBO_OK;
}

static int tr_raft_coronet_peer_service_resolve_identity(
    void *context,
    const char *verified_certificate_sha256,
    tr_raft_node_id_t *out_peer_node_id)
{
    tr_raft_coronet_peer_service_t *service =
        (tr_raft_coronet_peer_service_t *) context;

    if (service == NULL) {
        return TURBO_EINVAL;
    }
    return tr_raft_coronet_identity_registry_resolve(
        service->identity_registry, verified_certificate_sha256,
        out_peer_node_id);
}

static int tr_raft_coronet_peer_service_is_quiescent(
    const tr_raft_coronet_peer_service_t *service)
{
    tr_raft_coronet_inbound_status_t inbound_status;
    int result;

    if (service->active_operation_count != 0U || service->step_active) {
        return TURBO_EBUSY;
    }
    if (service->inbound_service == NULL) {
        return TURBO_OK;
    }
    result = tr_raft_coronet_inbound_service_get_status(
        service->inbound_service, &inbound_status);
    if (result != TURBO_OK) {
        return result;
    }
    return inbound_status.active_admission_count == 0U ? TURBO_OK
                                                        : TURBO_EBUSY;
}

static size_t tr_raft_coronet_peer_service_find_scheduler(
    const tr_raft_coronet_peer_service_t *service,
    tr_raft_node_id_t peer_node_id)
{
    size_t index;

    for (index = 0U; index < service->scheduler_count; ++index) {
        if (service->scheduler_peer_ids[index] == peer_node_id) {
            return index;
        }
    }
    return SIZE_MAX;
}

static size_t tr_raft_coronet_peer_service_queued_count(
    const tr_raft_coronet_peer_service_t *service)
{
    size_t index;
    size_t count = 0U;

    for (index = 0U; index < service->peer_count; ++index) {
        count += tr_raft_coronet_payload_queue_t_size(
            &service->outbound_queues[index]);
    }
    return count;
}

static void tr_raft_coronet_peer_service_writer(coro_t *coroutine,
                                                 void *context)
{
    tr_raft_coronet_peer_service_t *service =
        (tr_raft_coronet_peer_service_t *) context;
    int made_progress = 1;

    (void) coroutine;
    while (!service->stopping && made_progress) {
        size_t index;
        made_progress = 0;
        for (index = 0U; index < service->peer_count; ++index) {
            tr_raft_coronet_payload_t *front =
                tr_raft_coronet_payload_queue_t_front(
                    &service->outbound_queues[index]);
            tr_raft_coronet_payload_t payload;
            tr_raft_coronet_payload_t discarded;
            int result;

            if (front == NULL) {
                continue;
            }
            payload = *front;
            result = tr_raft_coronet_peer_manager_enqueue_payload(
                service->manager, &payload);
            if (result == TURBO_OK) {
                if (!tr_raft_coronet_payload_queue_t_pop_front(
                        &service->outbound_queues[index], &discarded)) {
                    service->last_pump_error = TURBO_EPROTO;
                    service->stopping = 1;
                    break;
                }
                made_progress = 1;
            } else if (result != TURBO_ENOTCONN && result != TURBO_EPROTO) {
                service->last_pump_error = result;
            }
        }
    }
    service->writer_active = 0;
    --service->active_operation_count;
}

static int tr_raft_coronet_peer_service_start_writer(
    tr_raft_coronet_peer_service_t *service)
{
    int result;

    if (service->writer_active || service->stopping ||
        tr_raft_coronet_peer_service_queued_count(service) == 0U) {
        return TURBO_OK;
    }
    service->writer_active = 1;
    ++service->active_operation_count;
    result = coro_context_spawn(service->context,
                                tr_raft_coronet_peer_service_writer,
                                service);
    if (result != TURBO_OK) {
        --service->active_operation_count;
        service->writer_active = 0;
    }
    return result;
}

static int tr_raft_coronet_peer_service_receive_snapshot(
    void *context,
    const tr_raft_coronet_payload_t *payload)
{
    tr_raft_coronet_peer_service_t *service =
        (tr_raft_coronet_peer_service_t *) context;

    if (service == NULL || payload == NULL) {
        return TURBO_EINVAL;
    }
    if (payload->kind == TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_ACK) {
        int result;

        if (service->on_snapshot_ack == NULL) {
            return TURBO_EPROTO;
        }
        result = service->on_snapshot_ack(service->snapshot_ack_context,
                                          payload);
        if (result == TURBO_OK) {
            ++service->snapshot_ack_count;
        }
        return result;
    }
    if (payload->kind == TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK) {
        tr_raft_snapshot_receive_result_t receive_result;
        tr_raft_coronet_payload_t ack_payload;
        int receive_status;
        int enqueue_status;

        if (service->snapshot_receiver == NULL) {
            return TURBO_EPROTO;
        }
        memset(&receive_result, 0, sizeof(receive_result));
        receive_status = tr_raft_snapshot_receiver_handle(
            service->snapshot_receiver, &payload->data.snapshot_chunk,
            &receive_result);
        memset(&ack_payload, 0, sizeof(ack_payload));
        ack_payload.kind = TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_ACK;
        ack_payload.data.snapshot_ack = receive_result.ack;
        enqueue_status = tr_raft_coronet_peer_service_enqueue_payload(
            service, &ack_payload);
        if (enqueue_status != TURBO_OK) {
            return enqueue_status;
        }
        if (receive_result.installed) {
            ++service->snapshot_install_count;
        }
        if (receive_status == TURBO_EPROTO) {
            ++service->snapshot_reject_count;
            return TURBO_OK;
        }
        return receive_status;
    }
    return TURBO_EPROTO;
}

static int tr_raft_coronet_peer_service_activate_reader(
    tr_raft_coronet_peer_service_t *service,
    tr_raft_node_id_t peer_node_id,
    tr_raft_coronet_reader_slot_t **out_slot)
{
    tr_raft_coronet_reader_slot_t *slot;
    size_t index;
    int result;

    if (service->stopping) {
        return TURBO_EPIPE;
    }
    result = tr_raft_coronet_peer_service_find_peer(service, peer_node_id,
                                                     &index);
    if (result != TURBO_OK) {
        return result;
    }
    slot = &service->readers[index];
    if (slot->active) {
        return TURBO_EBUSY;
    }
    slot->service = service;
    slot->peer_node_id = peer_node_id;
    slot->active = 1;
    ++service->active_reader_count;
    ++service->active_operation_count;
    *out_slot = slot;
    return TURBO_OK;
}

static void tr_raft_coronet_peer_service_run_reader(
    tr_raft_coronet_reader_slot_t *slot)
{
    tr_raft_coronet_peer_service_t *service = slot->service;
    size_t scheduler_index;
    int release_result;
    int result = TURBO_OK;

    while (!service->stopping) {
        result = tr_raft_coronet_peer_manager_receive_peer(
            service->manager, slot->peer_node_id);
        if (result != TURBO_OK) {
            break;
        }
    }
    release_result = tr_raft_coronet_peer_manager_release_peer(
        service->manager, slot->peer_node_id);
    if (release_result != TURBO_OK && !service->stopping) {
        service->last_pump_error = TURBO_EPROTO;
    }
    scheduler_index = tr_raft_coronet_peer_service_find_scheduler(
        service, slot->peer_node_id);
    if (slot->reconnect_requested && !service->stopping &&
        release_result == TURBO_OK) {
        if (scheduler_index == SIZE_MAX ||
            tr_raft_coronet_dial_scheduler_reset(
                service->schedulers[scheduler_index],
                slot->reconnect_at_ms) != TURBO_OK) {
            service->last_pump_error = TURBO_EPROTO;
        }
    } else if (result != TURBO_OK && !service->stopping) {
        service->last_pump_error = result;
        if (scheduler_index != SIZE_MAX) {
            tr_raft_coronet_dial_scheduler_reset(
                service->schedulers[scheduler_index], 0U);
        }
    }
    slot->reconnect_requested = 0;
    slot->reconnect_at_ms = 0U;
    slot->active = 0;
    --service->active_reader_count;
    --service->active_operation_count;
}

static void tr_raft_coronet_peer_service_reader(coro_t *coroutine,
                                                 void *context)
{
    (void) coroutine;
    tr_raft_coronet_peer_service_run_reader(
        (tr_raft_coronet_reader_slot_t *) context);
}

static int tr_raft_coronet_peer_service_start_reader(
    tr_raft_coronet_peer_service_t *service,
    tr_raft_node_id_t peer_node_id)
{
    tr_raft_coronet_reader_slot_t *slot;
    int result;

    result = tr_raft_coronet_peer_service_activate_reader(
        service, peer_node_id, &slot);
    if (result != TURBO_OK) {
        return result;
    }
    result = coro_context_spawn(service->context,
                                tr_raft_coronet_peer_service_reader,
                                slot);
    if (result != TURBO_OK) {
        --service->active_operation_count;
        --service->active_reader_count;
        slot->active = 0;
        return result;
    }
    result = tr_raft_coronet_peer_service_start_writer(service);
    if (result != TURBO_OK) {
        service->last_pump_error = result;
    }
    return TURBO_OK;
}

static void tr_raft_coronet_peer_service_destroy_queues(
    tr_raft_coronet_peer_service_t *service,
    size_t queue_count)
{
    size_t index;

    for (index = 0U; index < queue_count; ++index) {
        tr_raft_coronet_payload_queue_t_destroy(
            &service->outbound_queues[index]);
    }
}

int tr_raft_coronet_peer_service_create(
    const tr_raft_coronet_peer_service_config_t *config,
    tr_raft_coronet_peer_service_t **out_service)
{
    tr_raft_coronet_peer_service_t *service = NULL;
    size_t queue_count = 0U;
    int result = TURBO_OK;

    if (out_service == NULL) {
        return TURBO_EINVAL;
    }
    *out_service = NULL;
    if (config == NULL || config->context == NULL ||
        config->admit_owned_socket == NULL ||
        config->connect_outbound == NULL ||
        config->outbound_queue_capacity == 0U ||
        config->outbound_queue_capacity >
            TR_RAFT_CORONET_MAX_OUTBOUND_QUEUE_CAPACITY ||
        config->manager.peer_node_ids == NULL ||
        config->manager.peer_count == 0U ||
        config->manager.peer_count > TR_RAFT_MAX_VOTERS - 1U) {
        return TURBO_EINVAL;
    }

    service = (tr_raft_coronet_peer_service_t *) calloc(
        1U, sizeof(*service));
    if (service == NULL) {
        return TURBO_ENOMEM;
    }
    service->context = config->context;
    service->local_node_id = config->manager.local_node_id;
    service->peer_count = config->manager.peer_count;
    service->outbound_queue_capacity = config->outbound_queue_capacity;
    memcpy(service->peer_node_ids, config->manager.peer_node_ids,
           service->peer_count * sizeof(service->peer_node_ids[0]));
    service->admit_owned_socket = config->admit_owned_socket;
    service->connect_outbound = config->connect_outbound;
    service->snapshot_receiver = config->snapshot_receiver;
    service->on_snapshot_ack = config->on_snapshot_ack;
    service->snapshot_ack_context = config->snapshot_ack_context;

    for (queue_count = 0U; queue_count < service->peer_count; ++queue_count) {
        result = tr_raft_coronet_payload_queue_t_init(
            &service->outbound_queues[queue_count]);
        if (result != TURBO_OK) {
            break;
        }
        result = tr_raft_coronet_payload_queue_t_reserve(
            &service->outbound_queues[queue_count],
            service->outbound_queue_capacity);
        if (result != TURBO_OK) {
            ++queue_count;
            break;
        }
    }
    if (result != TURBO_OK) {
        tr_raft_coronet_peer_service_destroy_queues(service, queue_count);
        free(service);
        return result;
    }

    result = tr_raft_coronet_peer_manager_create(&config->manager,
                                                  &service->manager);
    if (result != TURBO_OK) {
        tr_raft_coronet_peer_service_destroy_queues(service,
                                                     service->peer_count);
        free(service);
        return result;
    }
    result = tr_raft_coronet_peer_service_identities_validate(
        service, config->identity_entries, config->identity_entry_count);
    if (result == TURBO_OK) {
        result = tr_raft_coronet_identity_registry_create(
            config->identity_entries, config->identity_entry_count,
            &service->identity_registry);
    }
    if (result != TURBO_OK) {
        tr_raft_coronet_peer_manager_destroy(service->manager);
        tr_raft_coronet_peer_service_destroy_queues(service,
                                                     service->peer_count);
        free(service);
        return result;
    }
    service->identity_generation = 1U;
    *out_service = service;
    return TURBO_OK;
}

int tr_raft_coronet_peer_service_destroy(
    tr_raft_coronet_peer_service_t *service)
{
    size_t index;
    int result;

    if (service == NULL) {
        return TURBO_OK;
    }
    result = tr_raft_coronet_peer_service_is_quiescent(service);
    if (result != TURBO_OK) {
        return result;
    }
    result = tr_raft_coronet_peer_manager_destroy(service->manager);
    if (result != TURBO_OK) {
        return result;
    }
    service->manager = NULL;
    for (index = 0U; index < service->scheduler_count; ++index) {
        tr_raft_coronet_dial_scheduler_destroy(service->schedulers[index]);
    }
    if (service->inbound_service != NULL) {
        result = tr_raft_coronet_inbound_service_destroy(
            service->inbound_service);
        if (result != TURBO_OK) {
            return result;
        }
    }
    tr_raft_coronet_identity_registry_destroy(service->identity_registry);
    tr_raft_coronet_peer_service_destroy_queues(service,
                                                 service->peer_count);
    free(service);
    return TURBO_OK;
}

int tr_raft_coronet_peer_service_stop(
    tr_raft_coronet_peer_service_t *service)
{
    tr_raft_coronet_inbound_status_t inbound_status;
    int result;

    if (service == NULL) {
        return TURBO_EINVAL;
    }
    if (service->stopping) {
        return TURBO_OK;
    }
    if (service->inbound_service != NULL) {
        result = tr_raft_coronet_inbound_service_get_status(
            service->inbound_service, &inbound_status);
        if (result != TURBO_OK) {
            return result;
        }
        if (inbound_status.active_admission_count != 0U) {
            return TURBO_EBUSY;
        }
    }
    service->stopping = 1;
    result = tr_raft_coronet_peer_manager_close_all(service->manager);
    if (result != TURBO_OK) {
        service->stopping = 0;
    }
    return result;
}

int tr_raft_coronet_peer_service_configure_inbound(
    tr_raft_coronet_peer_service_t *service,
    const tr_raft_coronet_inbound_service_config_t *config)
{
    tr_raft_coronet_inbound_service_config_t resolved;
    int result;

    if (service == NULL || config == NULL || service->inbound_service != NULL ||
        config->manager != NULL || config->admit_owned_socket != NULL ||
        config->admission.handshake.resolve_peer_identity != NULL ||
        config->admission.handshake.identity_context != NULL ||
        config->admission.on_snapshot != NULL ||
        config->admission.snapshot_context != NULL) {
        return TURBO_EINVAL;
    }
    result = tr_raft_coronet_peer_service_is_quiescent(service);
    if (result != TURBO_OK) {
        return result;
    }
    resolved = *config;
    resolved.manager = service->manager;
    resolved.admit_owned_socket = service->admit_owned_socket;
    resolved.admission.handshake.resolve_peer_identity =
        tr_raft_coronet_peer_service_resolve_identity;
    resolved.admission.handshake.identity_context = service;
    if (service->snapshot_receiver != NULL ||
        service->on_snapshot_ack != NULL) {
        resolved.admission.on_snapshot =
            tr_raft_coronet_peer_service_receive_snapshot;
        resolved.admission.snapshot_context = service;
    }
    result = tr_raft_coronet_inbound_service_create(
        &resolved, &service->inbound_service);
    return result;
}

void tr_raft_coronet_peer_service_handle_inbound(coro_socket_t *socket,
                                                  void *context)
{
    tr_raft_coronet_peer_service_t *service =
        (tr_raft_coronet_peer_service_t *) context;
    tr_raft_coronet_reader_slot_t *slot;
    tr_raft_node_id_t peer_node_id = 0U;
    int result;

    if (service == NULL || service->inbound_service == NULL ||
        service->stopping) {
        if (socket != NULL) {
            coro_socket_destroy(socket);
        }
        return;
    }
    result = tr_raft_coronet_inbound_service_admit_internal(
        service->inbound_service, socket, &peer_node_id);
    if (result != TURBO_OK) {
        return;
    }
    result = tr_raft_coronet_peer_service_activate_reader(
        service, peer_node_id, &slot);
    if (result != TURBO_OK) {
        service->last_pump_error = result;
        (void) tr_raft_coronet_peer_manager_release_peer(service->manager,
                                                          peer_node_id);
        return;
    }
    result = tr_raft_coronet_peer_service_start_writer(service);
    if (result != TURBO_OK) {
        service->last_pump_error = result;
    }
    tr_raft_coronet_peer_service_run_reader(slot);
}

int tr_raft_coronet_peer_service_add_outbound(
    tr_raft_coronet_peer_service_t *service,
    const tr_raft_coronet_dial_scheduler_config_t *config)
{
    tr_raft_coronet_dial_scheduler_config_t resolved;
    tr_raft_coronet_connection_direction_t expected_direction;
    tr_raft_node_id_t peer_node_id;
    tr_raft_coronet_dial_scheduler_t *scheduler = NULL;
    int result;

    if (service == NULL || config == NULL || service->stopping ||
        config->manager != NULL ||
        config->connect_outbound != NULL ||
        config->outbound.admission.handshake.resolve_peer_identity != NULL ||
        config->outbound.admission.handshake.identity_context != NULL ||
        config->outbound.admission.on_snapshot != NULL ||
        config->outbound.admission.snapshot_context != NULL) {
        return TURBO_EINVAL;
    }
    if (service->scheduler_count >= TR_RAFT_MAX_VOTERS - 1U) {
        return TURBO_ENOSPC;
    }
    result = tr_raft_coronet_peer_service_is_quiescent(service);
    if (result != TURBO_OK) {
        return result;
    }
    peer_node_id = config->outbound.admission.expected_peer_node_id;
    if (tr_raft_coronet_peer_service_find_peer(service, peer_node_id, NULL) !=
            TURBO_OK ||
        tr_raft_coronet_peer_service_find_scheduler(service, peer_node_id) !=
            SIZE_MAX) {
        return TURBO_EINVAL;
    }
    result = tr_raft_coronet_expected_direction(
        service->local_node_id, peer_node_id, &expected_direction);
    if (result != TURBO_OK ||
        expected_direction != TR_RAFT_CORONET_CONNECTION_OUTBOUND) {
        return TURBO_EPROTO;
    }

    resolved = *config;
    resolved.manager = service->manager;
    resolved.connect_outbound = service->connect_outbound;
    resolved.outbound.admission.handshake.resolve_peer_identity =
        tr_raft_coronet_peer_service_resolve_identity;
    resolved.outbound.admission.handshake.identity_context = service;
    if (service->snapshot_receiver != NULL ||
        service->on_snapshot_ack != NULL) {
        resolved.outbound.admission.on_snapshot =
            tr_raft_coronet_peer_service_receive_snapshot;
        resolved.outbound.admission.snapshot_context = service;
    }
    result = tr_raft_coronet_dial_scheduler_create(&resolved, &scheduler);
    if (result != TURBO_OK) {
        return result;
    }
    service->scheduler_peer_ids[service->scheduler_count] = peer_node_id;
    service->schedulers[service->scheduler_count] = scheduler;
    ++service->scheduler_count;
    return TURBO_OK;
}

int tr_raft_coronet_peer_service_step(
    tr_raft_coronet_peer_service_t *service,
    uint64_t now_ms,
    tr_raft_coronet_peer_service_step_result_t *out_result)
{
    size_t index;
    int result = TURBO_OK;

    if (service == NULL || out_result == NULL) {
        return TURBO_EINVAL;
    }
    if (service->stopping) {
        return TURBO_EPIPE;
    }
    memset(out_result, 0, sizeof(*out_result));
    if (service->step_active) {
        return TURBO_EBUSY;
    }
    service->step_active = 1;
    ++service->active_operation_count;
    out_result->scheduler_count = service->scheduler_count;

    for (index = 0U; index < service->scheduler_count; ++index) {
        tr_raft_coronet_dial_status_t before;
        tr_raft_coronet_dial_status_t after;
        int step_result;

        result = tr_raft_coronet_dial_scheduler_get_status(
            service->schedulers[index], &before);
        if (result != TURBO_OK) {
            break;
        }
        step_result = tr_raft_coronet_dial_scheduler_step(
            service->schedulers[index], now_ms);
        result = tr_raft_coronet_dial_scheduler_get_status(
            service->schedulers[index], &after);
        if (result != TURBO_OK) {
            break;
        }
        if (after.attempt_count != before.attempt_count) {
            ++out_result->attempted_count;
            if (step_result == TURBO_OK) {
                step_result = tr_raft_coronet_peer_service_start_reader(
                    service, service->scheduler_peer_ids[index]);
                if (step_result == TURBO_OK) {
                    ++out_result->newly_connected_count;
                } else {
                    tr_raft_coronet_peer_manager_release_peer(
                        service->manager,
                        service->scheduler_peer_ids[index]);
                    tr_raft_coronet_dial_scheduler_reset(
                        service->schedulers[index], now_ms);
                    ++out_result->failed_count;
                    if (out_result->first_error == TURBO_OK) {
                        out_result->first_error = step_result;
                    }
                }
            } else {
                ++out_result->failed_count;
                if (out_result->first_error == TURBO_OK) {
                    out_result->first_error = step_result;
                }
            }
        }
    }
    --service->active_operation_count;
    service->step_active = 0;
    return result;
}

int tr_raft_coronet_peer_service_reset_peer(
    tr_raft_coronet_peer_service_t *service,
    tr_raft_node_id_t peer_node_id,
    uint64_t now_ms)
{
    size_t index;

    if (service == NULL || service->step_active || service->stopping) {
        if (service == NULL) {
            return TURBO_EINVAL;
        }
        return service->stopping ? TURBO_EPIPE : TURBO_EBUSY;
    }
    index = tr_raft_coronet_peer_service_find_scheduler(service, peer_node_id);
    if (index == SIZE_MAX) {
        return TURBO_EPROTO;
    }
    return tr_raft_coronet_dial_scheduler_reset(service->schedulers[index],
                                                 now_ms);
}

int tr_raft_coronet_peer_service_disconnect_peer(
    tr_raft_coronet_peer_service_t *service,
    tr_raft_node_id_t peer_node_id,
    uint64_t now_ms)
{
    tr_raft_coronet_reader_slot_t *slot;
    size_t peer_index;
    size_t scheduler_index;
    int result;

    if (service == NULL || peer_node_id == 0U) {
        return TURBO_EINVAL;
    }
    if (service->stopping) {
        return TURBO_EPIPE;
    }
    if (service->step_active) {
        return TURBO_EBUSY;
    }
    scheduler_index = tr_raft_coronet_peer_service_find_scheduler(
        service, peer_node_id);
    if (scheduler_index == SIZE_MAX ||
        tr_raft_coronet_peer_service_find_peer(service, peer_node_id,
                                                &peer_index) != TURBO_OK) {
        return TURBO_EPROTO;
    }
    slot = &service->readers[peer_index];
    if (!slot->active) {
        return tr_raft_coronet_dial_scheduler_reset(
            service->schedulers[scheduler_index], now_ms);
    }
    if (slot->reconnect_requested) {
        return TURBO_EBUSY;
    }
    slot->reconnect_requested = 1;
    slot->reconnect_at_ms = now_ms;
    result = tr_raft_coronet_peer_manager_close_peer(service->manager,
                                                      peer_node_id);
    if (result != TURBO_OK) {
        slot->reconnect_requested = 0;
        slot->reconnect_at_ms = 0U;
    }
    return result;
}

int tr_raft_coronet_peer_service_get_peer_dial_status(
    const tr_raft_coronet_peer_service_t *service,
    tr_raft_node_id_t peer_node_id,
    tr_raft_coronet_dial_status_t *out_status)
{
    size_t index;

    if (service == NULL || out_status == NULL) {
        return TURBO_EINVAL;
    }
    if (service->step_active) {
        return TURBO_EBUSY;
    }
    index = tr_raft_coronet_peer_service_find_scheduler(service, peer_node_id);
    if (index == SIZE_MAX) {
        return TURBO_EPROTO;
    }
    return tr_raft_coronet_dial_scheduler_get_status(
        service->schedulers[index], out_status);
}

int tr_raft_coronet_peer_service_enqueue(void *context,
                                         const tr_raft_message_t *message)
{
    tr_raft_coronet_payload_t payload;

    if (message == NULL) {
        return TURBO_EINVAL;
    }
    memset(&payload, 0, sizeof(payload));
    payload.kind = TR_RAFT_WIRE_PAYLOAD_RAFT;
    payload.data.raft = *message;
    return tr_raft_coronet_peer_service_enqueue_payload(
        (tr_raft_coronet_peer_service_t *) context, &payload);
}

int tr_raft_coronet_peer_service_enqueue_payload(
    tr_raft_coronet_peer_service_t *service,
    const tr_raft_coronet_payload_t *payload)
{
    tr_raft_coronet_payload_t discarded;
    tr_raft_node_id_t from;
    tr_raft_node_id_t to;
    size_t index;
    int result;

    if (service == NULL || payload == NULL) {
        return TURBO_EINVAL;
    }
    if (service->stopping) {
        return TURBO_EPIPE;
    }
    switch (payload->kind) {
    case TR_RAFT_WIRE_PAYLOAD_RAFT:
        from = payload->data.raft.from;
        to = payload->data.raft.to;
        break;
    case TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK:
        from = payload->data.snapshot_chunk.from;
        to = payload->data.snapshot_chunk.to;
        break;
    case TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_ACK:
        from = payload->data.snapshot_ack.from;
        to = payload->data.snapshot_ack.to;
        break;
    default:
        return TURBO_EPROTO;
    }
    if (from != service->local_node_id ||
        tr_raft_coronet_peer_service_find_peer(service, to,
                                                &index) != TURBO_OK) {
        return TURBO_EPROTO;
    }
    if (tr_raft_coronet_payload_queue_t_size(
            &service->outbound_queues[index]) >=
        service->outbound_queue_capacity) {
        return TURBO_ENOSPC;
    }
    result = tr_raft_coronet_payload_queue_t_push_back(
        &service->outbound_queues[index], *payload);
    if (result != TURBO_OK) {
        return result;
    }
    result = tr_raft_coronet_peer_service_start_writer(service);
    if (result != TURBO_OK) {
        tr_raft_coronet_payload_queue_t_pop_back(
            &service->outbound_queues[index], &discarded);
    }
    return result;
}

int tr_raft_coronet_peer_service_update_identities(
    tr_raft_coronet_peer_service_t *service,
    const tr_raft_coronet_identity_entry_t *entries,
    size_t entry_count)
{
    tr_raft_coronet_identity_registry_t *replacement = NULL;
    tr_raft_coronet_identity_registry_t *previous;
    int result;

    if (service == NULL || service->identity_generation == UINT64_MAX) {
        return TURBO_EINVAL;
    }
    result = tr_raft_coronet_peer_service_is_quiescent(service);
    if (result != TURBO_OK) {
        return result;
    }
    result = tr_raft_coronet_peer_service_identities_validate(
        service, entries, entry_count);
    if (result == TURBO_OK) {
        result = tr_raft_coronet_identity_registry_create(
            entries, entry_count, &replacement);
    }
    if (result != TURBO_OK) {
        return result;
    }
    previous = service->identity_registry;
    service->identity_registry = replacement;
    ++service->identity_generation;
    tr_raft_coronet_identity_registry_destroy(previous);
    return TURBO_OK;
}

int tr_raft_coronet_peer_service_get_status(
    const tr_raft_coronet_peer_service_t *service,
    tr_raft_coronet_peer_service_status_t *out_status)
{
    if (service == NULL || out_status == NULL) {
        return TURBO_EINVAL;
    }
    out_status->peer_count = service->peer_count;
    out_status->scheduler_count = service->scheduler_count;
    out_status->identity_generation = service->identity_generation;
    out_status->active_operation_count = service->active_operation_count;
    out_status->outbound_queue_capacity = service->outbound_queue_capacity;
    out_status->queued_message_count =
        tr_raft_coronet_peer_service_queued_count(service);
    out_status->queued_payload_count = out_status->queued_message_count;
    out_status->active_reader_count = service->active_reader_count;
    out_status->writer_active = service->writer_active;
    out_status->stopping = service->stopping;
    out_status->last_pump_error = service->last_pump_error;
    out_status->inbound_configured = service->inbound_service != NULL;
    out_status->step_active = service->step_active;
    out_status->snapshot_install_count = service->snapshot_install_count;
    out_status->snapshot_reject_count = service->snapshot_reject_count;
    out_status->snapshot_ack_count = service->snapshot_ack_count;
    return TURBO_OK;
}
