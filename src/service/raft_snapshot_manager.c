#include <turboraft/raft_snapshot_manager.h>

#include "raft_snapshot_peer.h"

#include <salts_error.h>

#include <stdlib.h>
#include <string.h>

struct tr_raft_snapshot_manager {
    tr_raft_node_id_t self_id;
    tr_raft_group_id_t group_id;
    tr_raft_node_id_t peer_node_ids[TR_RAFT_MAX_VOTERS - 1U];
    tr_raft_snapshot_peer_t *peers[TR_RAFT_MAX_VOTERS - 1U];
    size_t peer_count;
    tr_raft_snapshot_manager_payload_enqueue_fn enqueue;
    void *enqueue_context;
    size_t max_snapshot_bytes;
    uint8_t *snapshot_buffer;
    tr_raft_snapshot_provider_fn provider;
    void *provider_context;
    tr_raft_snapshot_complete_fn complete;
    void *complete_context;
    bool completion_notified[TR_RAFT_MAX_VOTERS - 1U];
};

static size_t tr_snapshot_manager_find_peer(
    const tr_raft_snapshot_manager_t *manager,
    tr_raft_node_id_t peer_id)
{
    size_t index;

    for (index = 0U; index < manager->peer_count; ++index) {
        if (manager->peer_node_ids[index] == peer_id) {
            return index;
        }
    }
    return SIZE_MAX;
}

static int tr_snapshot_manager_enqueue(
    void *context,
    const tr_raft_transport_payload_t *payload)
{
    tr_raft_snapshot_manager_t *manager =
        (tr_raft_snapshot_manager_t *) context;

    if (manager == NULL || payload == NULL) {
        return SALTS_EINVAL;
    }
    if (manager->enqueue == NULL) {
        return SALTS_ENOTCONN;
    }
    return manager->enqueue(manager->enqueue_context, payload);
}

int tr_raft_snapshot_manager_create(
    const tr_raft_snapshot_manager_config_t *config,
    tr_raft_snapshot_manager_t **out_manager)
{
    tr_raft_snapshot_manager_t *manager;
    size_t index;
    int result;

    if (config == NULL || out_manager == NULL || config->self_id == 0U ||
        config->group_id == 0U ||
        config->peer_node_ids == NULL || config->peer_count == 0U ||
        config->peer_count > TR_RAFT_MAX_VOTERS - 1U ||
        config->max_snapshot_bytes == 0U) {
        return SALTS_EINVAL;
    }
    for (index = 0U; index < config->peer_count; ++index) {
        if (config->peer_node_ids[index] == 0U ||
            config->peer_node_ids[index] == config->self_id ||
            (index > 0U && config->peer_node_ids[index - 1U] >=
                               config->peer_node_ids[index])) {
            return SALTS_EINVAL;
        }
    }

    *out_manager = NULL;
    manager = (tr_raft_snapshot_manager_t *) calloc(1U, sizeof(*manager));
    if (manager == NULL) {
        return SALTS_ENOMEM;
    }
    manager->self_id = config->self_id;
    manager->group_id = config->group_id;
    manager->peer_count = config->peer_count;
    manager->enqueue = config->enqueue;
    manager->enqueue_context = config->enqueue_context;
    manager->max_snapshot_bytes = config->max_snapshot_bytes;
    manager->provider = config->provider;
    manager->provider_context = config->provider_context;
    manager->complete = config->complete;
    manager->complete_context = config->complete_context;
    if (manager->provider != NULL) {
        manager->snapshot_buffer = (uint8_t *) malloc(
            manager->max_snapshot_bytes);
        if (manager->snapshot_buffer == NULL) {
            free(manager);
            return SALTS_ENOMEM;
        }
    }
    memcpy(manager->peer_node_ids, config->peer_node_ids,
           config->peer_count * sizeof(manager->peer_node_ids[0]));

    for (index = 0U; index < manager->peer_count; ++index) {
        tr_raft_snapshot_peer_config_t peer_config;

        memset(&peer_config, 0, sizeof(peer_config));
        peer_config.self_id = manager->self_id;
        peer_config.peer_id = manager->peer_node_ids[index];
        peer_config.group_id = manager->group_id;
        peer_config.max_snapshot_bytes = config->max_snapshot_bytes;
        peer_config.chunk_size = config->snapshot_chunk_size;
        peer_config.max_inflight_chunks =
            config->snapshot_max_inflight_chunks;
        peer_config.enqueue = tr_snapshot_manager_enqueue;
        peer_config.enqueue_context = manager;
        result = tr_raft_snapshot_peer_create(&peer_config,
                                              &manager->peers[index]);
        if (result != SALTS_OK) {
            while (index > 0U) {
                --index;
                tr_raft_snapshot_peer_destroy(manager->peers[index]);
            }
            free(manager->snapshot_buffer);
            free(manager);
            return result;
        }
    }
    *out_manager = manager;
    return SALTS_OK;
}

void tr_raft_snapshot_manager_destroy(tr_raft_snapshot_manager_t *manager)
{
    size_t index;

    if (manager == NULL) {
        return;
    }
    for (index = 0U; index < manager->peer_count; ++index) {
        tr_raft_snapshot_peer_destroy(manager->peers[index]);
    }
    free(manager->snapshot_buffer);
    free(manager);
}

int tr_raft_snapshot_manager_begin(
    tr_raft_snapshot_manager_t *manager,
    tr_raft_node_id_t peer_id,
    tr_raft_term_t leader_term,
    tr_raft_index_t snapshot_index,
    tr_raft_term_t snapshot_term,
    const tr_raft_conf_t *configuration,
    const uint8_t *data,
    size_t size)
{
    size_t index;

    if (manager == NULL) {
        return SALTS_EINVAL;
    }
    if (manager->enqueue == NULL) {
        return SALTS_ENOTCONN;
    }
    index = tr_snapshot_manager_find_peer(manager, peer_id);
    if (index == SIZE_MAX) {
        return SALTS_EPROTO;
    }
    {
        int result = tr_raft_snapshot_peer_begin(
            manager->peers[index], leader_term, snapshot_index, snapshot_term,
            configuration, data, size);

        if (result == SALTS_OK) {
            manager->completion_notified[index] = false;
        }
        return result;
    }
}

int tr_raft_snapshot_manager_enqueue_request(
    void *context,
    const tr_raft_snapshot_request_t *request)
{
    tr_raft_snapshot_manager_t *manager =
        (tr_raft_snapshot_manager_t *) context;
    tr_raft_snapshot_sender_status_t status;
    tr_raft_snapshot_point_t point;
    size_t snapshot_size = 0U;
    size_t index;
    int result;

    if (manager == NULL || request == NULL || request->peer_id == 0U ||
        request->leader_term == 0U || request->snapshot_index == 0U ||
        request->snapshot_term == 0U) {
        return SALTS_EINVAL;
    }
    if (manager->provider == NULL || manager->snapshot_buffer == NULL) {
        return SALTS_EINVAL;
    }
    index = tr_snapshot_manager_find_peer(manager, request->peer_id);
    if (index == SIZE_MAX) {
        return SALTS_EPROTO;
    }
    result = tr_raft_snapshot_peer_get_status(manager->peers[index], &status);
    if (result != SALTS_OK) {
        return result;
    }
    if (status.active && !status.complete) {
        return status.snapshot_index == request->snapshot_index
                   ? SALTS_OK
                   : SALTS_EBUSY;
    }

    memset(&point, 0, sizeof(point));
    result = manager->provider(
        manager->provider_context, request->snapshot_index,
        request->snapshot_term, &point, manager->snapshot_buffer,
        manager->max_snapshot_bytes, &snapshot_size);
    if (result != SALTS_OK) {
        return result;
    }
    if (point.index != request->snapshot_index ||
        point.term != request->snapshot_term ||
        tr_raft_conf_validate(&point.configuration) != SALTS_OK ||
        snapshot_size > manager->max_snapshot_bytes) {
        return SALTS_EPROTO;
    }
    return tr_raft_snapshot_manager_begin(
        manager, request->peer_id, request->leader_term, point.index,
        point.term, &point.configuration, manager->snapshot_buffer,
        snapshot_size);
}

int tr_raft_snapshot_manager_handle_payload(
    void *context,
    const tr_raft_transport_payload_t *payload)
{
    tr_raft_snapshot_manager_t *manager =
        (tr_raft_snapshot_manager_t *) context;
    size_t index;

    if (manager == NULL || payload == NULL ||
        payload->group_id != manager->group_id ||
        payload->kind != TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_ACK ||
        payload->data.snapshot_ack.to != manager->self_id) {
        return SALTS_EPROTO;
    }
    index = tr_snapshot_manager_find_peer(
        manager, payload->data.snapshot_ack.from);
    if (index == SIZE_MAX) {
        return SALTS_EPROTO;
    }
    {
        tr_raft_snapshot_sender_status_t status;
        int result = tr_raft_snapshot_peer_handle_payload(
            manager->peers[index], payload);

        if (result != SALTS_OK) {
            return result;
        }
        result = tr_raft_snapshot_peer_get_status(manager->peers[index],
                                                  &status);
        if (result != SALTS_OK || !status.complete ||
            manager->completion_notified[index]) {
            return result;
        }
        if (manager->complete != NULL) {
            result = manager->complete(manager->complete_context,
                                       payload->data.snapshot_ack.from,
                                       status.snapshot_index);
            if (result != SALTS_OK) {
                return result;
            }
        }
        manager->completion_notified[index] = true;
        return SALTS_OK;
    }
}

int tr_raft_snapshot_manager_resume(
    tr_raft_snapshot_manager_t *manager,
    tr_raft_node_id_t peer_id)
{
    size_t index;

    if (manager == NULL) {
        return SALTS_EINVAL;
    }
    if (manager->enqueue == NULL) {
        return SALTS_ENOTCONN;
    }
    index = tr_snapshot_manager_find_peer(manager, peer_id);
    if (index == SIZE_MAX) {
        return SALTS_EPROTO;
    }
    return tr_raft_snapshot_peer_resume(manager->peers[index]);
}

int tr_raft_snapshot_manager_get_status(
    const tr_raft_snapshot_manager_t *manager,
    tr_raft_node_id_t peer_id,
    tr_raft_snapshot_sender_status_t *out_status)
{
    size_t index;

    if (manager == NULL || out_status == NULL) {
        return SALTS_EINVAL;
    }
    index = tr_snapshot_manager_find_peer(manager, peer_id);
    if (index == SIZE_MAX) {
        return SALTS_EPROTO;
    }
    return tr_raft_snapshot_peer_get_status(manager->peers[index], out_status);
}
