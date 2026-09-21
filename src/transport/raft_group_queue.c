#include "raft_group_queue.h"

#include <salts_error.h>

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

typedef struct tr_raft_group_queue_entry {
    tr_raft_owned_transport_payload_t owned;
    size_t data_bytes;
} tr_raft_group_queue_entry_t;

struct tr_raft_group_queue_slot {
    tr_raft_group_id_t group_id;
    tr_raft_group_queue_entry_t *entries;
    size_t head;
    size_t count;
    size_t queued_data_bytes;
};

static int tr_raft_group_queue_config_valid(
    const tr_raft_group_queue_config_t *config)
{
    return config != NULL &&
           config->max_groups != 0U &&
           config->total_item_capacity != 0U &&
           config->total_data_bytes != 0U &&
           config->per_group_item_capacity != 0U &&
           config->per_group_data_bytes != 0U &&
           config->release != NULL &&
           config->max_groups <= config->total_item_capacity &&
           config->per_group_item_capacity <= config->total_item_capacity &&
           config->per_group_data_bytes <= config->total_data_bytes &&
           config->per_group_item_capacity <=
               SIZE_MAX / sizeof(tr_raft_group_queue_entry_t) &&
           config->max_groups <=
               SIZE_MAX / sizeof(tr_raft_group_queue_slot_t);
}

static tr_raft_group_queue_slot_t *tr_raft_group_queue_find_group(
    const tr_raft_group_queue_t *queue,
    tr_raft_group_id_t group_id,
    size_t *out_index)
{
    size_t index;

    for (index = 0U; index < queue->config.max_groups; ++index) {
        if (queue->slots[index].group_id == group_id) {
            if (out_index != NULL) {
                *out_index = index;
            }
            return &queue->slots[index];
        }
    }
    return NULL;
}

static tr_raft_group_queue_slot_t *tr_raft_group_queue_find_free(
    tr_raft_group_queue_t *queue,
    size_t *out_index)
{
    size_t index;

    for (index = 0U; index < queue->config.max_groups; ++index) {
        if (queue->slots[index].group_id == 0U) {
            if (out_index != NULL) {
                *out_index = index;
            }
            return &queue->slots[index];
        }
    }
    return NULL;
}

static int tr_raft_group_queue_prepare_slot(
    tr_raft_group_queue_t *queue,
    tr_raft_group_queue_slot_t *slot,
    tr_raft_group_id_t group_id)
{
    if (slot->entries == NULL) {
        slot->entries = (tr_raft_group_queue_entry_t *)calloc(
            queue->config.per_group_item_capacity,
            sizeof(tr_raft_group_queue_entry_t));
        if (slot->entries == NULL) {
            return SALTS_ENOMEM;
        }
    }
    slot->group_id = group_id;
    slot->head = 0U;
    slot->count = 0U;
    slot->queued_data_bytes = 0U;
    ++queue->active_group_count;
    return SALTS_OK;
}

int tr_raft_group_queue_init(
    tr_raft_group_queue_t *queue,
    const tr_raft_group_queue_config_t *config)
{
    if (queue == NULL || !tr_raft_group_queue_config_valid(config)) {
        return SALTS_EINVAL;
    }

    memset(queue, 0, sizeof(*queue));
    queue->slots = (tr_raft_group_queue_slot_t *)calloc(
        config->max_groups, sizeof(tr_raft_group_queue_slot_t));
    if (queue->slots == NULL) {
        return SALTS_ENOMEM;
    }
    queue->config = *config;
    queue->initialized = 1;
    return SALTS_OK;
}

int tr_raft_group_queue_clear(tr_raft_group_queue_t *queue)
{
    size_t index;

    if (queue == NULL || !queue->initialized) {
        return SALTS_EINVAL;
    }

    for (index = 0U; index < queue->config.max_groups; ++index) {
        tr_raft_group_queue_slot_t *slot = &queue->slots[index];

        while (slot->count != 0U) {
            tr_raft_group_queue_entry_t *entry =
                &slot->entries[slot->head];

            queue->config.release(&entry->owned);
            memset(entry, 0, sizeof(*entry));
            slot->head =
                (slot->head + 1U) % queue->config.per_group_item_capacity;
            --slot->count;
        }
        slot->group_id = 0U;
        slot->head = 0U;
        slot->queued_data_bytes = 0U;
    }
    queue->active_group_count = 0U;
    queue->queued_item_count = 0U;
    queue->queued_data_bytes = 0U;
    queue->cursor = 0U;
    return SALTS_OK;
}

void tr_raft_group_queue_destroy(tr_raft_group_queue_t *queue)
{
    size_t index;

    if (queue == NULL || !queue->initialized) {
        return;
    }
    (void)tr_raft_group_queue_clear(queue);
    for (index = 0U; index < queue->config.max_groups; ++index) {
        free(queue->slots[index].entries);
    }
    free(queue->slots);
    memset(queue, 0, sizeof(*queue));
}

int tr_raft_group_queue_enqueue(
    tr_raft_group_queue_t *queue,
    const tr_raft_owned_transport_payload_t *owned,
    size_t data_bytes)
{
    tr_raft_group_queue_slot_t *slot;
    size_t slot_index = 0U;
    size_t tail;
    int result;

    if (queue == NULL || !queue->initialized || owned == NULL ||
        owned->payload.group_id == 0U) {
        return SALTS_EINVAL;
    }
    if (queue->queued_item_count >= queue->config.total_item_capacity ||
        data_bytes > queue->config.total_data_bytes -
                         queue->queued_data_bytes) {
        return SALTS_ENOSPC;
    }

    slot = tr_raft_group_queue_find_group(
        queue, owned->payload.group_id, &slot_index);
    if (slot == NULL) {
        if (queue->active_group_count >= queue->config.max_groups) {
            return SALTS_ENOSPC;
        }
        slot = tr_raft_group_queue_find_free(queue, &slot_index);
        if (slot == NULL) {
            return SALTS_EPROTO;
        }
        result = tr_raft_group_queue_prepare_slot(
            queue, slot, owned->payload.group_id);
        if (result != SALTS_OK) {
            return result;
        }
    }

    if (slot->count >= queue->config.per_group_item_capacity ||
        data_bytes > queue->config.per_group_data_bytes -
                         slot->queued_data_bytes) {
        if (slot->count == 0U && slot->queued_data_bytes == 0U) {
            slot->group_id = 0U;
            --queue->active_group_count;
        }
        return SALTS_ENOSPC;
    }

    tail = (slot->head + slot->count) %
           queue->config.per_group_item_capacity;
    slot->entries[tail].owned = *owned;
    slot->entries[tail].data_bytes = data_bytes;
    ++slot->count;
    slot->queued_data_bytes += data_bytes;
    ++queue->queued_item_count;
    queue->queued_data_bytes += data_bytes;
    return SALTS_OK;
}

int tr_raft_group_queue_peek_next(
    const tr_raft_group_queue_t *queue,
    tr_raft_group_queue_token_t *out_token,
    const tr_raft_owned_transport_payload_t **out_owned)
{
    size_t offset;

    if (queue == NULL || !queue->initialized || out_token == NULL ||
        out_owned == NULL) {
        return SALTS_EINVAL;
    }
    if (queue->queued_item_count == 0U) {
        return SALTS_EBUSY;
    }

    for (offset = 0U; offset < queue->config.max_groups; ++offset) {
        size_t index = (queue->cursor + offset) % queue->config.max_groups;
        const tr_raft_group_queue_slot_t *slot = &queue->slots[index];

        if (slot->group_id != 0U && slot->count != 0U) {
            *out_token = index;
            *out_owned = &slot->entries[slot->head].owned;
            return SALTS_OK;
        }
    }
    return SALTS_EPROTO;
}

int tr_raft_group_queue_pop(
    tr_raft_group_queue_t *queue,
    tr_raft_group_queue_token_t token,
    tr_raft_owned_transport_payload_t *out_owned,
    size_t *out_data_bytes)
{
    tr_raft_group_queue_slot_t *slot;
    tr_raft_group_queue_entry_t *entry;
    size_t data_bytes;

    if (queue == NULL || !queue->initialized ||
        token >= queue->config.max_groups) {
        return SALTS_EINVAL;
    }
    slot = &queue->slots[token];
    if (slot->group_id == 0U || slot->count == 0U) {
        return SALTS_EPROTO;
    }

    entry = &slot->entries[slot->head];
    data_bytes = entry->data_bytes;
    if (out_owned != NULL) {
        *out_owned = entry->owned;
    } else {
        queue->config.release(&entry->owned);
    }
    if (out_data_bytes != NULL) {
        *out_data_bytes = data_bytes;
    }
    memset(entry, 0, sizeof(*entry));

    slot->head =
        (slot->head + 1U) % queue->config.per_group_item_capacity;
    --slot->count;
    slot->queued_data_bytes -= data_bytes;
    --queue->queued_item_count;
    queue->queued_data_bytes -= data_bytes;
    queue->cursor = (token + 1U) % queue->config.max_groups;

    if (slot->count == 0U) {
        slot->group_id = 0U;
        slot->head = 0U;
        slot->queued_data_bytes = 0U;
        --queue->active_group_count;
    }
    return SALTS_OK;
}

size_t tr_raft_group_queue_size(const tr_raft_group_queue_t *queue)
{
    return queue != NULL && queue->initialized
               ? queue->queued_item_count
               : 0U;
}

size_t tr_raft_group_queue_data_bytes(const tr_raft_group_queue_t *queue)
{
    return queue != NULL && queue->initialized
               ? queue->queued_data_bytes
               : 0U;
}

size_t tr_raft_group_queue_active_groups(const tr_raft_group_queue_t *queue)
{
    return queue != NULL && queue->initialized
               ? queue->active_group_count
               : 0U;
}

int tr_raft_group_queue_get_group_status(
    const tr_raft_group_queue_t *queue,
    tr_raft_group_id_t group_id,
    tr_raft_group_queue_group_status_t *out_status)
{
    tr_raft_group_queue_slot_t *slot;

    if (queue == NULL || !queue->initialized || group_id == 0U ||
        out_status == NULL) {
        return SALTS_EINVAL;
    }
    slot = tr_raft_group_queue_find_group(queue, group_id, NULL);
    if (slot == NULL) {
        return SALTS_ENOENT;
    }
    memset(out_status, 0, sizeof(*out_status));
    out_status->group_id = group_id;
    out_status->queued_item_count = slot->count;
    out_status->queued_data_bytes = slot->queued_data_bytes;
    return SALTS_OK;
}
