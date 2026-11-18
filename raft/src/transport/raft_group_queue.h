#ifndef TURBORAFT_RAFT_GROUP_QUEUE_H
#define TURBORAFT_RAFT_GROUP_QUEUE_H

#include "raft_transport_payload_storage.h"

#include <stddef.h>
#include <stdint.h>

typedef size_t tr_raft_group_queue_token_t;

typedef void (*tr_raft_group_queue_release_fn)(
    tr_raft_owned_transport_payload_t *owned);

typedef struct tr_raft_group_queue_config {
    size_t max_groups;
    size_t total_item_capacity;
    size_t total_data_bytes;
    size_t per_group_item_capacity;
    size_t per_group_data_bytes;
    tr_raft_group_queue_release_fn release;
} tr_raft_group_queue_config_t;

typedef struct tr_raft_group_queue_group_status {
    tr_raft_group_id_t group_id;
    size_t queued_item_count;
    size_t queued_data_bytes;
} tr_raft_group_queue_group_status_t;

typedef struct tr_raft_group_queue_slot tr_raft_group_queue_slot_t;

typedef struct tr_raft_group_queue {
    tr_raft_group_queue_config_t config;
    tr_raft_group_queue_slot_t *slots;
    size_t active_group_count;
    size_t queued_item_count;
    size_t queued_data_bytes;
    size_t cursor;
    int initialized;
} tr_raft_group_queue_t;

/*
 * Ownership:
 * - enqueue transfers ownership only on SALTS_OK.
 * - pop transfers ownership back when out_owned is non-NULL.
 * - pop with out_owned == NULL and clear/destroy use config.release.
 */
int tr_raft_group_queue_init(
    tr_raft_group_queue_t *queue,
    const tr_raft_group_queue_config_t *config);

void tr_raft_group_queue_destroy(tr_raft_group_queue_t *queue);

int tr_raft_group_queue_clear(tr_raft_group_queue_t *queue);

int tr_raft_group_queue_enqueue(
    tr_raft_group_queue_t *queue,
    const tr_raft_owned_transport_payload_t *owned,
    size_t data_bytes);

int tr_raft_group_queue_peek_next(
    const tr_raft_group_queue_t *queue,
    tr_raft_group_queue_token_t *out_token,
    const tr_raft_owned_transport_payload_t **out_owned);

int tr_raft_group_queue_pop(
    tr_raft_group_queue_t *queue,
    tr_raft_group_queue_token_t token,
    tr_raft_owned_transport_payload_t *out_owned,
    size_t *out_data_bytes);

size_t tr_raft_group_queue_size(const tr_raft_group_queue_t *queue);
size_t tr_raft_group_queue_data_bytes(const tr_raft_group_queue_t *queue);
size_t tr_raft_group_queue_active_groups(const tr_raft_group_queue_t *queue);

int tr_raft_group_queue_get_group_status(
    const tr_raft_group_queue_t *queue,
    tr_raft_group_id_t group_id,
    tr_raft_group_queue_group_status_t *out_status);

#endif
