#ifndef TURBORAFT_RAFT_CORONET_PAYLOAD_STORAGE_H
#define TURBORAFT_RAFT_CORONET_PAYLOAD_STORAGE_H

#include <turboraft/raft_coronet_transport.h>

#include <turbo_buffer.h>
#include <turbo_error.h>

#include <string.h>

typedef struct tr_raft_owned_coronet_payload {
    tr_raft_coronet_payload_t payload;
    mem_buffer_t *snapshot_data;
} tr_raft_owned_coronet_payload_t;

static inline void tr_raft_owned_coronet_payload_release(
    tr_raft_owned_coronet_payload_t *owned)
{
    if (owned == NULL) {
        return;
    }
    mem_buffer_release(owned->snapshot_data);
    memset(owned, 0, sizeof(*owned));
}

static inline int tr_raft_owned_coronet_payload_copy(
    tr_raft_owned_coronet_payload_t *owned,
    const tr_raft_coronet_payload_t *payload)
{
    const tr_raft_snapshot_chunk_t *chunk;

    if (owned == NULL || payload == NULL) {
        return TURBO_EINVAL;
    }
    memset(owned, 0, sizeof(*owned));
    owned->payload = *payload;
    if (payload->kind != TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK) {
        return TURBO_OK;
    }
    chunk = &payload->data.snapshot_chunk;
    if (chunk->data_length == 0U) {
        owned->payload.data.snapshot_chunk.data = NULL;
        return TURBO_OK;
    }
    if (chunk->data == NULL ||
        chunk->data_length > TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES) {
        return TURBO_EINVAL;
    }
    owned->snapshot_data = mem_get_buffer(mem_global(), chunk->data_length);
    if (owned->snapshot_data == NULL) {
        return TURBO_ENOMEM;
    }
    memcpy(mem_buffer_data(owned->snapshot_data), chunk->data,
           chunk->data_length);
    mem_set_used(owned->snapshot_data, chunk->data_length);
    owned->payload.data.snapshot_chunk.data =
        (const uint8_t *)mem_buffer_const_data(owned->snapshot_data);
    return TURBO_OK;
}

#endif
