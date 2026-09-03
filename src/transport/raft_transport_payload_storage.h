#ifndef TURBORAFT_RAFT_TRANSPORT_PAYLOAD_STORAGE_H
#define TURBORAFT_RAFT_TRANSPORT_PAYLOAD_STORAGE_H

#include <turboraft/raft_transport.h>

#include <salts_buffer.h>
#include <salts_error.h>

#include <string.h>

typedef struct tr_raft_owned_transport_payload {
    tr_raft_transport_payload_t payload;
    mem_buffer_t *payload_data;
} tr_raft_owned_transport_payload_t;

static inline void tr_raft_owned_transport_payload_release(
    tr_raft_owned_transport_payload_t *owned)
{
    if (owned == NULL) {
        return;
    }
    mem_buffer_release(owned->payload_data);
    memset(owned, 0, sizeof(*owned));
}

static inline int tr_raft_owned_transport_payload_copy(
    tr_raft_owned_transport_payload_t *owned,
    const tr_raft_transport_payload_t *payload)
{
    const uint8_t *data = NULL;
    size_t data_length = 0U;

    if (owned == NULL || payload == NULL) {
        return SALTS_EINVAL;
    }
    memset(owned, 0, sizeof(*owned));
    owned->payload = *payload;
    if (payload->kind == TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK) {
        data = payload->data.snapshot_chunk.data;
        data_length = payload->data.snapshot_chunk.data_length;
    } else if (payload->kind == TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK) {
        data = payload->data.data_chunk.data;
        data_length = payload->data.data_chunk.data_length;
    } else {
        return SALTS_OK;
    }
    if (data_length == 0U) {
        if (payload->kind == TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK) {
            owned->payload.data.snapshot_chunk.data = NULL;
        } else {
            owned->payload.data.data_chunk.data = NULL;
        }
        return SALTS_OK;
    }
    if (data == NULL ||
        (payload->kind == TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK &&
         data_length > TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES) ||
        (payload->kind == TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK &&
         data_length > TR_RAFT_WIRE_MAX_DATA_CHUNK_BYTES)) {
        return SALTS_EINVAL;
    }
    owned->payload_data = mem_get_buffer(mem_global(), data_length);
    if (owned->payload_data == NULL) {
        return SALTS_ENOMEM;
    }
    memcpy(mem_buffer_data(owned->payload_data), data, data_length);
    mem_set_used(owned->payload_data, data_length);
    data = (const uint8_t *)mem_buffer_const_data(owned->payload_data);
    if (payload->kind == TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK) {
        owned->payload.data.snapshot_chunk.data = data;
    } else {
        owned->payload.data.data_chunk.data = data;
    }
    return SALTS_OK;
}

#endif
