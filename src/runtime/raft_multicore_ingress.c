#include <turboraft/raft_multicore_ingress.h>

#include "raft_multicore_chunk_internal.h"

#include <cmeta_error.h>

#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>

/* The ONLY per-ingress runtime state is one atomic correlation-ID source.
 * All queue credits, owner placement and request/completion storage belong
 * to tr_raft_multicore, not to this transport-neutral boundary. */
struct tr_raft_multicore_ingress {
    tr_raft_multicore_t *runtime; /* borrowed, shared, never swapped */
    atomic_uint_fast64_t next_request_id;
};

static int tr_multicore_ingress_request_id(
    tr_raft_multicore_ingress_t *ingress, uint64_t *out_request_id)
{
    uint_fast64_t previous;

    previous = atomic_load_explicit(
        &ingress->next_request_id, memory_order_relaxed);
    for (;;) {
        if (previous == 0U || previous == UINT64_MAX)
            return SALTS_ERANGE;
        if (atomic_compare_exchange_weak_explicit(
                &ingress->next_request_id,
                &previous, previous + 1U,
                memory_order_relaxed, memory_order_relaxed)) {
            *out_request_id = (uint64_t)previous;
            return SALTS_OK;
        }
    }
}

int tr_raft_multicore_ingress_create(
    tr_raft_multicore_t *runtime, uint64_t first_request_id,
    tr_raft_multicore_ingress_t **out_ingress)
{
    tr_raft_multicore_ingress_t *ingress;

    if (out_ingress == NULL) return SALTS_EINVAL;
    *out_ingress = NULL;
    if (runtime == NULL || first_request_id == 0U ||
        first_request_id == UINT64_MAX)
        return SALTS_EINVAL;

    ingress = (tr_raft_multicore_ingress_t *)calloc(1U, sizeof(*ingress));
    if (ingress == NULL) return SALTS_ENOMEM;
    ingress->runtime = runtime;
    atomic_init(&ingress->next_request_id, first_request_id);
    *out_ingress = ingress;
    return SALTS_OK;
}

int tr_raft_multicore_ingress_submit(
    tr_raft_multicore_ingress_t *ingress,
    const tr_raft_transport_payload_t *payload,
    uint64_t *out_request_id)
{
    tr_raft_multicore_request_t request = {0};
    uint64_t request_id = 0U;
    int result;

    if (out_request_id == NULL) return SALTS_EINVAL;
    *out_request_id = 0U;
    if (ingress == NULL || ingress->runtime == NULL || payload == NULL)
        return SALTS_EINVAL;
    if (payload->kind != TR_RAFT_WIRE_PAYLOAD_RAFT &&
        payload->kind != TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK &&
        payload->kind != TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK)
        return SALTS_ENOTSUP;
    if (payload->kind == TR_RAFT_WIRE_PAYLOAD_RAFT &&
        (payload->group_id == 0U ||
         payload->data.raft.from == 0U ||
         payload->data.raft.to == 0U ||
         payload->data.raft.from == payload->data.raft.to ||
         payload->data.raft.entry_count > TR_RAFT_MAX_APPEND_ENTRIES))
        return SALTS_EPROTO;

    result = tr_multicore_ingress_request_id(ingress, &request_id);
    if (result != SALTS_OK) return result;

    if (payload->kind == TR_RAFT_WIRE_PAYLOAD_RAFT) {
        request.operation = TR_RAFT_MULTICORE_STEP;
        request.request_id = request_id;
        request.value.message = payload->data.raft; /* fully inline copy */
        result = tr_raft_multicore_submit(
            ingress->runtime, payload->group_id, &request);
    } else {
        /* Byte + completion credits checked before the borrowed chunk is
         * materialized. The Group Owner receives an owned, bounded view. */
        result = tr_raft_multicore_submit_owned_chunk(
            ingress->runtime, payload, request_id);
    }
    if (result != SALTS_OK) return result;
    *out_request_id = request_id;
    return SALTS_OK;
}

int tr_raft_multicore_ingress_receive(
    void *ingress_context, const tr_raft_transport_payload_t *payload)
{
    uint64_t request_id = 0U;

    return tr_raft_multicore_ingress_submit(
        (tr_raft_multicore_ingress_t *)ingress_context,
        payload, &request_id);
}

int tr_raft_multicore_ingress_destroy(
    tr_raft_multicore_ingress_t *ingress)
{
    if (ingress == NULL) return SALTS_OK;
    /* Exclusive host lifecycle boundary: no callbacks in flight. */
    free(ingress);
    return SALTS_OK;
}
