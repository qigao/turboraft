#include "raft_control_operation.h"

#include <turbo_error.h>

#include <inttypes.h>
#include <stdio.h>

const char *tr_control_operation_state_name(
    tr_raft_operation_state_t state)
{
    switch (state) {
    case TR_RAFT_OPERATION_PENDING:
        return "PENDING";
    case TR_RAFT_OPERATION_COMMITTED:
        return "COMMITTED";
    case TR_RAFT_OPERATION_APPLIED:
        return "APPLIED";
    case TR_RAFT_OPERATION_LOST:
        return "LOST";
    case TR_RAFT_OPERATION_EXPIRED:
        return "EXPIRED";
    default:
        return NULL;
    }
}

int tr_control_operation_status_json(
    const tr_raft_operation_status_t *status,
    char *buffer,
    size_t capacity,
    size_t *out_size)
{
    const char *state;
    int written;

    if (status == NULL || buffer == NULL || capacity == 0U ||
        out_size == NULL || status->term == 0U || status->index == 0U) {
        return TURBO_EINVAL;
    }
    state = tr_control_operation_state_name(status->state);
    if (state == NULL) {
        return TURBO_EINVAL;
    }

    written = snprintf(
        buffer, capacity,
        "{\"state\":\"%s\",\"term\":%" PRIu64
        ",\"index\":%" PRIu64 ",\"commit_index\":%" PRIu64
        ",\"applied_index\":%" PRIu64 "}",
        state, (uint64_t)status->term, (uint64_t)status->index,
        (uint64_t)status->commit_index, (uint64_t)status->applied_index);
    if (written < 0 || (size_t)written >= capacity) {
        return TURBO_ENOSPC;
    }
    *out_size = (size_t)written;
    return TURBO_OK;
}
