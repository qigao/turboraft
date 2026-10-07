#include "raft_control_operation.h"

#include <cmeta_error.h>
#include <cmeta/enum.h>

#include <inttypes.h>
#include <stdio.h>

/* Project the existing public enum without changing its tag, values or ABI. */
static const cmeta_enum_item_desc tr_control_operation_states[] = {
    {TR_RAFT_OPERATION_PENDING, "TR_RAFT_OPERATION_PENDING", "PENDING"},
    {TR_RAFT_OPERATION_COMMITTED, "TR_RAFT_OPERATION_COMMITTED", "COMMITTED"},
    {TR_RAFT_OPERATION_APPLIED, "TR_RAFT_OPERATION_APPLIED", "APPLIED"},
    {TR_RAFT_OPERATION_LOST, "TR_RAFT_OPERATION_LOST", "LOST"},
    {TR_RAFT_OPERATION_EXPIRED, "TR_RAFT_OPERATION_EXPIRED", "EXPIRED"}};
static const cmeta_enum_desc tr_control_operation_state_desc = {
    "tr_raft_operation_state", tr_control_operation_states,
    sizeof(tr_control_operation_states) / sizeof(tr_control_operation_states[0])};

const char *tr_control_operation_state_name(
    tr_raft_operation_state_t state)
{
    return cmeta_enum_to_string(&tr_control_operation_state_desc, (int64_t)state);
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
        return SALTS_EINVAL;
    }
    state = tr_control_operation_state_name(status->state);
    if (state == NULL) {
        return SALTS_EINVAL;
    }

    written = snprintf(
        buffer, capacity,
        "{\"state\":\"%s\",\"term\":%" PRIu64
        ",\"index\":%" PRIu64 ",\"commit_index\":%" PRIu64
        ",\"applied_index\":%" PRIu64 "}",
        state, (uint64_t)status->term, (uint64_t)status->index,
        (uint64_t)status->commit_index, (uint64_t)status->applied_index);
    if (written < 0 || (size_t)written >= capacity) {
        return SALTS_ENOSPC;
    }
    *out_size = (size_t)written;
    return SALTS_OK;
}
