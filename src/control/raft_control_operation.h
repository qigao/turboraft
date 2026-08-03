#ifndef TURBORAFT_RAFT_CONTROL_OPERATION_H
#define TURBORAFT_RAFT_CONTROL_OPERATION_H

#include <turboraft/raft_core.h>

#include <stddef.h>

const char *tr_control_operation_state_name(
    tr_raft_operation_state_t state);

int tr_control_operation_status_json(
    const tr_raft_operation_status_t *status,
    char *buffer,
    size_t capacity,
    size_t *out_size);

#endif
