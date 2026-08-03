#ifndef TURBORAFT_CONTROL_OWNER_BRIDGE_H
#define TURBORAFT_CONTROL_OWNER_BRIDGE_H

#include <turboraft/raft_service_owner.h>

#include <stddef.h>
#include <stdint.h>

#if defined(TURBORAFT_HAS_SERVICE_OWNER)
int tr_control_owner_bridge_execute(
    tr_raft_service_owner_t *owner,
    tr_raft_service_owner_command_fn command,
    const void *payload,
    size_t payload_size,
    uint64_t queue_timeout_ms);
#endif

#endif
