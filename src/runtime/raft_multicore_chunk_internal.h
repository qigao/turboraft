#ifndef TURBORAFT_RAFT_MULTICORE_CHUNK_INTERNAL_H
#define TURBORAFT_RAFT_MULTICORE_CHUNK_INTERNAL_H

#include <turboraft/raft_multicore.h>

/* This is the only entry point that can publish runtime-owned pointer
 * metadata into an existing per-Group Multicore request ring. It enforces
 * both byte and completion credits before materializing the CNet-borrowed
 * bytes. It is not an exported SDK API. */
int tr_raft_multicore_submit_owned_chunk(
    tr_raft_multicore_t *runtime,
    const tr_raft_transport_payload_t *borrowed,
    uint64_t request_id,
    const tr_raft_transport_reply_origin_t *reply_origin);

#endif
