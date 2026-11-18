#ifndef TURBORAFT_RAFT_TRANSPORT_INTERNAL_H
#define TURBORAFT_RAFT_TRANSPORT_INTERNAL_H

#include <turboraft/raft_transport.h>

#include <stddef.h>
#include <stdint.h>

/*
 * Encodes the length prefix plus the fixed DATA_CHUNK wire prefix while
 * preserving the payload as a separate borrowed segment.
 *
 * Success consumes exactly one outbound message id, matching
 * tr_raft_transport_encode_payload().
 */
int tr_raft_transport_encode_data_chunk_prefix(
    tr_raft_transport_session_t *session,
    const tr_raft_transport_payload_t *payload,
    uint8_t *output,
    size_t output_capacity,
    size_t *out_prefix_size,
    size_t *out_packet_size);

/* Snapshot counterpart preserving the queue-owned chunk as a second span. */
int tr_raft_transport_encode_snapshot_chunk_prefix(
    tr_raft_transport_session_t *session,
    const tr_raft_transport_payload_t *payload,
    uint8_t *output,
    size_t output_capacity,
    size_t *out_prefix_size,
    size_t *out_packet_size);

#endif
