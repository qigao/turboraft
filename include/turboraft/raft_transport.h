#ifndef TURBORAFT_RAFT_TRANSPORT_H
#define TURBORAFT_RAFT_TRANSPORT_H

#include <turboraft/raft_core.h>
#include <turboraft/raft_peer_handshake.h>
#include <turboraft/raft_wire_codec.h>

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TR_RAFT_TRANSPORT_LENGTH_PREFIX_SIZE 4U
#define TR_RAFT_TRANSPORT_MAX_PACKET_SIZE \
    (TR_RAFT_TRANSPORT_LENGTH_PREFIX_SIZE + TR_RAFT_WIRE_MAX_FRAME_SIZE)

typedef enum tr_raft_transport_state {
    TR_RAFT_TRANSPORT_STATE_READY = 0,
    TR_RAFT_TRANSPORT_STATE_FAULTED
} tr_raft_transport_state_t;

typedef struct tr_raft_transport_queue_limits {
    size_t total_item_capacity;
    size_t total_data_bytes;
    size_t max_active_groups;
    size_t per_group_item_capacity;
    size_t per_group_data_bytes;
} tr_raft_transport_queue_limits_t;

typedef struct tr_raft_transport_group_queue_status {
    tr_raft_group_id_t group_id;
    size_t queued_payload_count;
    size_t queued_data_bytes;
} tr_raft_transport_group_queue_status_t;


typedef struct tr_raft_transport_payload {
    tr_raft_group_id_t group_id;
    tr_raft_wire_payload_kind_t kind;
    union {
        tr_raft_message_t raft;
        tr_raft_snapshot_chunk_t snapshot_chunk;
        tr_raft_snapshot_ack_t snapshot_ack;
        tr_raft_data_chunk_t data_chunk;
        tr_raft_data_ack_t data_ack;
    } data;
} tr_raft_transport_payload_t;

typedef int (*tr_raft_transport_payload_handler_fn)(
    void *context, const tr_raft_transport_payload_t *payload);

typedef struct tr_raft_transport_session_config {
    tr_raft_cluster_id_t cluster_id;
    tr_raft_node_id_t local_node_id;
    tr_raft_node_id_t peer_node_id;
    uint64_t first_outbound_message_id;
    /** Required completed peer negotiation; there is no implicit contract. */
    const tr_raft_handshake_result_t *handshake;
    /** Required unified inbound callback for Raft/snapshot/data payloads. */
    tr_raft_transport_payload_handler_fn on_payload;
    void *payload_context;
} tr_raft_transport_session_config_t;

typedef struct tr_raft_transport_status {
    tr_raft_transport_state_t state;
    uint64_t last_outbound_message_id;
    uint64_t last_inbound_message_id;
    uint64_t frames_encoded;
    uint64_t frames_decoded;
    uint64_t bytes_encoded;
    uint64_t bytes_decoded;
    uint64_t group_routing_rejections;
    tr_raft_group_id_t last_rejected_group_id;
    int last_group_routing_error;
} tr_raft_transport_status_t;

typedef struct tr_raft_transport_session tr_raft_transport_session_t;

int tr_raft_transport_session_create(
    const tr_raft_transport_session_config_t *config,
    tr_raft_transport_session_t **out_session);
int tr_raft_transport_session_destroy(tr_raft_transport_session_t *session);

/** Convenience encoder for one Raft message in a non-zero group. */
int tr_raft_transport_encode(
    tr_raft_transport_session_t *session,
    tr_raft_group_id_t group_id,
    const tr_raft_message_t *message,
    uint8_t *output,
    size_t output_capacity,
    size_t *output_size);

/** Encodes any current group-aware payload. payload->group_id must be non-zero. */
int tr_raft_transport_encode_payload(
    tr_raft_transport_session_t *session,
    const tr_raft_transport_payload_t *payload,
    uint8_t *output,
    size_t output_capacity,
    size_t *output_size);

/** Feeds any stream fragment; decoded values are borrowed during callbacks. */
int tr_raft_transport_feed(tr_raft_transport_session_t *session,
                           const uint8_t *data,
                           size_t size);
int tr_raft_transport_get_status(const tr_raft_transport_session_t *session,
                                 tr_raft_transport_status_t *out_status);

#ifdef __cplusplus
}
#endif

#endif
