#ifndef TURBORAFT_RAFT_PEER_HANDSHAKE_H
#define TURBORAFT_RAFT_PEER_HANDSHAKE_H

#include <turboraft/raft_wire_codec.h>

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TR_RAFT_HANDSHAKE_FORMAT_VERSION 1U
#define TR_RAFT_HANDSHAKE_RECORD_SIZE 84U
#define TR_RAFT_HANDSHAKE_LENGTH_PREFIX_SIZE 4U
#define TR_RAFT_HANDSHAKE_PACKET_SIZE \
    (TR_RAFT_HANDSHAKE_LENGTH_PREFIX_SIZE + TR_RAFT_HANDSHAKE_RECORD_SIZE)
#define TR_RAFT_HANDSHAKE_PROCESS_ID_SIZE 16U
#define TR_RAFT_HANDSHAKE_WIRE_MAJOR 1U
#define TR_RAFT_HANDSHAKE_WIRE_MINOR 0U
#define TR_RAFT_HANDSHAKE_MIN_FRAME_SIZE \
    (TR_RAFT_WIRE_HEADER_SIZE + TR_RAFT_WIRE_MAX_RAFT_PAYLOAD_SIZE)
#define TR_RAFT_HANDSHAKE_MIN_SNAPSHOT_CHUNK_SIZE 4096U

typedef enum tr_raft_handshake_message_type {
    TR_RAFT_HANDSHAKE_HELLO = 1,
    TR_RAFT_HANDSHAKE_HELLO_ACK = 2
} tr_raft_handshake_message_type_t;

typedef struct tr_raft_process_incarnation {
    uint8_t bytes[TR_RAFT_HANDSHAKE_PROCESS_ID_SIZE];
} tr_raft_process_incarnation_t;

typedef struct tr_raft_handshake_config {
    tr_raft_cluster_id_t cluster_id;
    tr_raft_node_id_t local_node_id;
    tr_raft_process_incarnation_t process_incarnation;
    uint64_t config_epoch;
    /** Reserved for future optional capabilities; current baseline is zero. */
    uint64_t feature_bits;
    uint16_t wire_major_min;
    uint16_t wire_major_max;
    uint16_t wire_minor_min;
    uint16_t wire_minor_max;
    uint32_t max_frame_size;
    uint32_t max_snapshot_chunk_size;
} tr_raft_handshake_config_t;

typedef struct tr_raft_handshake_message {
    tr_raft_handshake_message_type_t type;
    uint16_t wire_major_min;
    uint16_t wire_major_max;
    uint16_t wire_minor_min;
    uint16_t wire_minor_max;
    uint64_t feature_bits;
    tr_raft_cluster_id_t cluster_id;
    tr_raft_node_id_t node_id;
    tr_raft_process_incarnation_t process_incarnation;
    uint64_t config_epoch;
    uint32_t max_frame_size;
    uint32_t max_snapshot_chunk_size;
} tr_raft_handshake_message_t;

typedef struct tr_raft_handshake_result {
    int complete;
    tr_raft_cluster_id_t cluster_id;
    tr_raft_node_id_t local_node_id;
    tr_raft_node_id_t peer_node_id;
    tr_raft_process_incarnation_t peer_process_incarnation;
    uint64_t peer_config_epoch;
    uint64_t feature_bits;
    uint16_t wire_major;
    uint16_t wire_minor;
    uint32_t max_frame_size;
    uint32_t max_snapshot_chunk_size;
} tr_raft_handshake_result_t;

typedef enum tr_raft_handshake_exchange_state {
    TR_RAFT_HANDSHAKE_EXCHANGE_NEW = 0,
    TR_RAFT_HANDSHAKE_EXCHANGE_WAIT_HELLO,
    TR_RAFT_HANDSHAKE_EXCHANGE_WAIT_ACK,
    TR_RAFT_HANDSHAKE_EXCHANGE_COMPLETE,
    TR_RAFT_HANDSHAKE_EXCHANGE_FAULTED
} tr_raft_handshake_exchange_state_t;

typedef struct tr_raft_handshake_exchange tr_raft_handshake_exchange_t;

int tr_raft_handshake_make_hello(
    const tr_raft_handshake_config_t *config,
    tr_raft_handshake_message_t *out_hello);

int tr_raft_handshake_encode(const tr_raft_handshake_message_t *message,
                             uint8_t *output,
                             size_t output_capacity,
                             size_t *output_size);

int tr_raft_handshake_decode(const uint8_t *packet,
                             size_t packet_size,
                             tr_raft_handshake_message_t *out_message);

/**
 * Validates a remote HELLO against the authenticated TLS node identity,
 * creates the local ACK, and records the negotiated baseline/limits.
 */
int tr_raft_handshake_negotiate(
    const tr_raft_handshake_config_t *local,
    tr_raft_node_id_t authenticated_peer_node_id,
    const tr_raft_handshake_message_t *remote_hello,
    tr_raft_handshake_message_t *out_local_ack,
    tr_raft_handshake_result_t *out_result);

/** Marks result complete only when the remote ACK exactly matches negotiation. */
int tr_raft_handshake_validate_ack(
    tr_raft_handshake_result_t *result,
    const tr_raft_handshake_message_t *remote_ack);

int tr_raft_handshake_result_validate(
    const tr_raft_handshake_result_t *result,
    const tr_raft_cluster_id_t *cluster_id,
    tr_raft_node_id_t local_node_id,
    tr_raft_node_id_t peer_node_id);

int tr_raft_handshake_exchange_create(
    const tr_raft_handshake_config_t *local,
    tr_raft_node_id_t authenticated_peer_node_id,
    tr_raft_handshake_exchange_t **out_exchange);

void tr_raft_handshake_exchange_destroy(
    tr_raft_handshake_exchange_t *exchange);

int tr_raft_handshake_exchange_start(
    tr_raft_handshake_exchange_t *exchange,
    uint8_t *output,
    size_t output_capacity,
    size_t *output_size);

int tr_raft_handshake_exchange_feed(
    tr_raft_handshake_exchange_t *exchange,
    const uint8_t *data,
    size_t size,
    size_t *consumed_size,
    uint8_t *output,
    size_t output_capacity,
    size_t *output_size);

int tr_raft_handshake_exchange_get_state(
    const tr_raft_handshake_exchange_t *exchange,
    tr_raft_handshake_exchange_state_t *out_state);

int tr_raft_handshake_exchange_get_result(
    const tr_raft_handshake_exchange_t *exchange,
    tr_raft_handshake_result_t *out_result);

#ifdef __cplusplus
}
#endif

#endif
