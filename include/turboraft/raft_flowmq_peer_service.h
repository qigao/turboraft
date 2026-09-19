#ifndef TURBORAFT_RAFT_FLOWMQ_PEER_SERVICE_H
#define TURBORAFT_RAFT_FLOWMQ_PEER_SERVICE_H

#include <turboraft/raft_transport.h>

#include <flowmq.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TR_RAFT_FLOWMQ_MAX_IDENTITY_SIZE 255U
#define TR_RAFT_FLOWMQ_MAX_OUTBOUND_QUEUE_CAPACITY 65536U
#define TR_RAFT_FLOWMQ_RECOMMENDED_SEND_BATCH_ITEMS 16U
#define TR_RAFT_FLOWMQ_RECOMMENDED_RECEIVE_BATCH_ITEMS 64U
#define TR_RAFT_FLOWMQ_RECOMMENDED_INFLIGHT_DATA_BYTES (4U * 1024U * 1024U)

typedef struct tr_raft_flowmq_peer_service tr_raft_flowmq_peer_service_t;

typedef struct tr_raft_flowmq_tls_config {
    const char *ca_file;
    const char *cert_file;
    const char *key_file;
    const char *key_password;
    const char *server_name;
    int require_client_certificate;
} tr_raft_flowmq_tls_config_t;

typedef struct tr_raft_flowmq_peer_config {
    tr_raft_node_id_t node_id;
    /** Required completed negotiation for this exact peer. */
    const tr_raft_handshake_result_t *handshake;
    /** Exact ROUTER identity presented by this peer. */
    const char *identity;
    /** FlowMQ endpoint URI, for example tcp://127.0.0.1:9002. */
    const char *endpoint;
    tr_raft_flowmq_tls_config_t tls;
} tr_raft_flowmq_peer_config_t;

typedef struct tr_raft_flowmq_peer_service_config {
    tr_raft_handshake_config_t protocol;
    const char *bind_endpoint;
    const char *local_identity;
    tr_raft_flowmq_tls_config_t tls;
    const tr_raft_flowmq_peer_config_t *peers;
    size_t peer_count;
    /** All capacity, batch, HWM, and reconnect fields are required. */
    size_t outbound_queue_capacity;
    size_t max_send_batch_items;
    size_t max_receive_batch_items;
    size_t max_inflight_data_bytes;
    size_t send_hwm_messages;
    size_t receive_hwm_messages;
    size_t send_hwm_bytes;
    size_t receive_hwm_bytes;
    uint32_t reconnect_initial_ms;
    uint32_t reconnect_max_ms;
    uint32_t heartbeat_interval_ms;
    uint32_t heartbeat_timeout_ms;
    tr_raft_transport_payload_handler_fn on_payload;
    void *payload_context;
} tr_raft_flowmq_peer_service_config_t;

typedef struct tr_raft_flowmq_peer_service_step_result {
    size_t received_frames;
    size_t sent_frames;
    size_t blocked_peer_count;
    size_t failed_peer_count;
    int first_error;
} tr_raft_flowmq_peer_service_step_result_t;

typedef struct tr_raft_flowmq_peer_service_status {
    size_t peer_count;
    size_t queued_payload_count;
    size_t queued_data_bytes;
    size_t outbound_queue_capacity;
    size_t max_inflight_data_bytes;
    uint64_t frames_sent;
    uint64_t frames_received;
    int started;
    int stopping;
    int step_active;
    int last_error;
} tr_raft_flowmq_peer_service_status_t;

/**
 * Creates a caller-driven service with one ROUTER and one DEALER per peer.
 * No worker thread is created. Every lifecycle, enqueue, and step call belongs
 * to one owner thread. FlowMQ copies a frame before a successful send returns.
 */
int tr_raft_flowmq_peer_service_create(
    const tr_raft_flowmq_peer_service_config_t *config,
    tr_raft_flowmq_peer_service_t **out_service);

/** Binds the ROUTER and admits asynchronous DEALER connects. */
int tr_raft_flowmq_peer_service_start(tr_raft_flowmq_peer_service_t *service);

/** Drives I/O and dispatches bounded receive/send batches. */
int tr_raft_flowmq_peer_service_step(
    tr_raft_flowmq_peer_service_t *service,
    tr_raft_flowmq_peer_service_step_result_t *out_result);

/** Closes sockets in DEALER-before-ROUTER order and terminates the context. */
int tr_raft_flowmq_peer_service_stop(tr_raft_flowmq_peer_service_t *service);

/** Requires stop after start; releases queued payloads and service storage. */
int tr_raft_flowmq_peer_service_destroy(tr_raft_flowmq_peer_service_t *service);

/** Copies one Raft message with explicit group identity into the peer FIFO. */
int tr_raft_flowmq_peer_service_enqueue_group(
    tr_raft_flowmq_peer_service_t *service,
    tr_raft_group_id_t group_id,
    const tr_raft_message_t *message);
int tr_raft_flowmq_peer_service_enqueue_payload(
    tr_raft_flowmq_peer_service_t *service,
    const tr_raft_transport_payload_t *payload);

int tr_raft_flowmq_peer_service_get_status(
    const tr_raft_flowmq_peer_service_t *service,
    tr_raft_flowmq_peer_service_status_t *out_status);

#ifdef __cplusplus
}
#endif

#endif
