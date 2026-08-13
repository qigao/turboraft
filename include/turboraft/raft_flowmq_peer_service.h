#ifndef TURBORAFT_RAFT_FLOWMQ_PEER_SERVICE_H
#define TURBORAFT_RAFT_FLOWMQ_PEER_SERVICE_H

#include <turboraft/raft_coronet_transport.h>

#include <flowmq.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TR_RAFT_FLOWMQ_MAX_IDENTITY_SIZE 255U
#define TR_RAFT_FLOWMQ_MAX_OUTBOUND_QUEUE_CAPACITY 65536U
#define TR_RAFT_FLOWMQ_DEFAULT_SEND_BATCH_ITEMS 1U
#define TR_RAFT_FLOWMQ_MAX_SEND_BATCH_ITEMS 256U
#define TR_RAFT_FLOWMQ_MIN_INBOUND_QUEUE_BYTES 16384U
#define TR_RAFT_FLOWMQ_MAX_INBOUND_QUEUE_BYTES (64U * 1024U * 1024U)

typedef struct tr_raft_flowmq_peer_service tr_raft_flowmq_peer_service_t;

/** One directed DEALER connection to a remote node's ROUTER endpoint. */
typedef struct tr_raft_flowmq_peer_config {
  tr_raft_node_id_t node_id;
  /** FMQ identity expected on this peer's inbound DEALER. */
  const char *peer_identity;
  /** DEALER/CONNECT endpoint. TurboRaft owns its context and callbacks. */
  flowmq_connect_endpoint_config_t endpoint;
} tr_raft_flowmq_peer_config_t;

typedef struct tr_raft_flowmq_peer_service_config {
  tr_raft_handshake_config_t handshake;
  /** ROUTER/BIND TLS or WSS endpoint. TurboRaft owns its context and callbacks. */
  flowmq_router_endpoint_config_t router_endpoint;
  const tr_raft_flowmq_peer_config_t *peers;
  size_t peer_count;
  size_t outbound_queue_capacity;
  /** Zero preserves the historical one-payload-per-peer step behavior. */
  size_t max_send_batch_items;
  /** Zero derives the bound from max_send_batch_items and max frame size. */
  size_t max_send_batch_bytes;
  /** Power-of-two byte capacity between I/O callbacks and step(). */
  size_t inbound_queue_capacity_bytes;
  tr_raft_coronet_message_handler_fn on_message;
  void *message_context;
  tr_raft_coronet_snapshot_handler_fn on_snapshot;
  void *snapshot_context;
} tr_raft_flowmq_peer_service_config_t;

typedef struct tr_raft_flowmq_peer_service_step_result {
  size_t peer_count;
  size_t control_frames_sent;
  size_t payload_frames_sent;
  size_t payload_batches_sent;
  size_t blocked_peer_count;
  size_t failed_peer_count;
  int first_error;
} tr_raft_flowmq_peer_service_step_result_t;

typedef struct tr_raft_flowmq_peer_service_status {
  size_t peer_count;
  size_t outbound_queue_capacity;
  size_t max_send_batch_items;
  size_t max_send_batch_bytes;
  size_t inbound_queue_capacity_bytes;
  size_t queued_inbound_bytes;
  size_t queued_payload_count;
  size_t handshake_complete_count;
  uint32_t callback_depth;
  int started;
  int stopping;
  int step_active;
  int last_error;
} tr_raft_flowmq_peer_service_status_t;

/**
 * Creates one ROUTER and one DEALER per peer. Each FlowMQ endpoint owns its
 * CoroNet execution context; the service owns their complete lifecycle.
 * Only TLS/WSS endpoints are accepted. URI schemes select the FlowMQ transport;
 * no transport fallback is performed.
 *
 * Public lifecycle, enqueue, step, and status calls are serialized on one
 * control thread. Endpoint callback fields and context fields must be zero;
 * TurboRaft installs private callbacks and owned CoroNet contexts. Independent
 * FlowMQ I/O contexts serialize ingress through a
 * producer mutex and copy frames into one bounded SPSC queue; Raft callbacks
 * run from step() on the control thread.
 */
int tr_raft_flowmq_peer_service_create(const tr_raft_flowmq_peer_service_config_t *config,
                                       tr_raft_flowmq_peer_service_t **out_service);

/**
 * start() synchronously makes the local ROUTER ready, then starts one bounded
 * connector thread per peer. A remote peer being offline does not fail local
 * startup; each connector retries with endpoint.reconnect_initial_ms. Zero
 * disables retry after the first connection failure.
 */
int tr_raft_flowmq_peer_service_start(tr_raft_flowmq_peer_service_t *service);

/**
 * Drains copied inbound frames, drives pending Raft HELLO/ACK control frames,
 * and sends one configured bounded payload batch per ready peer. Transient disconnection
 * keeps queue ownership in this service. Inbound saturation fails fast with
 * TURBO_ENOSPC because an authentication or protocol event may have been lost.
 */
int tr_raft_flowmq_peer_service_step(tr_raft_flowmq_peer_service_t *service,
                                     tr_raft_flowmq_peer_service_step_result_t *out_result);

/**
 * Rejects new work, requests connector shutdown, joins every connector, then
 * stops DEALER and ROUTER endpoints. A connector currently inside FlowMQ start
 * may delay shutdown by at most the endpoint connection timeout.
 */
int tr_raft_flowmq_peer_service_stop(tr_raft_flowmq_peer_service_t *service);

/** Requires stop completion and no active callback or step. */
int tr_raft_flowmq_peer_service_destroy(tr_raft_flowmq_peer_service_t *service);

/** Runtime transport adapter; copies one Raft message into its peer FIFO. */
int tr_raft_flowmq_peer_service_enqueue(void *context, const tr_raft_message_t *message);

/** Copies one tagged Raft/snapshot payload into its peer FIFO. */
int tr_raft_flowmq_peer_service_enqueue_payload(tr_raft_flowmq_peer_service_t *service,
                                                const tr_raft_coronet_payload_t *payload);

int tr_raft_flowmq_peer_service_get_status(const tr_raft_flowmq_peer_service_t *service,
                                           tr_raft_flowmq_peer_service_status_t *out_status);

#ifdef __cplusplus
}
#endif

#endif
