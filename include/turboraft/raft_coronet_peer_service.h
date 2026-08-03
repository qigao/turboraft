#ifndef TURBORAFT_RAFT_CORONET_PEER_SERVICE_H
#define TURBORAFT_RAFT_CORONET_PEER_SERVICE_H

#include <turboraft/raft_coronet_transport.h>
#include <turboraft/raft_snapshot_receiver.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct tr_raft_coronet_peer_service
    tr_raft_coronet_peer_service_t;

#define TR_RAFT_CORONET_MAX_OUTBOUND_QUEUE_CAPACITY 65536U

typedef struct tr_raft_coronet_peer_service_config {
    coro_context_t *context;
    tr_raft_coronet_peer_manager_config_t manager;
    const tr_raft_coronet_identity_entry_t *identity_entries;
    size_t identity_entry_count;
    size_t outbound_queue_capacity;
    tr_raft_coronet_admit_owned_socket_fn admit_owned_socket;
    tr_raft_coronet_connect_outbound_fn connect_outbound;
    /* Borrowed; optional and must outlive the service. */
    tr_raft_snapshot_receiver_t *snapshot_receiver;
    tr_raft_coronet_snapshot_handler_fn on_snapshot_ack;
    void *snapshot_ack_context;
} tr_raft_coronet_peer_service_config_t;

typedef struct tr_raft_coronet_peer_service_step_result {
    size_t scheduler_count;
    size_t attempted_count;
    size_t newly_connected_count;
    size_t failed_count;
    int first_error;
} tr_raft_coronet_peer_service_step_result_t;

typedef struct tr_raft_coronet_peer_service_status {
    size_t peer_count;
    size_t scheduler_count;
    uint64_t identity_generation;
    uint32_t active_operation_count;
    size_t outbound_queue_capacity;
    size_t queued_message_count;
    size_t queued_payload_count;
    size_t active_reader_count;
    int writer_active;
    int stopping;
    int last_pump_error;
    int inbound_configured;
    int step_active;
    uint64_t snapshot_install_count;
    uint64_t snapshot_reject_count;
    uint64_t snapshot_ack_count;
} tr_raft_coronet_peer_service_status_t;

int tr_raft_coronet_peer_service_create(
    const tr_raft_coronet_peer_service_config_t *config,
    tr_raft_coronet_peer_service_t **out_service);

/**
 * Requires the CoroNet listener to be closed and drained. Returns TURBO_EBUSY
 * without changing ownership when an operation or callback is active.
 */
int tr_raft_coronet_peer_service_destroy(
    tr_raft_coronet_peer_service_t *service);

/**
 * Rejects new work and closes managed sockets to wake I/O pumps. The caller
 * must run the CoroNet context until active_operation_count becomes zero before
 * destroy. Pending queue entries are discarded only by destroy.
 */
int tr_raft_coronet_peer_service_stop(
    tr_raft_coronet_peer_service_t *service);

/**
 * Configures the single inbound handler. manager, admit_owned_socket, and the
 * handshake identity resolver fields must be NULL because the service injects
 * its owned dependencies.
 */
int tr_raft_coronet_peer_service_configure_inbound(
    tr_raft_coronet_peer_service_t *service,
    const tr_raft_coronet_inbound_service_config_t *config);

/** CoroNet listen_on handler; context must be the peer service. */
void tr_raft_coronet_peer_service_handle_inbound(coro_socket_t *socket,
                                                  void *context);

/**
 * Adds one outbound scheduler. manager, connect_outbound, and the handshake
 * identity resolver fields must be NULL because the service injects them.
 */
int tr_raft_coronet_peer_service_add_outbound(
    tr_raft_coronet_peer_service_t *service,
    const tr_raft_coronet_dial_scheduler_config_t *config);

int tr_raft_coronet_peer_service_step(
    tr_raft_coronet_peer_service_t *service,
    uint64_t now_ms,
    tr_raft_coronet_peer_service_step_result_t *out_result);

int tr_raft_coronet_peer_service_reset_peer(
    tr_raft_coronet_peer_service_t *service,
    tr_raft_node_id_t peer_node_id,
    uint64_t now_ms);

/**
 * Close one outbound peer session without discarding queued messages.
 *
 * The active reader owns final session release. After it observes the close,
 * the peer dial scheduler is reset with now_ms. If no reader is active, the
 * scheduler is reset immediately. Call step() to perform the reconnect.
 *
 * @return TURBO_OK on acceptance, TURBO_EINVAL for invalid arguments,
 * TURBO_EBUSY during a step or duplicate disconnect, TURBO_EPIPE after stop,
 * or TURBO_EPROTO when the peer has no outbound scheduler.
 */
int tr_raft_coronet_peer_service_disconnect_peer(
    tr_raft_coronet_peer_service_t *service,
    tr_raft_node_id_t peer_node_id,
    uint64_t now_ms);

int tr_raft_coronet_peer_service_get_peer_dial_status(
    const tr_raft_coronet_peer_service_t *service,
    tr_raft_node_id_t peer_node_id,
    tr_raft_coronet_dial_status_t *out_status);

/**
 * Runtime transport adapter. Copies one message into the target peer's bounded
 * local queue. Returns TURBO_ENOSPC on backpressure and TURBO_EPIPE after stop.
 * The service is single-context state; call from its CoroNet owner thread.
 */
int tr_raft_coronet_peer_service_enqueue(void *context,
                                         const tr_raft_message_t *message);

/** Copies one tagged payload into the target peer's bounded FIFO. */
int tr_raft_coronet_peer_service_enqueue_payload(
    tr_raft_coronet_peer_service_t *service,
    const tr_raft_coronet_payload_t *payload);

/** Replaces the immutable identity snapshot at a quiescent boundary. */
int tr_raft_coronet_peer_service_update_identities(
    tr_raft_coronet_peer_service_t *service,
    const tr_raft_coronet_identity_entry_t *entries,
    size_t entry_count);

int tr_raft_coronet_peer_service_get_status(
    const tr_raft_coronet_peer_service_t *service,
    tr_raft_coronet_peer_service_status_t *out_status);

#ifdef __cplusplus
}
#endif

#endif
