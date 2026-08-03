#ifndef TURBORAFT_RAFT_CORONET_TRANSPORT_H
#define TURBORAFT_RAFT_CORONET_TRANSPORT_H

#include <turboraft/raft_core.h>
#include <turboraft/raft_peer_handshake.h>
#include <turboraft/raft_wire_codec.h>

#include <CoroNet/turbo_coro_socket.h>

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TR_RAFT_CORONET_LENGTH_PREFIX_SIZE 4U
#define TR_RAFT_CORONET_MAX_PACKET_SIZE \
    (TR_RAFT_CORONET_LENGTH_PREFIX_SIZE + TR_RAFT_WIRE_MAX_FRAME_SIZE)
#define TR_RAFT_CORONET_MAX_PEERS (TR_RAFT_MAX_VOTERS - 1U)

typedef enum tr_raft_coronet_state {
    TR_RAFT_CORONET_STATE_DETACHED = 0,
    TR_RAFT_CORONET_STATE_CONNECTED,
    TR_RAFT_CORONET_STATE_FAULTED
} tr_raft_coronet_state_t;

typedef enum tr_raft_coronet_connection_direction {
    TR_RAFT_CORONET_CONNECTION_INBOUND = 0,
    TR_RAFT_CORONET_CONNECTION_OUTBOUND
} tr_raft_coronet_connection_direction_t;

typedef int (*tr_raft_coronet_message_handler_fn)(
    void *context,
    const tr_raft_message_t *message);

typedef struct tr_raft_coronet_payload {
    tr_raft_wire_payload_kind_t kind;
    union {
        tr_raft_message_t raft;
        tr_raft_snapshot_chunk_t snapshot_chunk;
        tr_raft_snapshot_ack_t snapshot_ack;
    } data;
} tr_raft_coronet_payload_t;

typedef int (*tr_raft_coronet_snapshot_handler_fn)(
    void *context,
    const tr_raft_coronet_payload_t *payload);

typedef int (*tr_raft_coronet_resolve_peer_identity_fn)(
    void *context,
    const char *verified_certificate_sha256,
    tr_raft_node_id_t *out_peer_node_id);

typedef struct tr_raft_coronet_handshake_config {
    const tr_raft_handshake_config_t *handshake;
    uint64_t timeout_ms;
    tr_raft_coronet_resolve_peer_identity_fn resolve_peer_identity;
    void *identity_context;
} tr_raft_coronet_handshake_config_t;

typedef struct tr_raft_coronet_receive_remainder {
    void *receive_buffer;
    const uint8_t *data;
    size_t size;
} tr_raft_coronet_receive_remainder_t;

typedef struct tr_raft_coronet_session_config {
    /* NULL selects deterministic encode/feed-only operation. */
    coro_socket_t *socket;
    /* When nonzero, session_destroy closes socket on the owner-loop thread. */
    int owns_socket;
    tr_raft_cluster_id_t cluster_id;
    tr_raft_node_id_t local_node_id;
    tr_raft_node_id_t peer_node_id;
    uint64_t first_outbound_message_id;
    /* Required for connected sockets; ignored only in detached operation. */
    const tr_raft_handshake_result_t *handshake;
    tr_raft_coronet_message_handler_fn on_message;
    void *message_context;
    tr_raft_coronet_snapshot_handler_fn on_snapshot;
    void *snapshot_context;
} tr_raft_coronet_session_config_t;

typedef struct tr_raft_coronet_owned_socket_admission_config {
    tr_raft_coronet_handshake_config_t handshake;
    tr_raft_coronet_connection_direction_t direction;
    tr_raft_node_id_t expected_peer_node_id;
    uint64_t first_outbound_message_id;
    uint64_t peer_idle_timeout_ms;
    tr_raft_coronet_message_handler_fn on_message;
    void *message_context;
    tr_raft_coronet_snapshot_handler_fn on_snapshot;
    void *snapshot_context;
} tr_raft_coronet_owned_socket_admission_config_t;

typedef struct tr_raft_coronet_outbound_config {
    coro_context_t *context;
    const char *connect_host;
    const char *request_host;
    int port;
    uint64_t connect_timeout_ms;
    turbo_tls_client_config_t tls;
    tr_raft_coronet_owned_socket_admission_config_t admission;
} tr_raft_coronet_outbound_config_t;

typedef struct tr_raft_coronet_status {
    tr_raft_coronet_state_t state;
    uint64_t last_outbound_message_id;
    uint64_t last_inbound_message_id;
    uint64_t frames_sent;
    uint64_t frames_received;
    uint64_t bytes_sent;
    uint64_t bytes_received;
} tr_raft_coronet_status_t;

typedef struct tr_raft_coronet_session tr_raft_coronet_session_t;
typedef struct tr_raft_coronet_peer_manager tr_raft_coronet_peer_manager_t;
typedef struct tr_raft_coronet_dial_scheduler
    tr_raft_coronet_dial_scheduler_t;
typedef struct tr_raft_coronet_inbound_service
    tr_raft_coronet_inbound_service_t;
typedef struct tr_raft_coronet_identity_registry
    tr_raft_coronet_identity_registry_t;

#define TR_RAFT_CORONET_MAX_CERT_IDENTITIES (TR_RAFT_MAX_VOTERS * 2U)

typedef struct tr_raft_coronet_identity_entry {
    char certificate_sha256[CORO_TLS_PEER_CERT_SHA256_CAPACITY];
    tr_raft_node_id_t node_id;
} tr_raft_coronet_identity_entry_t;

#define TR_RAFT_CORONET_ENDPOINT_HOST_CAPACITY 256U

typedef struct tr_raft_coronet_endpoint {
    char connect_host[TR_RAFT_CORONET_ENDPOINT_HOST_CAPACITY];
    char request_host[TR_RAFT_CORONET_ENDPOINT_HOST_CAPACITY];
    int port;
} tr_raft_coronet_endpoint_t;

typedef enum tr_raft_coronet_dial_state {
    TR_RAFT_CORONET_DIAL_WAITING = 0,
    TR_RAFT_CORONET_DIAL_CONNECTED,
    TR_RAFT_CORONET_DIAL_EXHAUSTED
} tr_raft_coronet_dial_state_t;

typedef int (*tr_raft_coronet_resolve_endpoint_fn)(
    void *context,
    tr_raft_node_id_t peer_node_id,
    tr_raft_coronet_endpoint_t *out_endpoint);

typedef int (*tr_raft_coronet_connect_outbound_fn)(
    tr_raft_coronet_peer_manager_t *manager,
    const tr_raft_coronet_outbound_config_t *config,
    tr_raft_node_id_t *out_peer_node_id);

typedef int (*tr_raft_coronet_retry_decider_fn)(void *context,
                                                int error_code);

typedef struct tr_raft_coronet_dial_scheduler_config {
    tr_raft_coronet_peer_manager_t *manager;
    tr_raft_coronet_outbound_config_t outbound;
    tr_raft_coronet_resolve_endpoint_fn resolve_endpoint;
    void *resolve_context;
    tr_raft_coronet_connect_outbound_fn connect_outbound;
    tr_raft_coronet_retry_decider_fn is_retryable;
    void *retry_context;
    uint64_t initial_retry_delay_ms;
    uint64_t max_retry_delay_ms;
    uint32_t max_attempts;
} tr_raft_coronet_dial_scheduler_config_t;

typedef struct tr_raft_coronet_dial_status {
    tr_raft_coronet_dial_state_t state;
    uint32_t attempt_count;
    uint64_t next_attempt_ms;
    int last_error;
} tr_raft_coronet_dial_status_t;

typedef int (*tr_raft_coronet_admit_owned_socket_fn)(
    tr_raft_coronet_peer_manager_t *manager,
    coro_socket_t *socket,
    const tr_raft_coronet_owned_socket_admission_config_t *config,
    tr_raft_node_id_t *out_peer_node_id);

typedef void (*tr_raft_coronet_inbound_result_fn)(void *context,
                                                  int result,
                                                  tr_raft_node_id_t peer_node_id);

typedef struct tr_raft_coronet_inbound_service_config {
    tr_raft_coronet_peer_manager_t *manager;
    tr_raft_coronet_owned_socket_admission_config_t admission;
    tr_raft_coronet_admit_owned_socket_fn admit_owned_socket;
    tr_raft_coronet_inbound_result_fn on_result;
    void *result_context;
} tr_raft_coronet_inbound_service_config_t;

typedef struct tr_raft_coronet_inbound_status {
    uint64_t accepted_socket_count;
    uint64_t admitted_socket_count;
    uint64_t rejected_socket_count;
    uint32_t active_admission_count;
    int last_error;
} tr_raft_coronet_inbound_status_t;

typedef struct tr_raft_coronet_peer_manager_config {
    tr_raft_cluster_id_t cluster_id;
    tr_raft_node_id_t local_node_id;
    const tr_raft_node_id_t *peer_node_ids;
    size_t peer_count;
} tr_raft_coronet_peer_manager_config_t;

typedef struct tr_raft_coronet_peer_manager_status {
    size_t configured_count;
    size_t attached_count;
    size_t detached_count;
    size_t connected_count;
    size_t faulted_count;
} tr_raft_coronet_peer_manager_status_t;

typedef struct tr_raft_coronet_peer_status {
    tr_raft_node_id_t peer_node_id;
    int attached;
    tr_raft_coronet_status_t session;
} tr_raft_coronet_peer_status_t;

/**
 * Creates a single-owner peer session around an optional caller-owned socket.
 * The decoded message passed to on_message is borrowed for that call only.
 */
int tr_raft_coronet_session_create(
    const tr_raft_coronet_session_config_t *config,
    tr_raft_coronet_session_t **out_session);

/**
 * Releases session state and an owned socket. Destruction of the session whose
 * callback is active is deferred until that callback returns.
 */
int tr_raft_coronet_session_destroy(tr_raft_coronet_session_t *session);

/** Matches the runtime transport enqueue callback. */
int tr_raft_coronet_enqueue(void *context, const tr_raft_message_t *message);

/** Sends one tagged payload through the session's sole socket writer. */
int tr_raft_coronet_enqueue_payload(
    tr_raft_coronet_session_t *session,
    const tr_raft_coronet_payload_t *payload);

/** Receives one CoroNet chunk and dispatches every complete frame in it. */
int tr_raft_coronet_receive_once(tr_raft_coronet_session_t *session);

/** Feeds an arbitrary stream fragment into the bounded frame decoder. */
int tr_raft_coronet_feed(tr_raft_coronet_session_t *session,
                         const uint8_t *data,
                         size_t size);

/** Encodes one length-prefixed packet and consumes one outbound message id. */
int tr_raft_coronet_encode_packet(tr_raft_coronet_session_t *session,
                                  const tr_raft_message_t *message,
                                  uint8_t *output,
                                  size_t output_capacity,
                                  size_t *output_size);

int tr_raft_coronet_encode_payload_packet(
    tr_raft_coronet_session_t *session,
    const tr_raft_coronet_payload_t *payload,
    uint8_t *output,
    size_t output_capacity,
    size_t *output_size);

/** Copies a diagnostic snapshot. */
int tr_raft_coronet_get_status(const tr_raft_coronet_session_t *session,
                               tr_raft_coronet_status_t *out_status);

/**
 * Runs mutual HELLO/ACK exchange on a verified TLS socket. The caller owns the
 * socket; a returned receive remainder must be applied or released exactly once.
 */
int tr_raft_coronet_handshake_exchange(
    coro_socket_t *socket,
    const tr_raft_coronet_handshake_config_t *config,
    tr_raft_handshake_result_t *out_result,
    tr_raft_coronet_receive_remainder_t *out_remainder);

void tr_raft_coronet_receive_remainder_release(
    tr_raft_coronet_receive_remainder_t *remainder);

/** Feeds and releases a remainder regardless of the feed result. */
int tr_raft_coronet_receive_remainder_apply(
    tr_raft_coronet_session_t *session,
    tr_raft_coronet_receive_remainder_t *remainder);

/**
 * Creates a bounded single-owner router. Peer IDs are copied and must be
 * nonzero, strictly ascending, unique, and different from local_node_id.
 */
int tr_raft_coronet_peer_manager_create(
    const tr_raft_coronet_peer_manager_config_t *config,
    tr_raft_coronet_peer_manager_t **out_manager);

/**
 * Destroys the manager and every owned session. Returns TURBO_EBUSY when
 * called reentrantly from a managed session callback.
 */
int tr_raft_coronet_peer_manager_destroy(
    tr_raft_coronet_peer_manager_t *manager);

/**
 * Transfers session ownership to the manager on success. On failure the
 * caller retains ownership.
 */
int tr_raft_coronet_peer_manager_attach(
    tr_raft_coronet_peer_manager_t *manager,
    tr_raft_coronet_session_t *session);

/**
 * Returns the deterministic connection direction for one node pair. The
 * smaller node ID dials outbound and the larger node ID accepts inbound.
 */
int tr_raft_coronet_expected_direction(
    tr_raft_node_id_t local_node_id,
    tr_raft_node_id_t peer_node_id,
    tr_raft_coronet_connection_direction_t *out_direction);

/**
 * Admits one connected session using the deterministic direction rule and
 * transfers ownership to the manager on success.
 */
int tr_raft_coronet_peer_manager_admit(
    tr_raft_coronet_peer_manager_t *manager,
    tr_raft_coronet_connection_direction_t direction,
    tr_raft_coronet_session_t *session);

/**
 * Consumes one connected TLS socket on every path. On success the manager owns
 * the resulting session and socket; on failure the socket is closed.
 */
int tr_raft_coronet_peer_manager_admit_owned_socket(
    tr_raft_coronet_peer_manager_t *manager,
    coro_socket_t *socket,
    const tr_raft_coronet_owned_socket_admission_config_t *config,
    tr_raft_node_id_t *out_peer_node_id);

int tr_raft_coronet_outbound_config_validate(
    const tr_raft_coronet_outbound_config_t *config);

int tr_raft_coronet_peer_manager_connect_outbound(
    tr_raft_coronet_peer_manager_t *manager,
    const tr_raft_coronet_outbound_config_t *config,
    tr_raft_node_id_t *out_peer_node_id);

/**
 * Creates a single-event-loop dial state machine. Pointer fields copied from
 * config must outlive the scheduler. No coroutine or thread is spawned.
 */
int tr_raft_coronet_dial_scheduler_create(
    const tr_raft_coronet_dial_scheduler_config_t *config,
    tr_raft_coronet_dial_scheduler_t **out_scheduler);

void tr_raft_coronet_dial_scheduler_destroy(
    tr_raft_coronet_dial_scheduler_t *scheduler);

/** Performs at most one endpoint resolution and connection attempt. */
int tr_raft_coronet_dial_scheduler_step(
    tr_raft_coronet_dial_scheduler_t *scheduler,
    uint64_t now_ms);

/** Starts a fresh retry cycle after the owner has detached a failed session. */
int tr_raft_coronet_dial_scheduler_reset(
    tr_raft_coronet_dial_scheduler_t *scheduler,
    uint64_t now_ms);

int tr_raft_coronet_dial_scheduler_get_status(
    const tr_raft_coronet_dial_scheduler_t *scheduler,
    tr_raft_coronet_dial_status_t *out_status);

int tr_raft_coronet_inbound_service_config_validate(
    const tr_raft_coronet_inbound_service_config_t *config);

int tr_raft_coronet_inbound_service_create(
    const tr_raft_coronet_inbound_service_config_t *config,
    tr_raft_coronet_inbound_service_t **out_service);

/** Requires the CoroNet listener to be closed and its handlers drained. */
int tr_raft_coronet_inbound_service_destroy(
    tr_raft_coronet_inbound_service_t *service);

/** CoroNet listen_on handler; consumes socket on every path. */
void tr_raft_coronet_inbound_service_handle(coro_socket_t *socket,
                                             void *context);

int tr_raft_coronet_inbound_service_get_status(
    const tr_raft_coronet_inbound_service_t *service,
    tr_raft_coronet_inbound_status_t *out_status);

/**
 * Copies and sorts an immutable certificate identity snapshot. Multiple
 * fingerprints may map to one node during certificate rotation.
 */
int tr_raft_coronet_identity_registry_create(
    const tr_raft_coronet_identity_entry_t *entries,
    size_t entry_count,
    tr_raft_coronet_identity_registry_t **out_registry);

/** The caller must exclude concurrent resolve calls before destruction. */
void tr_raft_coronet_identity_registry_destroy(
    tr_raft_coronet_identity_registry_t *registry);

/** Compatible with tr_raft_coronet_resolve_peer_identity_fn. */
int tr_raft_coronet_identity_registry_resolve(
    void *context,
    const char *verified_certificate_sha256,
    tr_raft_node_id_t *out_peer_node_id);

/** Transfers one attached session back to the caller. */
int tr_raft_coronet_peer_manager_detach(
    tr_raft_coronet_peer_manager_t *manager,
    tr_raft_node_id_t peer_node_id,
    tr_raft_coronet_session_t **out_session);

/** Matches the runtime transport enqueue callback and routes by message.to. */
int tr_raft_coronet_peer_manager_enqueue(
    void *context,
    const tr_raft_message_t *message);

int tr_raft_coronet_peer_manager_enqueue_payload(
    tr_raft_coronet_peer_manager_t *manager,
    const tr_raft_coronet_payload_t *payload);

int tr_raft_coronet_peer_manager_get_status(
    const tr_raft_coronet_peer_manager_t *manager,
    tr_raft_coronet_peer_manager_status_t *out_status);

int tr_raft_coronet_peer_manager_get_peer_status(
    const tr_raft_coronet_peer_manager_t *manager,
    tr_raft_node_id_t peer_node_id,
    tr_raft_coronet_peer_status_t *out_status);

#ifdef __cplusplus
}
#endif

#endif
