#ifndef TURBORAFT_RAFT_MULTICORE_H
#define TURBORAFT_RAFT_MULTICORE_H

#include <turboraft/raft_service.h>
#include <turboraft/raft_transport.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TR_RAFT_MULTICORE_VERSION 1U
#define TR_RAFT_MULTICORE_MAX_OWNERS 64U
#define TR_RAFT_MULTICORE_MAX_GROUPS 1024U
#define TR_RAFT_MULTICORE_MAX_CAPACITY 65536U
/* Aggregate request/completion storage limit, independent of host resources. */
#define TR_RAFT_MULTICORE_MAX_QUEUE_BYTES (UINT64_C(1024) * 1024U * 1024U)
/* Aggregate upper bound on accepted external chunk leases across all Groups.
 * A zero per-Group budget disables ingress for borrowed DATA/SNAPSHOT. */
#define TR_RAFT_MULTICORE_MAX_CHUNK_LEASE_BYTES (UINT64_C(256) * 1024U * 1024U)

typedef struct tr_raft_multicore tr_raft_multicore_t;
typedef struct tr_raft_owner tr_raft_owner_t;

typedef struct tr_raft_group_assignment {
    uint64_t group_id;
    uint32_t owner_index;
    /* Must fall within the Core's configured election interval. */
    uint32_t election_min_ticks;
    uint32_t election_max_ticks;
} tr_raft_group_assignment_t;

typedef struct tr_raft_multicore_config {
    uint32_t version;
    uint32_t owner_count;
    /* Per-group outstanding requests INCLUDING unread completions. */
    uint32_t capacity;
    /* Requests per group per scheduling round; every group receives a turn. */
    uint32_t work_budget;
    uint32_t tick_ms;
    /* Upper bound on owner polling latency when no requests are queued. */
    uint32_t idle_ms;
    const tr_raft_group_assignment_t *groups;
    size_t group_count;
    /* Required explicit byte credit for owned DATA/SNAPSHOT Group transfers.
     * Zero disables chunks; otherwise bound * group_count <= global limit.
     * Byte credit is restored when the Group Owner finishes the callback;
     * item/completion credit is reserved until take() acknowledges it. */
    size_t owned_chunk_bytes_per_group;
} tr_raft_multicore_config_t;

/**
 * Resource factory executed on each fixed owner thread, without runtime locks.
 * context is borrowed until stop returns. Different owners call concurrently.
 * owner_open/owner_poll/owner_close are either all provided or all NULL.
 * owner_poll must be bounded and nonblocking (e.g. peer_service_step).
 *
 * group_open transfers a newly created Service to the runtime on success;
 * storage/state machine/transport contexts remain owned by the factory.
 * group_close runs AFTER Service destruction. Each successful open has one
 * close, in reverse order. A failing open cleans its own partial resources and
 * leaves *out_service NULL.
 * Owner resources outlive all groups on that owner. No callback may destroy or
 * synchronously stop the runtime; request_stop is allowed after startup.
 */
/* A stream ACK is produced only by the Group Owner's installed receiver.
 * The host retrieves it via the normal completion ring and decides if/when
 * to send it from its own CNet Owner. No automatic network ACK or retry. */
typedef struct tr_raft_multicore_chunk_result {
    tr_raft_wire_payload_kind_t kind;
    bool ack_valid;
    bool durable_or_installed;
    union {
        tr_raft_snapshot_ack_t snapshot;
        tr_raft_data_ack_t data;
    } ack;
} tr_raft_multicore_chunk_result_t;

typedef struct tr_raft_multicore_factory {
    void *context;
    int (*owner_open)(void *context, tr_raft_owner_t *owner);
    int (*owner_poll)(void *context, tr_raft_owner_t *owner);
    void (*owner_close)(void *context, tr_raft_owner_t *owner);
    int (*group_open)(void *context, tr_raft_owner_t *owner,
                      uint64_t group_id, tr_raft_service_t **out_service);
    void (*group_close)(void *context, tr_raft_owner_t *owner, uint64_t group_id);
    /* Optional exact Group Owner hook for owned DATA/SNAPSHOT. This runs
     * after Group open, never on the CNet progress Owner. Implement using
     * the existing SnapshotReceiver/DataStreamReceiver as appropriate.
     * A returned ACK must reflect actual receiver/storage semantics; it
     * must NOT certify fsync just because the callback was invoked. */
    int (*receive_chunk)(void *context, tr_raft_owner_t *owner,
                         uint64_t group_id,
                         const tr_raft_transport_payload_t *chunk,
                         tr_raft_multicore_chunk_result_t *out_result);
} tr_raft_multicore_factory_t;

typedef enum tr_raft_multicore_operation {
    TR_RAFT_MULTICORE_PROPOSE,
    TR_RAFT_MULTICORE_STEP,
    TR_RAFT_MULTICORE_STATUS,
    TR_RAFT_MULTICORE_READ_INDEX,
    TR_RAFT_MULTICORE_TAKE_READ,
    TR_RAFT_MULTICORE_TRANSFER,
    TR_RAFT_MULTICORE_MEMBERSHIP,
    TR_RAFT_MULTICORE_SNAPSHOT,
    TR_RAFT_MULTICORE_OPERATION_STATUS,
    /* Reserved for the internally owned, capacity-gated ingress path only.
     * Direct tr_raft_multicore_submit() rejects this operation. */
    TR_RAFT_MULTICORE_RECEIVE_CHUNK
} tr_raft_multicore_operation_t;

/* All data is inline: submit copies the entire request before returning. */
typedef struct tr_raft_multicore_request {
    tr_raft_multicore_operation_t operation;
    /* Caller-defined correlation ID; uniqueness is the caller's responsibility. */
    uint64_t request_id;
    /* Captured by the CNet Owner after certified TLS and Directory auth.
     * Zero for ordinary/local work. This is copied through the SAME ring
     * unchanged and never references Channel or CNet runtime memory. */
    tr_raft_transport_reply_origin_t reply_origin;
    union {
        struct {
            uint64_t command_id;
            size_t size;
            uint8_t data[TR_RAFT_MAX_ENTRY_BYTES];
        } proposal;
        tr_raft_message_t message;
        uint64_t context_id;
        tr_raft_node_id_t transferee_id;
        struct {
            uint64_t transition_id;
            size_t voter_count;
            size_t learner_count;
            tr_raft_node_id_t voters[TR_RAFT_MAX_MEMBERS];
            tr_raft_node_id_t learners[TR_RAFT_MAX_MEMBERS];
        } membership;
        struct { tr_raft_term_t term; tr_raft_index_t index; } operation_status;
        struct { void *internal_owned_chunk; } chunk; /* runtime-private lease */
    } value;
} tr_raft_multicore_request_t;

typedef struct tr_raft_multicore_completion {
    uint64_t request_id;
    tr_raft_multicore_operation_t operation;
    int result;
    /* Owner completion carries the admission-time epoch even after the
     * originating Channel is closed/destroyed; host must revalidate it. */
    tr_raft_transport_reply_origin_t reply_origin;
    union {
        tr_raft_service_status_t status;
        tr_raft_operation_status_t receipt;
        tr_raft_read_state_t read;
        tr_raft_multicore_chunk_result_t chunk;
    } value;
} tr_raft_multicore_completion_t;

typedef struct tr_raft_multicore_group_status {
    size_t outstanding;
    size_t queued;
    size_t completed;
    size_t owned_chunk_bytes; /* admitted chunks awaiting Owner execution */
    uint64_t rejected;
    int background_error;
    bool stopped;
} tr_raft_multicore_group_status_t;

/* Shared validation for programmatic and file configuration. No side effects. */
int tr_raft_multicore_config_validate(const tr_raft_multicore_config_t *config);
/**
 * Copies config/assignments/factory, allocates bounded queues, starts owners,
 * and waits for all resource factories. On failure joins and rolls back every
 * successful open, leaving *out_runtime NULL. No submissions during startup.
 */
int tr_raft_multicore_create(const tr_raft_multicore_config_t *config,
                            const tr_raft_multicore_factory_t *factory,
                            tr_raft_multicore_t **out_runtime);
/**
 * Thread-safe nonblocking admission. OK means queued, NOT committed/applied.
 * ENOSPC: no credit; ENOENT: unknown group; ECANCELED: stopped/stopping.
 * A successful admission produces exactly one completion, including on stop.
 * Service receipts retain their existing durability/commit/apply semantics.
 */
int tr_raft_multicore_submit(tr_raft_multicore_t *runtime, uint64_t group_id,
                            const tr_raft_multicore_request_t *request);
/* Thread-safe competing consumers; ENOENT means no completion yet. */
int tr_raft_multicore_take(tr_raft_multicore_t *runtime, uint64_t group_id,
                          tr_raft_multicore_completion_t *out_completion);
int tr_raft_multicore_group_status(tr_raft_multicore_t *runtime, uint64_t group_id,
                                  tr_raft_multicore_group_status_t *out_status);
/* Thread-safe, idempotent, nonblocking; pending requests complete ECANCELED. */
void tr_raft_multicore_request_stop(tr_raft_multicore_t *runtime);
/**
 * Waits up to timeout_ms for all owner resource cleanup to finish. Does not
 * request stop or join threads. ETIMEDOUT leaves the runtime alive and stopping
 * (if stop was requested); it never frees resources or interrupts callbacks.
 * Zero timeout is a poll. After success, call stop/destroy to join threads.
 */
int tr_raft_multicore_wait_stopped(tr_raft_multicore_t *runtime, uint32_t timeout_ms);
/**
 * Concurrent/idempotent stop and join. EBUSY from any runtime owner callback.
 * Waits for the current callback; cannot interrupt host I/O. Completions remain
 * readable after stop. Returns the first owner background failure, or OK.
 */
int tr_raft_multicore_stop(tr_raft_multicore_t *runtime);
/* After stop and after ALL callers have quiesced. NULL is allowed. */
void tr_raft_multicore_destroy(tr_raft_multicore_t *runtime);

uint32_t tr_raft_owner_index(const tr_raft_owner_t *owner);
/* True only on this owner's thread for one of its configured groups. */
bool tr_raft_owner_contains(tr_raft_owner_t *owner, uint64_t group_id);
/**
 * Owner-thread-only borrowed Service for network/snapshot adapters. NULL for
 * wrong thread, unknown group, or a group assigned to another owner. Lifetime
 * ends before group_close; never retain across a thread or shutdown boundary.
 */
tr_raft_service_t *tr_raft_owner_service(tr_raft_owner_t *owner, uint64_t group_id);

#ifdef __cplusplus
}
#endif
#endif
