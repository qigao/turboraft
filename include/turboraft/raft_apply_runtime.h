#ifndef TURBORAFT_RAFT_APPLY_RUNTIME_H
#define TURBORAFT_RAFT_APPLY_RUNTIME_H

#include <turboraft/raft_runtime.h>

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TR_RAFT_ENTRY_STATE_MACHINE_ABI_V1 1U
#define TR_RAFT_APPLY_RUNTIME_CONFIG_ABI_V1 1U

typedef struct tr_raft_apply_runtime tr_raft_apply_runtime_t;

typedef enum tr_raft_apply_admission {
    TR_RAFT_APPLY_ADMISSION_INVALID = 0,
    TR_RAFT_APPLY_ADMISSION_ACCEPTED = 1,
    TR_RAFT_APPLY_ADMISSION_FULL = 2,
    TR_RAFT_APPLY_ADMISSION_CLOSED = 3,
    TR_RAFT_APPLY_ADMISSION_FAILED = 4
} tr_raft_apply_admission_t;

typedef enum tr_raft_apply_outcome {
    TR_RAFT_APPLY_OUTCOME_INVALID = 0,
    TR_RAFT_APPLY_OUTCOME_APPLIED = 1,
    TR_RAFT_APPLY_OUTCOME_PENDING = 2,
    TR_RAFT_APPLY_OUTCOME_GAP = 3,
    TR_RAFT_APPLY_OUTCOME_CONFLICT = 4,
    TR_RAFT_APPLY_OUTCOME_UNKNOWN = 5
} tr_raft_apply_outcome_t;

typedef struct tr_raft_apply_settlement {
    uint64_t token;
    tr_raft_apply_outcome_t outcome;
    int cause;
} tr_raft_apply_settlement_t;

/**
 * Versioned asynchronous state-machine SPI.
 *
 * try_apply() borrows entry only for the call. ACCEPTED means the state
 * machine owns an independent copy and will later expose a settlement for
 * token. FULL means no ownership transfer and authorizes an exact retry.
 * PENDING releases the prior attempt and authorizes an exact retry of the same
 * token; every other settlement is terminal. poll_settlement() is nonblocking
 * and sets out_ready=false when no record is available.
 */
typedef struct tr_raft_entry_state_machine_v1 {
    uint32_t abi_version;
    size_t struct_size;
    void *context;
    tr_raft_apply_admission_t (*try_apply)(void *context,
                                           const tr_raft_entry_t *entry,
                                           uint64_t token,
                                           int *out_cause);
    int (*poll_settlement)(void *context,
                           tr_raft_apply_settlement_t *out_settlement,
                           bool *out_ready);
} tr_raft_entry_state_machine_v1_t;

typedef struct tr_raft_apply_runtime_config_v1 {
    uint32_t abi_version;
    size_t struct_size;
    tr_raft_core_t *core;
    tr_raft_storage_t storage;
    tr_raft_transport_t transport;
    tr_raft_entry_state_machine_v1_t state_machine;
    /** Required positive hard bound; storage is allocated once by create(). */
    size_t max_pending_entries;
} tr_raft_apply_runtime_config_v1_t;

typedef enum tr_raft_apply_runtime_state {
    TR_RAFT_APPLY_RUNTIME_IDLE = 0,
    TR_RAFT_APPLY_RUNTIME_WAITING_ADMISSION = 1,
    TR_RAFT_APPLY_RUNTIME_WAITING_SETTLEMENT = 2,
    TR_RAFT_APPLY_RUNTIME_COMPLETE = 3,
    TR_RAFT_APPLY_RUNTIME_FAULTED = 4
} tr_raft_apply_runtime_state_t;

typedef struct tr_raft_apply_runtime_result {
    tr_raft_apply_runtime_state_t state;
    tr_raft_runtime_stage_t stage;
    int cause;
    int rollback_error;
    size_t messages_enqueued;
    size_t snapshots_requested;
    tr_raft_index_t applied_through;
    uint64_t in_flight_token;
    bool durable;
    bool read_state_ready;
    tr_raft_read_state_t read_state;
} tr_raft_apply_runtime_result_t;

/**
 * Creates a single-owner Runtime with fixed committed-entry storage.
 * Returns SALTS_EINVAL for an invalid ABI/configuration and SALTS_ENOMEM when
 * the one-time allocation fails. On failure, *out_runtime is NULL.
 */
int tr_raft_apply_runtime_create(
    const tr_raft_apply_runtime_config_v1_t *config,
    tr_raft_apply_runtime_t **out_runtime);

/**
 * Consumes one outstanding Ready without waiting for application settlement.
 *
 * The Runtime copies the Ready's newly committed suffix before external side
 * effects, persists and enqueues the Ready exactly once, then transfers that
 * exact suffix through tr_raft_core_ack_ready(). While an earlier entry is
 * still waiting for admission or settlement, later Ready objects may be
 * consumed and appended to the same fixed pending-entry buffer so Raft
 * storage/transport progress is not head-of-line blocked by application work.
 *
 * max_pending_entries bounds the total retained current+queued committed
 * entries across Ready objects. Applied prefixes are compacted before each new
 * Ready. Capacity exhaustion returns SALTS_ENOBUFS before storage, transport,
 * or Core acknowledgement and leaves the Ready outstanding for retry.
 */
int tr_raft_apply_runtime_start(tr_raft_apply_runtime_t *runtime,
                                const tr_raft_ready_t *ready,
                                tr_raft_apply_runtime_result_t *result);

/**
 * Polls one admission/settlement step without blocking.
 * Returns SALTS_OK for progress or a wait state; protocol/callback failures
 * fault the Runtime and are returned unchanged when nonzero.
 */
int tr_raft_apply_runtime_poll(tr_raft_apply_runtime_t *runtime,
                               tr_raft_apply_runtime_result_t *result);

/**
 * Resolves the current exact terminal apply fault from an application-owned
 * durable fact source.
 *
 * `entry` is borrowed only for this call and must match the Runtime-owned
 * entry byte-for-byte in index, term, command identity, length, and payload.
 * `outcome` must be APPLIED, proving that both application/CFlow state and its
 * durable marker include this entry, or PENDING, proving that the entry did
 * not commit and may be admitted again. Before reporting APPLIED, the caller
 * must rebuild or otherwise reconcile its app-owned CFlow instance to that
 * same durable boundary. Before either decision, the prior state-machine
 * attempt must be terminal and own no live work for the token.
 *
 * Only a matching GAP, CONFLICT, or UNKNOWN settlement creates a recoverable
 * fault. Callback, storage, transport, Core, and wrong-token failures require
 * process-level recovery. Invalid state or entry identity changes neither the
 * Runtime nor Core.
 */
int tr_raft_apply_runtime_reconcile(
    tr_raft_apply_runtime_t *runtime,
    const tr_raft_entry_t *entry,
    tr_raft_apply_outcome_t outcome,
    tr_raft_apply_runtime_result_t *result);

bool tr_raft_apply_runtime_is_faulted(
    const tr_raft_apply_runtime_t *runtime);

/**
 * Releases only Runtime-owned storage and never advances the Core.
 *
 * Returns SALTS_EBUSY while a non-faulted Runtime still owns an unsettled
 * entry or an uncompleted batch. A faulted Runtime may be released only as
 * part of process-level recovery; reconstruct the Core from its durable
 * applied prefix before resuming application.
 */
int tr_raft_apply_runtime_destroy(tr_raft_apply_runtime_t *runtime);

#ifdef __cplusplus
}
#endif

#endif
