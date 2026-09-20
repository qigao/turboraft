#ifndef TURBORAFT_RAFT_SERVICE_H
#define TURBORAFT_RAFT_SERVICE_H

#include <turboraft/raft_runtime.h>
#include <turboraft/raft_snapshot_stream.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct tr_raft_service tr_raft_service_t;

typedef int (*tr_raft_snapshot_create_fn)(
    void *context,
    tr_raft_index_t applied_index,
    uint8_t *buffer,
    size_t capacity,
    size_t *out_size);

typedef int (*tr_raft_snapshot_store_fn)(
    void *context,
    tr_raft_index_t snapshot_index,
    tr_raft_term_t snapshot_term,
    const tr_raft_conf_t *configuration,
    const uint8_t *data,
    size_t size);

typedef int (*tr_raft_snapshot_source_create_fn)(
    void *context,
    tr_raft_index_t applied_index,
    tr_raft_snapshot_source_t *out_source);

typedef int (*tr_raft_snapshot_source_store_fn)(
    void *context,
    tr_raft_index_t snapshot_index,
    tr_raft_term_t snapshot_term,
    const tr_raft_conf_t *configuration,
    const tr_raft_snapshot_source_t *source);

/**
 * Compacts a journal derived from a snapshot after store has made that
 * snapshot durable. SALTS_EIO keeps the snapshot pending for a retry of this
 * exact index and term; every other failure faults the Service without
 * compacting Core.
 */
typedef int (*tr_raft_snapshot_journal_compact_fn)(
    void *context,
    tr_raft_index_t snapshot_index,
    tr_raft_term_t snapshot_term);

typedef struct tr_raft_snapshot_policy {
    tr_raft_index_t applied_entry_threshold;
    /** Logical snapshot limit; does not imply an in-memory allocation. */
    uint64_t max_snapshot_bytes;
    /**
     * Buffered compatibility cap. Required and non-zero only when create/store
     * are used; must be zero for source_create/source_store.
     */
    uint64_t max_buffered_snapshot_bytes;

    /**
     * Database-scale path. source_create returns an owned immutable source.
     * Service releases it exactly once after source_store returns, regardless
     * of success or failure. source_store must make the snapshot durable before
     * returning SALTS_OK and must not retain the source after return.
     */
    tr_raft_snapshot_source_create_fn source_create;
    void *source_create_context;
    tr_raft_snapshot_source_store_fn source_store;
    void *source_store_context;

    /** Small-snapshot compatibility path; mutually exclusive above. */
    tr_raft_snapshot_create_fn create;
    void *create_context;
    tr_raft_snapshot_store_fn store;
    void *store_context;

    /* Optional derived-journal compaction; context is borrowed by Service. */
    tr_raft_snapshot_journal_compact_fn journal_compact;
    void *journal_compact_context;
} tr_raft_snapshot_policy_t;

typedef struct tr_raft_service_config {
    tr_raft_core_config_t core;
    tr_raft_storage_t storage;
    tr_raft_transport_t transport;
    tr_raft_state_machine_t state_machine;
    /* All-zero disables automatic local snapshots and compaction. */
    tr_raft_snapshot_policy_t snapshot_policy;
} tr_raft_service_config_t;

typedef struct tr_raft_service_status {
    bool faulted;
    int cause;
    bool backup_prepared;
    bool read_state_available;
    size_t completed_read_count;
    size_t max_completed_reads;
    bool journal_compaction_pending;
    tr_raft_runtime_result_t runtime;
    tr_raft_status_t core;
} tr_raft_service_status_t;

/*
 * Service is single-owner; callers must serialize every operation.
 *
 * When transport enqueue first returns SALTS_ENOSPC, Service retains the
 * unsent suffix of that Ready in bounded local storage and returns success for
 * the accepted operation. Before consuming a later operation it retries that
 * suffix; if the transport is still full, the later operation returns
 * SALTS_ENOSPC without being consumed. Other transport errors fault Service.
 */
int tr_raft_service_create(
    const tr_raft_service_config_t *config,
    tr_raft_service_t **out_service);

void tr_raft_service_destroy(tr_raft_service_t *service);

/*
 * Freezes Service after confirming there is no outstanding Ready, unread read
 * state, or pending journal compaction. The caller then owns closing and
 * copying its WAL/snapshot files; mutation APIs return SALTS_EBUSY until
 * tr_raft_service_resume_backup() succeeds.
 */
int tr_raft_service_prepare_backup(tr_raft_service_t *service);

/*
 * Binds the storage adapter for the reopened WAL after prepare_backup().
 * Service borrows the adapter callbacks and context exactly as at creation.
 */
int tr_raft_service_resume_backup(tr_raft_service_t *service,
                                  const tr_raft_storage_t *storage);

int tr_raft_service_tick(
    tr_raft_service_t *service,
    const tr_raft_tick_t *tick);

int tr_raft_service_step(
    tr_raft_service_t *service,
    const tr_raft_message_t *message);

int tr_raft_service_propose(
    tr_raft_service_t *service,
    const tr_raft_proposal_t *proposal);

int tr_raft_service_propose_with_receipt(
    tr_raft_service_t *service,
    const tr_raft_proposal_t *proposal,
    tr_raft_operation_status_t *out_receipt);

/* Starts leader transfer through the same durability/transport owner path. */
int tr_raft_service_transfer_leadership(
    tr_raft_service_t *service,
    tr_raft_node_id_t transferee_id);

/* Starts one leader-only Joint Consensus transition. */
int tr_raft_service_change_membership(
    tr_raft_service_t *service,
    const tr_raft_membership_change_t *change);

int tr_raft_service_change_membership_with_receipt(
    tr_raft_service_t *service,
    const tr_raft_membership_change_t *change,
    tr_raft_operation_status_t *out_receipt);

/* Starts one quorum-confirmed linearizable read barrier. */
int tr_raft_service_read_index(tr_raft_service_t *service,
                               uint64_t context_id);

/*
 * Takes the completed read state after enforcing applied_index >= read index.
 * Returns SALTS_ENOENT until quorum confirmation completes.
 */
int tr_raft_service_take_read_state(tr_raft_service_t *service,
                                    tr_raft_read_state_t *out_read_state);

/* Processes committed-but-unapplied entries restored during startup. */
int tr_raft_service_poll(tr_raft_service_t *service);

/* Forces one local application snapshot through create/store/compact. */
int tr_raft_service_trigger_snapshot(tr_raft_service_t *service);

/* Resumes replication after the peer acknowledges InstallSnapshot. */
int tr_raft_service_snapshot_completed(tr_raft_service_t *service,
                                       tr_raft_node_id_t peer_id,
                                       tr_raft_index_t snapshot_index);

/* Callback adapter for Snapshot Manager completion notification. */
int tr_raft_service_snapshot_complete_callback(void *context,
                                               tr_raft_node_id_t peer_id,
                                               tr_raft_index_t snapshot_index);

/*
 * Replaces Core from an authoritative recovery configuration. A successful
 * reload clears a prior Service fault and discards any transport suffix from
 * the replaced Core; failure leaves Service faulted.
 */
int tr_raft_service_reload(
    tr_raft_service_t *service,
    const tr_raft_core_config_t *recovery_config);

int tr_raft_service_status(
    const tr_raft_service_t *service,
    tr_raft_service_status_t *out_status);

int tr_raft_service_configuration(
    const tr_raft_service_t *service,
    tr_raft_conf_t *out_configuration);

int tr_raft_service_progress(
    const tr_raft_service_t *service,
    tr_raft_progress_view_t *out_progress);

int tr_raft_service_operation_status(
    const tr_raft_service_t *service,
    tr_raft_term_t term,
    tr_raft_index_t index,
    tr_raft_operation_status_t *out_status);

#ifdef __cplusplus
}
#endif

#endif
