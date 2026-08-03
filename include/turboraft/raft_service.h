#ifndef TURBORAFT_RAFT_SERVICE_H
#define TURBORAFT_RAFT_SERVICE_H

#include <turboraft/raft_runtime.h>

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

typedef struct tr_raft_snapshot_policy {
    tr_raft_index_t applied_entry_threshold;
    size_t max_snapshot_bytes;
    tr_raft_snapshot_create_fn create;
    void *create_context;
    tr_raft_snapshot_store_fn store;
    void *store_context;
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
    bool read_state_available;
    tr_raft_runtime_result_t runtime;
    tr_raft_status_t core;
} tr_raft_service_status_t;

/* Service is single-owner; callers must serialize every operation. */
int tr_raft_service_create(
    const tr_raft_service_config_t *config,
    tr_raft_service_t **out_service);

void tr_raft_service_destroy(tr_raft_service_t *service);

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
 * Returns TURBO_ENOENT until quorum confirmation completes.
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
 * reload clears a prior Service fault; failure leaves Service faulted.
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
