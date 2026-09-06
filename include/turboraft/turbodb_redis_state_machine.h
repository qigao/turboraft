#ifndef TURBORAFT_TURBODB_REDIS_STATE_MACHINE_H
#define TURBORAFT_TURBODB_REDIS_STATE_MACHINE_H

#include <turboraft/raft_runtime.h>

#include <redis/redis_cflow.h>

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct tr_turbodb_redis_state_machine
    tr_turbodb_redis_state_machine_t;

typedef enum tr_turbodb_redis_reconcile_result {
    TR_TURBODB_REDIS_RECONCILE_REPLAYED = 0,
    TR_TURBODB_REDIS_RECONCILE_PENDING
} tr_turbodb_redis_reconcile_result_t;

typedef enum tr_turbodb_redis_compact_result {
    TR_TURBODB_REDIS_COMPACTED = 0,
    TR_TURBODB_REDIS_COMPACT_REPLAYED
} tr_turbodb_redis_compact_result_t;

typedef struct tr_turbodb_redis_state_machine_config {
    /* Borrowed and driven synchronously by the Runtime owner thread. */
    redis_cflow_connection *connection;
    redis_io_runtime *io_runtime;
    uint64_t wait_timeout_ns;
    size_t max_wait_steps;
    size_t max_batch_entries;
    const char *metadata_key;
    const char *journal_key;
    const char *identity_key;
    const char *outbox_key;
} tr_turbodb_redis_state_machine_config_t;

int tr_turbodb_redis_state_machine_open(
    const tr_turbodb_redis_state_machine_config_t *config,
    tr_turbodb_redis_state_machine_t **out_state_machine);
int tr_turbodb_redis_state_machine_close(
    tr_turbodb_redis_state_machine_t *state_machine);

/**
 * Verifies a Raft range after the caller has reconnected Redis and rebuilt the
 * exact range from WAL. PENDING authorizes one retry through the bound
 * state-machine callback; no error authorizes a retry.
 */
int tr_turbodb_redis_state_machine_reconcile_batch(
    tr_turbodb_redis_state_machine_t *state_machine,
    const tr_raft_entry_t *entries,
    size_t entry_count,
    tr_turbodb_redis_reconcile_result_t *out_result);

/**
 * Compacts the durable Redis journal through a snapshot already made durable
 * by the caller. Redis metadata is the sole source of the journal floor. A
 * subsequent call returns REPLAYED once that floor has reached the snapshot
 * index, including recovery after an unknown commit result.
 *
 * @param state_machine Open adapter driven by its owning Runtime thread.
 * @param snapshot_index Index covered by the already durable snapshot.
 * @param snapshot_term Term recorded for snapshot_index.
 * @param out_result Receives COMPACTED or COMPACT_REPLAYED on success.
 * @return SALTS_OK on a completed state transition; SALTS_EIO when Redis
 *         cannot determine a read or commit result, after which the caller
 *         may invoke this same request again; SALTS_EINVAL for invalid input.
 */
int tr_turbodb_redis_state_machine_compact_snapshot(
    tr_turbodb_redis_state_machine_t *state_machine,
    tr_raft_index_t snapshot_index, tr_raft_term_t snapshot_term,
    tr_turbodb_redis_compact_result_t *out_result);

/**
 * Adapter for tr_raft_snapshot_policy_t::journal_compact. context must be an
 * open tr_turbodb_redis_state_machine_t driven by its owning Runtime thread.
 */
int tr_turbodb_redis_state_machine_compact_snapshot_callback(
    void *context, tr_raft_index_t snapshot_index,
    tr_raft_term_t snapshot_term);

int tr_turbodb_redis_state_machine_bind(
    tr_turbodb_redis_state_machine_t *state_machine,
    tr_raft_state_machine_t *out_state_machine);

#ifdef __cplusplus
}
#endif

#endif
