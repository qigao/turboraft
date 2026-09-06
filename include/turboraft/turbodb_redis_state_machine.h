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
 * exact range from WAL.
 *
 * @param state_machine Open adapter using the replacement Redis connection.
 * @param entries Exact, contiguous Raft entries retained by WAL recovery.
 * @param entry_count Number of entries, bounded by max_batch_entries.
 * @param out_result Receives REPLAYED or PENDING and must not be NULL.
 * @return SALTS_OK only when Redis verifies REPLAYED or PENDING. GAP,
 * CONFLICT, COMMIT_UNKNOWN, malformed replies, timeouts, and I/O errors are
 * returned as failures without modifying out_result.
 *
 * A PENDING result authorizes exactly one retry of the same entries through
 * the bound state-machine callback; no retry is authorized for any error.
 *
 * @code
 * tr_turbodb_redis_reconcile_result_t result;
 * if (tr_turbodb_redis_state_machine_reconcile_batch(adapter, entries, count,
 *                                                    &result) == SALTS_OK &&
 *     result == TR_TURBODB_REDIS_RECONCILE_PENDING)
 *     state_machine.apply_batch(state_machine.context, entries, count);
 * @endcode
 */
int tr_turbodb_redis_state_machine_reconcile_batch(
    tr_turbodb_redis_state_machine_t *state_machine,
    const tr_raft_entry_t *entries,
    size_t entry_count,
    tr_turbodb_redis_reconcile_result_t *out_result);
int tr_turbodb_redis_state_machine_bind(
    tr_turbodb_redis_state_machine_t *state_machine,
    tr_raft_state_machine_t *out_state_machine);

#ifdef __cplusplus
}
#endif

#endif
