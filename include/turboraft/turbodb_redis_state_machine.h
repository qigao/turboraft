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
int tr_turbodb_redis_state_machine_bind(
    tr_turbodb_redis_state_machine_t *state_machine,
    tr_raft_state_machine_t *out_state_machine);

#ifdef __cplusplus
}
#endif

#endif
