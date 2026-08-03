#ifndef TURBORAFT_RAFT_SERVICE_OWNER_H
#define TURBORAFT_RAFT_SERVICE_OWNER_H

#include <turboraft/raft_service.h>

#include <turbo_coro_context.h>

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TR_RAFT_SERVICE_OWNER_DEFAULT_MAX_PENDING 256U
#define TR_RAFT_SERVICE_OWNER_MAX_PENDING 65536U
#define TR_RAFT_SERVICE_OWNER_DEFAULT_MAX_COMMAND_BYTES (64U * 1024U)
#define TR_RAFT_SERVICE_OWNER_MAX_COMMAND_BYTES (1024U * 1024U)

typedef struct tr_raft_service_owner tr_raft_service_owner_t;

typedef int (*tr_raft_service_owner_timeout_fn)(void *context,
                                                uint32_t *election_timeout);

typedef int (*tr_raft_service_owner_command_fn)(tr_raft_service_t *service,
                                                const void *payload,
                                                size_t payload_size);

typedef void (*tr_raft_service_owner_complete_fn)(void *context, int result);

typedef struct tr_raft_service_owner_config {
    tr_raft_service_t *service;
    coro_context_t *context;
    uint64_t tick_interval_ms;
    uint32_t elapsed_ticks;
    size_t max_pending_commands;
    size_t max_command_bytes;
    tr_raft_service_owner_timeout_fn next_election_timeout;
    void *timeout_context;
} tr_raft_service_owner_config_t;

typedef struct tr_raft_service_owner_status {
    bool started;
    bool running;
    bool stopping;
    bool faulted;
    int cause;
    size_t pending_commands;
    uint64_t completed_commands;
    uint64_t tick_count;
} tr_raft_service_owner_status_t;

/**
 * Creates a single-consumer owner for a borrowed service and CoroNet context.
 * The caller must keep both dependencies alive until close succeeds.
 */
int tr_raft_service_owner_create(const tr_raft_service_owner_config_t *config,
                                 tr_raft_service_owner_t **owner);

/** Starts the periodic tick coroutine. An owner is intentionally one-shot. */
int tr_raft_service_owner_start(tr_raft_service_owner_t *owner);

/**
 * Copies payload and posts one command to the owner context.
 * This function is thread-safe. A successful submission invokes completion
 * exactly once on the owner context. Queue saturation returns TURBO_EBUSY.
 */
int tr_raft_service_owner_submit(
    tr_raft_service_owner_t *owner,
    tr_raft_service_owner_command_fn command,
    const void *payload,
    size_t payload_size,
    tr_raft_service_owner_complete_fn completion,
    void *completion_context);

/** Returns the immutable borrowed dependencies configured for this owner. */
tr_raft_service_t *tr_raft_service_owner_service(
    const tr_raft_service_owner_t *owner);
coro_context_t *tr_raft_service_owner_context(
    const tr_raft_service_owner_t *owner);

/** Executes a borrowed command synchronously only from the owner context. */
int tr_raft_service_owner_execute_current(
    tr_raft_service_owner_t *owner,
    tr_raft_service_owner_command_fn command,
    const void *payload,
    size_t payload_size);

/** Rejects new submissions and lets accepted commands drain. Thread-safe. */
int tr_raft_service_owner_stop(tr_raft_service_owner_t *owner);

int tr_raft_service_owner_status(const tr_raft_service_owner_t *owner,
                                 tr_raft_service_owner_status_t *status);

/**
 * Releases the owner after its producers are quiescent and drain is complete.
 * This lifecycle operation must not race with submit. TURBO_EBUSY means the
 * event loop must continue until running is false and pending reaches zero.
 */
int tr_raft_service_owner_close(tr_raft_service_owner_t *owner);

#ifdef __cplusplus
}
#endif

#endif
