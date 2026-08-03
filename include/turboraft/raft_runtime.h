#ifndef TURBORAFT_RAFT_RUNTIME_H
#define TURBORAFT_RAFT_RUNTIME_H

#include <turboraft/raft_core.h>

#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum tr_raft_runtime_stage {
    TR_RAFT_RUNTIME_IDLE = 0,
    TR_RAFT_RUNTIME_STORAGE_BEGIN,
    TR_RAFT_RUNTIME_STORAGE_HARD_STATE,
    TR_RAFT_RUNTIME_STORAGE_TRUNCATE,
    TR_RAFT_RUNTIME_STORAGE_APPEND,
    TR_RAFT_RUNTIME_STORAGE_COMMIT_INDEX,
    TR_RAFT_RUNTIME_STORAGE_COMMIT,
    TR_RAFT_RUNTIME_TRANSPORT_SEND,
    TR_RAFT_RUNTIME_STATE_MACHINE_APPLY,
    TR_RAFT_RUNTIME_CORE_ADVANCE,
    TR_RAFT_RUNTIME_COMPLETE
} tr_raft_runtime_stage_t;

typedef struct tr_raft_storage {
    void *context;
    int (*begin)(void *context);
    int (*write_hard_state)(void *context,
                            tr_raft_term_t term,
                            tr_raft_node_id_t voted_for);
    int (*truncate_log)(void *context, tr_raft_index_t from_index);
    int (*append_log)(void *context,
                      const tr_raft_entry_t *entries,
                      size_t entry_count);
    int (*write_commit_index)(void *context, tr_raft_index_t commit_index);
    int (*commit)(void *context);
    int (*rollback)(void *context);
} tr_raft_storage_t;

typedef struct tr_raft_transport {
    void *context;
    int (*enqueue)(void *context, const tr_raft_message_t *message);
    void *snapshot_context;
    int (*enqueue_snapshot)(void *context,
                            const tr_raft_snapshot_request_t *request);
} tr_raft_transport_t;

typedef struct tr_raft_state_machine {
    void *context;
    int (*apply_batch)(void *context,
                       const tr_raft_entry_t *entries,
                       size_t entry_count);
} tr_raft_state_machine_t;

typedef struct tr_raft_runtime_config {
    tr_raft_core_t *core;
    tr_raft_storage_t storage;
    tr_raft_transport_t transport;
    tr_raft_state_machine_t state_machine;
} tr_raft_runtime_config_t;

typedef struct tr_raft_runtime_result {
    tr_raft_runtime_stage_t stage;
    int cause;
    int rollback_error;
    size_t messages_enqueued;
    size_t snapshots_requested;
    tr_raft_index_t applied_through;
    bool durable;
    bool read_state_ready;
    tr_raft_read_state_t read_state;
} tr_raft_runtime_result_t;

typedef struct tr_raft_runtime {
    tr_raft_core_t *core;
    tr_raft_storage_t storage;
    tr_raft_transport_t transport;
    tr_raft_state_machine_t state_machine;
    bool faulted;
} tr_raft_runtime_t;

int tr_raft_runtime_init(tr_raft_runtime_t *runtime,
                         const tr_raft_runtime_config_t *config);

/**
 * Consumes one outstanding Ready in durability-safe order.
 *
 * Storage callbacks implement one atomic transaction. Failed commit means no
 * writes became durable. enqueue() means accepted by a reliable local queue.
 * apply_batch() atomically persists application state and its applied index.
 * Any callback failure faults the runtime and leaves Core unadvanced.
 */
int tr_raft_runtime_process(tr_raft_runtime_t *runtime,
                            const tr_raft_ready_t *ready,
                            tr_raft_runtime_result_t *result);

bool tr_raft_runtime_is_faulted(const tr_raft_runtime_t *runtime);

#ifdef __cplusplus
}
#endif

#endif
