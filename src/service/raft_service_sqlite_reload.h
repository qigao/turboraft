#ifndef TURBORAFT_RAFT_SERVICE_SQLITE_RELOAD_H
#define TURBORAFT_RAFT_SERVICE_SQLITE_RELOAD_H

#include <turboraft/raft_service.h>
#include <turboraft/raft_sqlite_storage.h>

typedef struct tr_raft_service_sqlite_reload
    tr_raft_service_sqlite_reload_t;

typedef struct tr_raft_service_sqlite_reload_config {
    /* Borrowed and must outlive the bridge. */
    tr_raft_service_t *service;
    tr_raft_sqlite_storage_t *storage;
    tr_raft_node_id_t self_id;
    const tr_raft_node_id_t *voters;
    size_t voter_count;
    const tr_raft_node_id_t *learners;
    size_t learner_count;
    uint32_t heartbeat_ticks;
    uint32_t election_min_ticks;
    uint32_t election_max_ticks;
    uint32_t initial_election_timeout_ticks;
    size_t max_log_entries;
} tr_raft_service_sqlite_reload_config_t;

int tr_raft_service_sqlite_reload_create(
    const tr_raft_service_sqlite_reload_config_t *config,
    tr_raft_service_sqlite_reload_t **out_reload);

void tr_raft_service_sqlite_reload_destroy(
    tr_raft_service_sqlite_reload_t *reload);

/* Signature-compatible with tr_raft_snapshot_reload_runtime_fn. */
int tr_raft_service_sqlite_reload_runtime(
    void *context,
    tr_raft_index_t snapshot_index,
    tr_raft_term_t snapshot_term);

#endif
