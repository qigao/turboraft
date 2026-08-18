#ifndef TURBORAFT_RAFT_WAL_STORAGE_H
#define TURBORAFT_RAFT_WAL_STORAGE_H

#include <turboraft/raft_runtime.h>

#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TR_RAFT_WAL_MIN_SEGMENT_BYTES (64U * 1024U)
#define TR_RAFT_WAL_DEFAULT_SEGMENT_BYTES (64U * 1024U * 1024U)
#define TR_RAFT_WAL_DEFAULT_TRANSACTION_BYTES (256U * 1024U)
#define TR_RAFT_WAL_MAX_SEGMENTS 65535U
#define TR_RAFT_WAL_MAX_SNAPSHOT_BYTES (256U * 1024U * 1024U)

typedef struct tr_raft_wal_storage tr_raft_wal_storage_t;

typedef struct tr_raft_wal_storage_config {
    /* Segment files are path_prefix.NNNNNNNN.wal; path_prefix.lock is held. */
    const char *path_prefix;
    size_t segment_bytes;
    size_t max_transaction_bytes;
    size_t max_segments;
    size_t max_log_entries;
    size_t max_snapshot_bytes;
    bool create_if_missing;
} tr_raft_wal_storage_config_t;

typedef struct tr_raft_wal_recovery {
    tr_raft_term_t term;
    tr_raft_node_id_t voted_for;
    tr_raft_index_t commit_index;
    tr_raft_index_t snapshot_index;
    tr_raft_term_t snapshot_term;
    bool has_snapshot_configuration;
    tr_raft_conf_t snapshot_configuration;
    uint8_t *snapshot_data;
    size_t snapshot_size;
    tr_raft_entry_t *entries;
    size_t entry_count;
} tr_raft_wal_recovery_t;

int tr_raft_wal_storage_open(
    const tr_raft_wal_storage_config_t *config,
    tr_raft_wal_storage_t **out_storage);
int tr_raft_wal_storage_close(tr_raft_wal_storage_t *storage);
int tr_raft_wal_storage_bind(tr_raft_wal_storage_t *storage,
                             tr_raft_storage_t *out_storage);
int tr_raft_wal_storage_load(tr_raft_wal_storage_t *storage,
                             tr_raft_wal_recovery_t *out_recovery);
int tr_raft_wal_storage_store_snapshot(
    tr_raft_wal_storage_t *storage,
    tr_raft_index_t last_included_index,
    tr_raft_term_t last_included_term,
    const tr_raft_conf_t *configuration,
    const void *data,
    size_t size);
int tr_raft_wal_storage_install_snapshot(
    tr_raft_wal_storage_t *storage,
    tr_raft_term_t leader_term,
    tr_raft_index_t last_included_index,
    tr_raft_term_t last_included_term,
    const tr_raft_conf_t *configuration,
    const void *data,
    size_t size);
void tr_raft_wal_recovery_destroy(tr_raft_wal_recovery_t *recovery);

#ifdef __cplusplus
}
#endif

#endif
