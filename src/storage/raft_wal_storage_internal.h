#ifndef TURBORAFT_RAFT_WAL_STORAGE_INTERNAL_H
#define TURBORAFT_RAFT_WAL_STORAGE_INTERNAL_H

#include <turboraft/raft_wal_storage.h>

#include <salts_fs.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct tr_raft_wal_storage_io_ops {
    void *context;
    int (*replace_durable)(
        void *context,
        const char *staging_path,
        const char *destination_path,
        salts_fs_replace_state_t *state);
} tr_raft_wal_storage_io_ops_t;

/*
 * Private deterministic test seam. This header is never installed and the
 * normal production constructor always starts with the Salts filesystem ops.
 */
int tr_raft_wal_storage_set_io_for_test(
    tr_raft_wal_storage_t *storage,
    const tr_raft_wal_storage_io_ops_t *ops);

#ifdef __cplusplus
}
#endif

#endif
