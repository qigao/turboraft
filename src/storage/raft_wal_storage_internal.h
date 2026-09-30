#ifndef TURBORAFT_RAFT_WAL_STORAGE_INTERNAL_H
#define TURBORAFT_RAFT_WAL_STORAGE_INTERNAL_H

#include <turboraft/raft_wal_storage.h>

#include <salts_fs.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef int (*tr_raft_wal_replace_durable_fn)(
    void *context,
    const char *staging_path,
    const char *destination_path,
    salts_fs_replace_state_t *state);

/*
 * Test-only seam. Not installed and not part of the TurboRaft public ABI.
 * Passing NULL restores the production Salts provider.
 */
int tr_raft_wal_storage_set_replace_durable_for_test(
    tr_raft_wal_storage_t *storage,
    tr_raft_wal_replace_durable_fn replace_durable,
    void *context);

#ifdef __cplusplus
}
#endif

#endif
