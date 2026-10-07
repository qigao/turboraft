#ifndef TURBORAFT_RAFT_WAL_STORAGE_INTERNAL_H
#define TURBORAFT_RAFT_WAL_STORAGE_INTERNAL_H

#include <turboraft/raft_wal_storage.h>

#include <cmeta_fs.h>

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef int (*tr_raft_wal_replace_durable_fn)(
    void *context,
    const char *staging_path,
    const char *destination_path,
    cmeta_fs_replace_state_t *state);

typedef enum tr_raft_wal_io_phase {
    TR_RAFT_WAL_IO_SEGMENT_WRITE = 0,
    TR_RAFT_WAL_IO_TRANSACTION_WRITE,
    TR_RAFT_WAL_IO_TRANSACTION_FSYNC,
    TR_RAFT_WAL_IO_SNAPSHOT_WRITE,
    TR_RAFT_WAL_IO_SNAPSHOT_HEADER_REWRITE,
    TR_RAFT_WAL_IO_REOPEN_OPEN,
    TR_RAFT_WAL_IO_REOPEN_TRUNCATE,
    TR_RAFT_WAL_IO_MANIFEST_WRITE,
    TR_RAFT_WAL_IO_PHASE_COUNT
} tr_raft_wal_io_phase_t;

/*
 * Private deterministic fault hook.
 *
 * The ordinal is 1-based and counted independently per phase. For write
 * phases, *inout_size begins as the requested byte count. A test provider may
 * reduce it to force a real short write before a later ordinal returns an
 * error. Returning a non-SALTS_OK status skips the underlying filesystem
 * operation and returns that status to WalStorage.
 */
typedef int (*tr_raft_wal_io_fault_fn)(
    void *context,
    tr_raft_wal_io_phase_t phase,
    uint64_t ordinal,
    size_t *inout_size);

typedef struct tr_raft_wal_io_fault_provider {
    tr_raft_wal_io_fault_fn before_io;
    void *context;
} tr_raft_wal_io_fault_provider_t;

/*
 * Test-only seam. Not installed and not part of the TurboRaft public ABI.
 * Passing NULL restores the production Salts provider.
 */
int tr_raft_wal_storage_set_replace_durable_for_test(
    tr_raft_wal_storage_t *storage,
    tr_raft_wal_replace_durable_fn replace_durable,
    void *context);

int tr_raft_wal_storage_set_io_fault_provider_for_test(
    tr_raft_wal_storage_t *storage,
    const tr_raft_wal_io_fault_provider_t *provider);

int tr_raft_wal_storage_open_with_io_fault_provider_for_test(
    const tr_raft_wal_storage_config_t *config,
    const tr_raft_wal_io_fault_provider_t *provider,
    tr_raft_wal_storage_t **out_storage);

/*
 * Test-only sequence accelerator. Requires an empty current segment and
 * durably rebases the live range to the requested monotonic sequence.
 */
int tr_raft_wal_storage_rebase_segment_sequence_for_test(
    tr_raft_wal_storage_t *storage,
    uint64_t sequence);

#ifdef __cplusplus
}
#endif

#endif
