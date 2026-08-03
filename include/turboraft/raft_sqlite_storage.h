#ifndef TURBORAFT_RAFT_SQLITE_STORAGE_H
#define TURBORAFT_RAFT_SQLITE_STORAGE_H

#include <turboraft/raft_runtime.h>

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct tr_raft_sqlite_storage tr_raft_sqlite_storage_t;

typedef struct tr_raft_sqlite_storage_config {
    const char *path;
    int busy_timeout_ms;
    bool create_if_missing;
    size_t max_snapshot_bytes;
} tr_raft_sqlite_storage_config_t;

typedef struct tr_raft_sqlite_recovery {
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
} tr_raft_sqlite_recovery_t;

#define TR_RAFT_SQLITE_MAX_SNAPSHOT_BYTES (64U * 1024U * 1024U)

/** Open one SQLite database and validate, migrate, or create schema version 3. */
int tr_raft_sqlite_storage_open(
    const tr_raft_sqlite_storage_config_t *config,
    tr_raft_sqlite_storage_t **out_storage);

/** Roll back an active transaction, close the database, and release storage. */
int tr_raft_sqlite_storage_close(tr_raft_sqlite_storage_t *storage);

/** Bind this database to the transaction callbacks consumed by Raft runtime. */
int tr_raft_sqlite_storage_bind(tr_raft_sqlite_storage_t *storage,
                                tr_raft_storage_t *out_storage);

/** Load an owned, immutable recovery snapshot from the durable database. */
int tr_raft_sqlite_storage_load(
    tr_raft_sqlite_storage_t *storage,
    tr_raft_sqlite_recovery_t *out_recovery);

/** Atomically persist a committed snapshot and remove its covered log prefix. */
int tr_raft_sqlite_storage_store_snapshot(
    tr_raft_sqlite_storage_t *storage,
    tr_raft_index_t last_included_index,
    tr_raft_term_t last_included_term,
    const tr_raft_conf_t *configuration,
    const void *data,
    size_t size);

/** Atomically installs a verified remote snapshot and reconciles the log. */
int tr_raft_sqlite_storage_install_snapshot(
    tr_raft_sqlite_storage_t *storage,
    tr_raft_term_t leader_term,
    tr_raft_index_t last_included_index,
    tr_raft_term_t last_included_term,
    const tr_raft_conf_t *configuration,
    const void *data,
    size_t size);

/** Release snapshot bytes and entries owned by recovery, then clear all fields. */
void tr_raft_sqlite_recovery_destroy(
    tr_raft_sqlite_recovery_t *recovery);

/** Return the latest extended SQLite result code recorded by this adapter. */
int tr_raft_sqlite_storage_last_sqlite_code(
    const tr_raft_sqlite_storage_t *storage);

/** Borrow the latest diagnostic until the next storage operation or close. */
const char *tr_raft_sqlite_storage_last_error(
    const tr_raft_sqlite_storage_t *storage);

#ifdef __cplusplus
}
#endif

#endif
