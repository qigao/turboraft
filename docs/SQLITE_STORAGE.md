# SQLite durable storage

`TurboRaft::SQLiteStorage` implements the transaction callbacks required by
`tr_raft_runtime_t`. It is optional at build time and uses the vcpkg `sqlite3`
port through `unofficial::sqlite3::sqlite3`.

## Durability contract

- One runtime Ready is one `BEGIN IMMEDIATE` transaction.
- Hard state, log truncation, log append, and commit index become durable only
  after `commit` succeeds.
- Runtime calls `rollback` after any write or commit failure.
- The adapter configures WAL journal mode and `synchronous=FULL`.
- Open runs `PRAGMA quick_check(1)` before schema migration or recovery. A
  physical or constraint-integrity failure returns `TURBO_EIO`.
- There is no fallback to weaker synchronization or an in-memory database.

## Schema version 3

`raft_state` contains the singleton current term, vote, and commit index.
`raft_log` contains the contiguous suffix after the snapshot boundary.
`raft_snapshot` contains one application snapshot, its last included index and
term, and a separate canonical committed ConfState BLOB. Version 2 migrates to
version 3 in one transaction. Legacy snapshots retain an empty ConfState and
recover only with explicit bootstrap membership; unknown versions fail with
`TURBO_EPROTO`.

SQLite stores integers as signed 64-bit values. Terms, node IDs, indexes, and
command IDs above `INT64_MAX` are rejected with `TURBO_ERANGE`.

## Recovery

```c
tr_raft_sqlite_storage_config_t storage_config = {
    .path = "raft.db",
    .busy_timeout_ms = 5000,
    .create_if_missing = true,
    .max_snapshot_bytes = 16U * 1024U * 1024U
};
tr_raft_sqlite_storage_t *storage = NULL;
tr_raft_sqlite_recovery_t recovery;

int result = tr_raft_sqlite_storage_open(&storage_config, &storage);
if (result != TURBO_OK) {
    return result;
}
result = tr_raft_sqlite_storage_load(storage, &recovery);
if (result != TURBO_OK) {
    tr_raft_sqlite_storage_close(storage);
    return result;
}

/* Restore recovery.snapshot_data into the application state machine first. */
/* Use snapshot index/term as the core log base and applied index. */
/* If present, pass recovery.snapshot_configuration to Core. */
tr_raft_sqlite_recovery_destroy(&recovery);
tr_raft_sqlite_storage_close(storage);
```

The recovery result owns its snapshot bytes and entry array. It remains valid until
`tr_raft_sqlite_recovery_destroy()` and does not borrow SQLite pages.

## Snapshot compaction

`tr_raft_sqlite_storage_store_snapshot()` accepts only a newer committed log
position whose durable term matches the supplied term and requires the exact
committed ConfState at that index. It writes application bytes and ConfState,
then deletes log entries through that index in one transaction. Snapshot
payloads are bounded by `max_snapshot_bytes`, which must not exceed
`TR_RAFT_SQLITE_MAX_SNAPSHOT_BYTES`.

Migration is forward-only. A binary that understands only schema version 1
cannot reopen a migrated database; rollback requires a database backup or an
application-level export made before migration.

Operational backup, corruption triage, and restore procedures are defined in
[`RECOVERY.md`](RECOVERY.md). Never copy only the main database while a live WAL
may contain committed state.
