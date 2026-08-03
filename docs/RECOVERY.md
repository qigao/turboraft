# Operator recovery and backup

This runbook covers the TurboRaft SQLite durable store. Application snapshot
backup and restore must additionally follow the embedding application's data
contract.

## Safety rules

- Isolate and stop the node before file-level inspection or replacement.
- Preserve the original database, `-wal`, and `-shm` files as one evidence set.
- Never delete, rename, swap, or copy only a live WAL file.
- Never edit `PRAGMA user_version`, Raft term, vote, commit index, or log rows to
  force startup.
- Restore to a new path first. Do not overwrite the only damaged or backup copy.
- A restored node must rejoin through the normal authenticated peer and
  membership path; file restore does not grant membership.

SQLite documents that a hot journal or WAL is part of the database state and
must remain paired with the database during recovery. See [SQLite: How To
Corrupt An SQLite Database File](https://www.sqlite.org/howtocorrupt.html).

## Consistent backup

Preferred online methods are the SQLite backup API or `VACUUM INTO`; both create
a transactionally consistent database image while the source is live. For
example, with the SQLite command-line tool:

```powershell
sqlite3.exe raft.db "VACUUM INTO 'raft-backup-20260802.db';"
sqlite3.exe raft-backup-20260802.db "PRAGMA quick_check;"
```

The destination must not already exist. Record the TurboRaft binary version,
SQLite schema version, node ID, cluster ID, and backup timestamp next to the
backup. Protect the backup with the same access controls as the live database.

A file copy is acceptable only after the node is stopped and no process holds
the database. If a `-wal` file remains, copy the database, `-wal`, and `-shm`
files together. The SQLite backup API and `VACUUM INTO` are preferred because
they avoid this pairing risk.

SQLite documents the supported live-copy methods in [SQLite backup and
corruption guidance](https://www.sqlite.org/howtocorrupt.html#_backup_or_restore_while_a_transaction_is_active).

## Startup failure triage

1. Stop and isolate the node. Do not retry writes against an uncertain store.
2. Preserve the database/WAL/SHM evidence set and logs.
3. Run `PRAGMA quick_check;` against a copy, not the only original.
4. Classify the TurboRaft result before choosing a recovery action.

| Result | Meaning | Action |
| --- | --- | --- |
| `TURBO_EBUSY` | Another connection or transaction owns the storage boundary | Find the owner; do not delete lock or WAL files |
| `TURBO_EIO` | SQLite I/O or quick-check integrity failure | Restore a verified backup or rebuild this node from a healthy cluster member |
| `TURBO_EPROTO` | Unsupported schema or invalid Raft recovery invariant | Use a compatible binary or restore a pre-migration backup; do not rewrite metadata |
| `TURBO_ERANGE` | Durable integer or payload exceeds the supported boundary | Treat as incompatible/corrupt input and preserve evidence |
| `TURBO_ENOMEM` / `TURBO_ENOSPC` | Host resource failure | Correct the resource condition before retrying the unchanged store |

`tr_raft_sqlite_storage_open()` performs quick-check and schema validation.
`tr_raft_sqlite_storage_load()` then validates snapshot pairing, configuration
encoding, log continuity, commit bounds, and payload limits. Neither path
repairs data or falls back to an empty database.

## Restore from backup

1. Stop and isolate the node.
2. Verify the selected backup with `PRAGMA quick_check;` and confirm its schema
   version is supported by the binary that will open it.
3. Place the backup at a new path with restrictive permissions. Do not carry
   unrelated old `-wal` or `-shm` files to the new path.
4. Configure the node to open the new path with `create_if_missing=false`.
5. Start the node and verify term, leader, commit, applied, and snapshot status
   through diagnostics before allowing client traffic.
6. Let normal Raft replication catch the node up. If retained log history is
   insufficient, the current cluster must install a snapshot.

Restoring a backup can move this node behind the cluster. It must never be used
to replace a quorum of nodes with the same stale image. Quorum-wide disaster
recovery requires an application-specific, externally reviewed procedure.

## Snapshot restore failure

Remote snapshot installation first commits durable SQLite state, then restores
the application state machine, then reloads the Raft runtime. If application
restore fails, the node remains faulted with the durable snapshot installed.
Fix the application restore cause and restart from the same database. Do not
roll the database back independently of the application state.

## Upgrade rollback

Schema migration is forward-only. Before an upgrade, retain a verified backup
made by the old binary. If rollback is required after schema migration, stop the
node and restore that backup to a new path. Wire rollback has an additional
constraint: a legacy peer cannot receive v4 snapshots, so retain enough log
history for the rollback window or upgrade the lagging peer again.

See [`UPGRADES.md`](UPGRADES.md) for the complete compatibility matrix.
