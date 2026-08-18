# Segmented WAL storage

`TurboRaft::WalStorage` is the sole durable implementation of the
`tr_raft_storage_t` transaction boundary. It stores Raft hard state, log,
commit index, snapshot metadata, snapshot configuration, and the opaque FSM
snapshot payload without depending on a database.

## Protocol

- One caller owns an instance. Calls are synchronous and must follow
  `begin -> mutations -> commit|rollback`.
- A transaction is encoded into one preallocated bounded buffer. It never
  crosses a segment and `commit` performs one sequential write followed by
  `turbo_fs_fsync`.
- Segment and transaction capacities are fixed at open. Capacity exhaustion
  returns `TURBO_ENOSPC`; there is no unbounded allocation or in-memory
  fallback.
- Every transaction frame contains its transaction id, predecessor id,
  payload length, and XXH3 checksum. Recovery requires consecutive segment and
  transaction ids.
- EOF before a complete final frame is an uncommitted torn tail and is
  truncated before the segment becomes writable. A checksum, format, sequence,
  or committed-state invariant failure rejects open/recovery.
- Any write, truncate, or fsync failure faults the instance. It cannot accept
  another transaction and must be closed.
- The lock file is held exclusively for the instance lifetime. Open, close,
  recovery, and rotation are control-plane operations and require quiescence.

Files use `<path_prefix>.NNNNNNNN.wal`; `<path_prefix>.lock` prevents two
processes from opening the same log. Snapshot payloads use
`<path_prefix>.snapshot.<index>.<term>`.

Snapshot data is written and fsynced before its referencing WAL transaction.
Recovery accepts a snapshot only when its header, index, term, size, and XXH3
checksum match the committed WAL record. A pre-existing identical snapshot is
safe to reuse after a crash before WAL commit; conflicting bytes fail fast.

When a snapshot covers the complete retained log, storage rotates first and
writes the self-contained snapshot checkpoint as the first transaction of the
new segment. Only after that transaction is fsynced are older segments removed.
Recovery may therefore begin at a segment number greater than one, but such a
segment must begin with a valid checkpoint. If an unmatched suffix remains,
the older segments stay authoritative until a later complete checkpoint can
reclaim them.

## Scope

The application owns live FSM state. It creates opaque snapshot bytes and
restores them through the snapshot callbacks. Startup restores the snapshot
first, creates Core at the snapshot index/term, then applies committed suffix
entries. A restore failure faults startup; it never falls back to an empty FSM.
