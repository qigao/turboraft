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
  `salts_fs_fsync`.
- New segment files are staged and published with
  `salts_fs_replace_durable`; a segment is not admitted as the writable
  current segment until its namespace publication is durable.
- Segment and transaction capacities are fixed at open. `max_live_segments` is
  the maximum simultaneously live WAL segment count, not a lifetime sequence
  ceiling. The legacy `max_segments` field spelling remains a deprecated
  layout-preserving alias only. Segment identities are monotonic 64-bit sequence numbers and are
  never reused. Capacity exhaustion returns `SALTS_ENOSPC`; there is no
  unbounded allocation or in-memory fallback.
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

Files use `<path_prefix>.NNNNNNNN.wal` (eight digits minimum; larger
monotonic sequences extend naturally). `<path_prefix>.manifest` records the
authoritative bounded live sequence range, and `<path_prefix>.lock` prevents
two processes from opening the same log. Snapshot payloads use
`<path_prefix>.snapshot.<index>.<term>`.

The manifest is staged and published with `salts_fs_replace_durable`. For
ordinary rotation, a new segment and its transaction are durable before the
manifest extends `last_live_sequence`; the operation is not acknowledged
until that manifest update succeeds. For a complete checkpoint, the checkpoint
transaction is durable before the manifest atomically advances both
`first_live_sequence` and `last_live_sequence` to the checkpoint segment;
only then are older segments unlinked. A crash therefore observes either the
old authoritative history or the complete new checkpoint, never a range that
references already-reclaimed history.

Pre-manifest WALs are supported by one bounded compatibility scan from
sequence 1 through the configured `max_live_segments`. A successful open writes
the durable manifest immediately. All subsequent recovery scans only the
manifest's bounded live range, so startup work is independent of lifetime
segment sequence.

Snapshot data is written to a staging file and published with
`salts_fs_replace_durable` before its referencing WAL transaction. A
pre-publication failure leaves the previous authoritative path untouched; a
post-replacement durability failure is surfaced as an error and faults the
owner instead of being treated as a successful snapshot. Recovery accepts a
snapshot only when its header, index, term, size, and XXH3
checksum match the committed WAL record. A pre-existing identical snapshot is
safe to reuse after a crash before WAL commit; conflicting bytes fail fast.

When a snapshot covers the complete retained log, storage rotates first and
writes the self-contained snapshot checkpoint as the first transaction of the
new segment. Only after that transaction is fsynced are older segments removed.
Recovery may therefore begin at a segment number greater than one—even a
sequence far beyond `max_live_segments`—but such a segment must begin with a valid
checkpoint. If an unmatched suffix remains,
the older segments stay authoritative until a later complete checkpoint can
reclaim them.

## Deterministic durability fault boundary

Durability fault injection is test-only and remains outside the installed
TurboRaft ABI. Tests under `src/storage` may bind a private per-storage I/O
provider before mutation or open/recovery. The provider receives a 1-based
ordinal counted independently for each phase:

- segment write;
- transaction write;
- transaction fsync;
- snapshot staging write;
- snapshot header rewrite;
- manifest staging write;
- writable segment reopen;
- torn-tail recovery truncate.

For write phases the provider may reduce the requested byte count to force a
real short write, or return an explicit status such as `SALTS_ENOSPC` or
`SALTS_EIO` before the filesystem operation. Production instances use the
normal Salts filesystem path and do not carry a public fault-control surface.

Namespace publication has a separate private provider because its contract is
stronger than a single filesystem call. Snapshot and new-segment publication
use `salts_fs_replace_durable`, whose result distinguishes not-published,
published-and-durable, and durability-unknown outcomes. The phased I/O
provider tests writes/fsync/reopen/truncate, including snapshot staging writes
and the final snapshot-header rewrite. The durable-replace provider models the
two caller-visible fsync boundaries explicitly:

- staging-file fsync failure -> `NOT_PUBLISHED`, so the previous authoritative
  path is untouched and retry remains safe;
- post-rename parent-directory fsync failure -> `DURABILITY_UNKNOWN`, so the
  live owner faults and reopen/recovery decides the authoritative state.

The actual internal fsync ordinals of `salts_fs_replace_durable` are owned and
fault-tested by Salts itself. TurboRaft tests the resulting ownership states
instead of duplicating the filesystem primitive or adding an extra production
fsync. These providers are complementary and must not be collapsed into a
retry fallback.

Service preserves the same durability barrier. Core may hold a provisional
commit in an outstanding Ready while persistence is attempted, but Runtime
does not acknowledge that Ready or invoke the FSM until storage commit
succeeds. If WAL write/fsync fails, Service faults, `applied_index` stays at
the last durable value, and the failed Ready remains unacknowledged. Normal
reopen/recovery is authoritative: it exposes only the last proven durable
prefix, or a fully published state allowed by the filesystem durability
contract.

## Scope

The application owns live FSM state. It creates opaque snapshot bytes and
restores them through the snapshot callbacks. Startup restores the snapshot
first, creates Core at the snapshot index/term, then applies committed suffix
entries. A restore failure faults startup; it never falls back to an empty FSM.
