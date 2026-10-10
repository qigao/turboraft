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

## Incomplete Snapshot reception and process death

`SnapshotReceiver` is one-owner, bounded **transfer state**, not a second WAL.
A successful non-final `SNAPSHOT_ACK` is a validated `next_offset`
**progress indication only**; it is not durable or installed. The final
full-size ACK is legal only after the receiver verifies the complete SHA-256
and the configured install/stream-commit sink successfully publishes the
snapshot through the authoritative `TurboRaft::WalStorage` transaction.
An accepted duplicate of an *already installed* final chunk retains
`installed=true` but never invokes installation again. The CNet send
boundary rejects an accepted final ACK without this installed Owner receipt.

After a process crashes during a **partial** receive, neither the in-memory
receiver cursor nor any unfinished stream sink is authoritative. A streamed
sink may leave raw snapshot **bytes** in its own temporary file after a real
SIGKILL, but that is not serialized receiver runtime state and cannot be
accepted as a manifest, a recovered offset, or a durable Snapshot. On a new
owner startup, first reopen the WAL and recover its last authoritative
committed prefix. A fresh `SnapshotReceiver` must reject a tail-only
`snapshot_offset > 0` request, even if a similarly named orphan exists.
The host must explicitly discard/quarantine the orphan according to its
storage-provider contract and choose a new transmission from offset 0;
neither the transport nor the receiver automatically retries or salvages a
partially verified transfer. Errors with **unknown WAL durability** instead
require the established fail-closed owner fault/recovery procedure, never a
speculative repeat of a possibly published commit.

Linux installed-SDK CTests
`turboraft.ace23.partial_snapshot_crash_buffered` and
`turboraft.ace23.partial_snapshot_crash_streamed` run an independent
`fork+exec` writer, terminate it with a real `SIGKILL` after exactly
12/24 bytes, then reopen WAL in the parent. The streaming fixture deliberately
`fsync`s a private orphan file to prove that its mere existence gives it no
authority. Both tests assert old commit/manifest recovery, reject tail-only
resume, explicitly replay the complete digest-valid transfer from offset 0,
publish only one Snapshot, and commit/reopen the next Raft suffix. The
streamed test calls the production `WalStorage.install_snapshot_source` path;
neither test persists the receiver's raw ownership state or adds a public
fault/retry API. These are **process-death** tests, not proof of hardware
power-loss durability or end-to-end multi-node reconnect/consensus.

### Certified CNet peer restart across a whole-process crash

The new Linux-only `turboraft.flowmq13.cnet_process_restart` installed-SDK
test uses an independent **controller** process and two `fork+exec` workers,
not the same long-lived CNet owner after a socket reset. The first worker
constructs actual Node3/Node2 mutual TLS and reciprocal Raft HELLO, dispatches
the first 12 bytes of Snapshot19 through the certified Peer Directory and
the real bounded Group103 Owner, and returns precisely one accepted but
non-durable `SNAPSHOT_ACK.next_offset=12`. It then dies from actual
`SIGKILL` with no orderly receiver, listener or WAL cleanup. The controller
rejects any exit except `SIGKILL` for this phase.

A **new OS process** is passed only the same WAL path. Its Group103 Owner
must reopen that path with `create_if_missing=false` and verify no partially
received Snapshot, committed prefix or speculative offset has become
authoritative; it must not silently create a replacement WAL. The new process
builds fresh CNet/TLS/HELLO/Group owner identities, explicitly sends the
complete matching Snapshot again from offset 0, and requires one installed,
WAL-durable final ACK. Reopening that WAL after the network lifecycle ends
must recover the full Snapshot19 and a subsequently committed index20 log
suffix. There is **no persisted receiver cursor, native CNet connection
token, host-module generation, group mailbox, or automatic application
retry**. This tests the safe **retransmit-from-zero** policy, not durable
resume from an incomplete stream or remote exactly-once delivery.

The dedicated `tests/flowmq13_installed_consumer/test_cnet_process_restart.c`
controller cannot access the workers' CNet or WAL instances. It waits for
the actual SIGKILL, requires success from the independently exec'ed recovery
worker, and requires the isolated temporary namespace to be fully reclaimed
before passing. All native transport/storage code under test is linked from
the independently **installed** exact released SDK. The process-loss test
does not simulate abrupt machine power loss, multi-node consensus, large
streaming snapshots, or upstream fully source-instrumented TSan.

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
