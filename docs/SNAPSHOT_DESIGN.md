# Snapshot and InstallSnapshot design

## Decision

The segmented WAL is the local durable fact source for the current snapshot, hard state,
commit index, and the log suffix. Snapshot creation and covered-prefix deletion
are one atomic transaction. The application state machine remains responsible
for producing and restoring the opaque snapshot bytes.

## Live compaction policy

Automatic local snapshots are optional and disabled by an all-zero Service
policy. An enabled policy defines an applied-entry threshold, a maximum snapshot
size, an application create callback, and a durable store callback. Service owns
one buffer bounded by that maximum for its full lifetime.

The trigger runs only after Runtime has durably processed, applied, and advanced
a Ready. Core exports a snapshot point only when `applied_index == commit_index`
and no Ready is outstanding; the point contains the exact index, term, and
ConfState for that application boundary. The operation order is application
create, durable store, optional derived-journal compaction, then in-memory Core
compaction. A create or store failure faults Service and leaves the live log
prefix intact. When the optional journal callback returns `SALTS_EIO`, Service
retains the one durable snapshot and retries its exact `(index, term)` without
recreating or storing it; other callback failures fault Service before Core
compaction. Snapshot transport remains a separate consumer and does not own
local snapshot or application state.

Online backup cannot race this sequence. `tr_raft_service_prepare_backup()`
rejects preparation while journal compaction is pending, so a caller only
closes and copies its WAL after the durable snapshot and derived journal have
reached the same boundary. The caller later supplies the adapter for the
reopened WAL through `tr_raft_service_resume_backup()`; this changes no Core
state and does not transfer WAL file ownership to Service.

After compaction, a leader never constructs AppendEntries from an index at or
before its log base. Core emits a bounded snapshot request containing the peer
identity and required snapshot boundary. Runtime submits that request through
the reliable local transport boundary. Once InstallSnapshot is acknowledged,
the owner calls `tr_raft_service_snapshot_completed()`; Core advances that
peer's replication progress and resumes with the retained log suffix. A missing
snapshot transport callback fails fast when the path is first required.
Snapshot Manager implements that callback with an owned streaming source or an
explicitly capped whole-buffer provider. The two providers are mutually
exclusive; see [the manager API](../include/turboraft/raft_snapshot_manager.h). It
deduplicates an active transfer for the same boundary and notifies Service once
the peer acknowledges complete installation; ordinary Raft messages and
snapshot requests use separate transport contexts.

## Recovery order

1. Open and strictly replay consecutive WAL segments.
2. Load the owned snapshot bytes and restore the application state machine.
3. Create Raft core with the snapshot index and term as its compacted log base.
4. Set the initial applied index to the snapshot index and provide only the
   contiguous suffix entries returned by recovery.
5. Apply committed suffix entries emitted by the first Ready.

Failure before step 2 leaves durable state unchanged. A state-machine restore
failure must stop startup; it must not fall back to an empty state machine.

## InstallSnapshot transport boundary

The [current wire protocol](WIRE_PROTOCOL.md) carries InstallSnapshot chunks and
acknowledgements. A follower stages chunks outside the active snapshot,
verify identity, ordering, total size, and digest, then atomically install the
completed snapshot through the storage boundary. Partial transfers never become
the durable fact source. AppendEntries continues to carry only entries after the
installed snapshot boundary.

The [receiver](../include/turboraft/raft_snapshot_receiver.h) accepts one bounded
active transfer and enforces exact chunk offsets. A streaming sink supplies
begin/write/commit/abort callbacks; the whole-buffer install path instead
requires an explicit `max_buffered_snapshot_bytes` cap. Duplicate chunks receive
the current next offset without advancing state. A complete transfer is
SHA-256 verified before sink commit or the buffered install callback.
WAL remote installation atomically updates hard term, clears a stale vote,
advances the commit index, installs the snapshot, and reconciles the log suffix.
The suffix is preserved only when the local entry at the snapshot boundary has
the same term.

The host routes snapshot chunks from its transport payload callback to the
receiver and enqueues acknowledgements through the bounded CNet adapter.
A completed receiver retains only the
installed snapshot identity and next offset, not the payload bytes, so a lost
ack can be answered idempotently without reinstalling the snapshot.

The [sender](../include/turboraft/raft_snapshot_sender.h) takes ownership of an
immutable source descriptor on successful `begin_source`; its small-snapshot
helper copies the whole input. Chunk size and the 1..4 in-flight window are
explicit. `next_chunk()` reserves a speculative claim and advances `next_offset`;
only validated cumulative ACKs advance `acknowledged_offset` and release slots.
With a one-chunk window, another call can return the current claim; larger full
windows return `SALTS_EBUSY`. Cancel the newest claim after synchronous enqueue
rejection, checking the result. After reconnect, `prepare_resume()` discards
speculative claims so retransmission starts at the acknowledged offset.

Chunk views borrow sender-owned slot storage. Enqueue/copy them before ACK,
cancel, resume, reset or destruction can reuse that storage. The source release
callback runs after final acknowledgement, reset or destruction. Transport
admission does not establish remote snapshot installation.

The sender validates peer identity, transfer identity, digest, and the exact
next chunk boundary before committing progress. A transport acknowledgement
callback can therefore enqueue the next sender chunk through the same bounded
per-peer FIFO used by Raft messages.

## Format safety

The WAL format is strict and does not import the removed database format.
Unknown format versions fail with `SALTS_EPROTO`. Restore requires a complete
matching WAL and snapshot set; see [Recovery](RECOVERY.md). This is a durable
data-safety requirement, not a promise of historical binary compatibility.
Peer wire admission follows its separate [current contract](WIRE_PROTOCOL.md).
