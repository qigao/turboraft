# Snapshot and InstallSnapshot design

## Decision

SQLite is the local durable fact source for the current snapshot, hard state,
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
create, durable store, then in-memory Core compaction. A create or store failure
faults Service and leaves the live log prefix intact. A failure after durable
store also faults Service; authoritative recovery can reconstruct Core from the
stored snapshot. Snapshot transport remains a separate consumer and does not
own local snapshot or application state.

After compaction, a leader never constructs AppendEntries from an index at or
before its log base. Core emits a bounded snapshot request containing the peer
identity and required snapshot boundary. Runtime submits that request through
the reliable local transport boundary. Once InstallSnapshot is acknowledged,
the owner calls `tr_raft_service_snapshot_completed()`; Core advances that
peer's replication progress and resumes with the retained log suffix. A missing
snapshot transport callback fails fast when the path is first required.
Snapshot Manager implements that callback with a bounded provider buffer. It
deduplicates an active transfer for the same boundary and notifies Service once
the peer acknowledges complete installation; ordinary Raft messages and
snapshot requests use separate transport contexts.

## Recovery order

1. Open SQLite and atomically migrate schema version 1 to version 2 when needed.
2. Load the owned snapshot bytes and restore the application state machine.
3. Create Raft core with the snapshot index and term as its compacted log base.
4. Set the initial applied index to the snapshot index and provide only the
   contiguous suffix entries returned by recovery.
5. Apply committed suffix entries emitted by the first Ready.

Failure before step 2 leaves durable state unchanged. A state-machine restore
failure must stop startup; it must not fall back to an empty state machine.

## InstallSnapshot transport boundary

Wire protocol version 2 defines a versioned InstallSnapshot chunk and
acknowledgement stream. A follower will stage chunks outside the active snapshot,
verify identity, ordering, total size, and digest, then atomically install the
completed snapshot through the storage boundary. Partial transfers never become
the durable fact source. AppendEntries continues to carry only entries after the
installed snapshot boundary.

The receiver owns one bounded in-memory staging buffer and enforces exact chunk
offsets. Duplicate chunks receive the current next offset without advancing
state. A complete transfer is SHA-256 verified before the install callback.
SQLite remote installation atomically updates hard term, clears a stale vote,
advances the commit index, installs the snapshot, and reconciles the log suffix.
The suffix is preserved only when the local entry at the snapshot boundary has
the same term.

Peer service borrows the receiver and injects one snapshot callback into every
admitted session. Chunks are processed by the receiver and their acknowledgements
are enqueued into the same per-peer FIFO. A completed receiver retains only the
installed snapshot identity and next offset, not the payload bytes, so a lost
ack can be answered idempotently without reinstalling the snapshot.

The leader-side sender owns an immutable copy of one snapshot and its SHA-256
digest. Its only progress fact is the acknowledged offset. Calling
`tr_raft_snapshot_sender_next_chunk()` does not advance that offset, so repeated
calls before a valid acknowledgement produce the same chunk and an
unacknowledged chunk can be enqueued again after reconnect.

The sender validates peer identity, transfer identity, digest, and the exact
next chunk boundary before committing progress. A peer-service acknowledgement
callback can therefore enqueue the next sender chunk through the same bounded
per-peer FIFO used by Raft messages.

## Compatibility and rollback

Schema version 1 databases migrate automatically and atomically. Unknown schema
versions fail with `TURBO_EPROTO`. Migration is forward-only because version 1
binaries do not understand compacted logs; rollback requires a pre-migration
backup or application export. Wire schema version 2 rejects version 1 peers;
capability negotiation can be added before a mixed-version rolling upgrade.
