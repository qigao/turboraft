# Redis Command Journal State Machine

## Status

Proposed. This document defines the optional TurboDB-backed Redis state-machine
adapter for TurboRaft.

## Context

TurboRaft keeps consensus, durable log storage, transport, and application
state machines separate. The runtime calls a state machine with a contiguous
committed batch and only advances the Core after that callback returns success.
Therefore a state machine batch is one durable state transition from
TurboRaft's point of view.

TurboDB currently provides redis_lua_apply for an atomic single-record hash
write, metadata update, and Stream event. It is not a valid implementation of
the TurboRaft batch callback when called once per record: a failure after a
successful earlier call would persist a prefix while TurboRaft faults before it
can advance its applied index.

## Decision

Add an optional leaf adapter, TurboRaft::TurboDbRedisStateMachine. Core,
Runtime, Service, WAL, and transport targets do not depend on TurboDB.

The adapter implements a fixed command journal rather than executing arbitrary
Redis commands. For every committed Raft entry it durably records the exact
payload, records its Raft identity, advances the applied metadata, and appends
an outbox event. An application consumes the journal or the outbox through a
separate adapter; the Redis Stream never drives consensus progress.

The adapter requires a new TurboDB atomic batch primitive. It invokes one Lua
script per TurboRaft apply_batch callback. The script either commits every
entry and metadata update or commits none of them.

## Redis Model

All keys must use exactly the same non-empty Cluster hash tag:

    raft:{cluster-a}:meta
    raft:{cluster-a}:journal
    raft:{cluster-a}:identity
    raft:{cluster-a}:outbox

The metadata hash owns the durable applied position:

    applied_index     canonical decimal uint64
    applied_term      canonical decimal uint64

The journal hash maps each canonical decimal Raft index to the exact command
payload. The identity hash maps that index to a canonical representation of
term and command identity. The Stream receives one event per entry, carrying
the index, term, command identity, and payload.

No Lua numeric conversion may be used for uint64 values. The script compares,
increments, and validates canonical decimal strings so indexes above 2^53
remain exact.

## Apply Protocol

The TurboDB batch primitive accepts one contiguous sequence:

    (index, term, command_id, payload)...

The Lua script performs the following ordered state transition:

1. Validate all key tags, all canonical uint64 fields, non-empty command IDs,
   a non-empty batch, and strictly consecutive indexes.
2. Read applied_index from metadata. The first new entry must be
   applied_index + 1.
3. For a replayed range, verify the stored identity and payload byte-for-byte.
   A mismatch returns CONFLICT; a gap returns GAP.
4. For a new range, write every journal and identity field, append every Stream
   event, then update metadata to the final index and term as the sole commit
   marker.
5. Return APPLIED only after the complete transaction is durable. A fully
   identical committed range returns REPLAYED.

Redis EVAL prevents interleaving, but Redis does not roll back writes that
precede a runtime script error. The script type-checks and validates its whole
input before its first write; metadata is written last. Metadata is therefore
the sole application-state fact source. Journal records above metadata are
uncommitted preparation and Stream events are only at-least-once hints. An
unexpected Redis server error is commit-unknown and requires reconciliation.

## Failure Semantics

| Result | Adapter action |
| --- | --- |
| APPLIED or REPLAYED | Return success to Runtime. |
| GAP or CONFLICT | Return an error; Runtime enters its fault path. |
| validation or Redis error | Return an error; Runtime enters its fault path. |
| COMMIT_UNKNOWN | Return an error; never retry blindly. |

COMMIT_UNKNOWN is deliberately fail-fast. Recovery requires rebuilding
TurboRaft from WAL, reconnecting Redis, and reconciling the requested command
range against the journal and metadata with an explicit TurboDB recovery API.
Only a verified identical range may be treated as applied.

## Outbox and TurboFlow

Stream events are at-least-once notifications derived from the journal
transition. They are not an acknowledgement of Raft commit and TurboRaft does
not XACK them. A future TurboFlow connector must first verify the event index
is at or below metadata applied_index, then acknowledge after downstream
settlement. The metadata-plus-journal view is the factual source for replay
and repair.

## Snapshot and Retention

The journal and identity hashes are required to prove replay identity. They may
be compacted only after a Raft snapshot establishes a recovery point and a
documented lower-bound index is retained in Redis metadata. Compaction is a
control-plane operation while the adapter is quiescent; it must be atomic with
its lower-bound metadata update.

## Build and Deployment

The adapter is opt-in. Enabling it requires a current TurboDB installation
containing the batch header, library target, and Redis client dependency. CMake
finds TurboDB directly and fails configuration when the option is enabled but
the package is missing. No compatibility copy, fallback implementation, or DLL
staging rule is added.

The user preset supplies TURBODB_ROOT and adds TURBODB_ROOT/bin to PATH so
CTest can load the DLLs from the selected package prefix.

## Migration

The initial adapter introduces no change to the existing Core, Runtime, Service
or WAL public APIs. It is a new optional target and a new state-machine factory.
Existing callers remain unaffected unless they explicitly link and configure
the Redis adapter.

Before implementation, TurboDB must publish and install the atomic batch
primitive. The installed package inspected during this design pass exports
TurboDB::Redis but does not contain redis_lua_apply.h, so it is older than the
single-record outbox feature and cannot support this adapter yet.
