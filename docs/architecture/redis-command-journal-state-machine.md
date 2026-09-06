# Redis Command Journal State Machine

## Status

Implemented. This document defines the optional TurboDB-backed Redis
state-machine adapter for TurboRaft.

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

The adapter invokes one TurboDB Lua batch script per TurboRaft apply_batch
callback. It validates all inputs before the first write, prepares journal and
outbox data, then advances metadata as its sole commit marker.

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

COMMIT_UNKNOWN is deliberately fail-fast. The Runtime faults and leaves Core
unadvanced. Its owner must replace the Redis connection, rebuild the exact
unconfirmed range from WAL, and call
`tr_turbodb_redis_state_machine_reconcile_batch` before applying that range
again. The explicit API returns one of two successful verification results:

| Recovery result | Owner action |
| --- | --- |
| REPLAYED | Treat the exact range as committed; do not apply it again. |
| PENDING | Apply the identical recovered entries exactly once. |

GAP, CONFLICT, COMMIT_UNKNOWN, malformed replies, timeouts, and I/O errors
remain failures. The adapter never reconnects, reconstructs a WAL range,
retries a batch, or advances Raft progress by itself.

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

When local snapshot policy configures `journal_compact`, Service owns the order:

1. create the application snapshot;
2. make that exact index, term, configuration and payload durable through
   `store`;
3. invoke `tr_turbodb_redis_state_machine_compact_snapshot_callback` with the
   durable index and term; and
4. compact the in-memory Core only after that callback completes.

The durable WAL snapshot remains the recovery fact source; Redis metadata's
`journal_floor` is the sole fact source for the derived journal retention
boundary. The callback never trims the outbox Stream. If it returns
`SALTS_EIO`, Service retains exactly one pending snapshot and later
`poll`, `tick`, or `step` retries only that same `(index, term)` compaction.
This covers an unknown Redis reply without creating or storing a second
snapshot. Any other callback failure faults Service and leaves Core's live log
prefix untrimmed.

## Build and Deployment

The adapter is opt-in. When enabled, the installed TurboRaft package exports
`TurboRaft::TurboDbRedisStateMachine`. A consumer requests it explicitly:

    find_package(TurboRaft CONFIG REQUIRED COMPONENTS TurboDbRedisStateMachine)
    target_link_libraries(app PRIVATE TurboRaft::TurboDbRedisStateMachine)

That component requires a current TurboDB installation containing the batch
header, library target, and Redis client dependency. The package config finds
TurboDB only through `TURBODB_ROOT` and fails configuration if that root is
missing or invalid. Consumers that request only Core do not need `TURBODB_ROOT`.
No compatibility copy, fallback implementation, or DLL staging rule is added.

The user preset supplies TURBODB_ROOT and adds TURBODB_ROOT/bin to PATH so
CTest can load the DLLs from the selected package prefix.

## Migration

The adapter introduces no change to Core, Runtime, WAL, wire, or transport
public APIs. `tr_raft_snapshot_policy_t` adds an optional journal-compaction
callback; zero-initialized and existing policies retain their previous behavior.
Existing callers remain unaffected unless they explicitly link and configure
the Redis adapter and that callback.

TurboDB must provide the installed atomic batch and reconciliation headers.
The selected user preset supplies a current package, and CMake fails fast if
that package is absent or does not export the required target.
