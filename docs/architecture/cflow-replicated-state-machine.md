# CFlow/CMeta replicated application state machine

This document defines the supported asynchronous application-state-machine
contract layered on TurboRaft Core. It is the canonical closeout for issue #22
and depends on the split Ready/application acknowledgement protocol delivered
by #23 / PR #50.

## Layering

```text
Raft WAL / Core / Service
        |
        | committed entries in index order
        v
ApplyRuntime
        |
        | versioned bounded async SPI
        v
CFlowStateMachine
        |
        | CMeta-typed Event + origin_token=index
        v
CFlow V5 exact macrostep settlement
        |
        +--> optional app-owned durable materialization
             SQLite/Orm::C, Redis Lua, PostgreSQL, TidesDB,
             file store, or no database
```

TurboRaft Core remains consensus-only. It does not know SQL, ORM objects,
Redis keys, application schemas, or business-state layouts. The legacy
synchronous `tr_raft_state_machine_t::apply_batch()` API remains available
and unchanged.

## Sources of truth

There are deliberately separate facts:

- the committed Raft log is the consensus fact and replay source;
- CFlow owns deterministic application transition semantics;
- `tr_raft_core_ack_ready()` means the committed suffix has been admitted to
  an application owner and Ready storage/transport work may be released;
- `tr_raft_core_ack_applied()` means one exact contiguous log index has a
  terminal proven application outcome;
- for database-backed applications, the application transaction plus its
  durable applied marker is the materialization fact source.

Mailbox admission, CFlow execution, durable host commit, and Core applied
acknowledgement are not interchangeable.

## Ordered application protocol

For each application entry:

1. ApplyRuntime copies the committed entry into its fixed-capacity ownership
   queue before the Ready borrow ends.
2. Configuration entries remain Core-owned and are never decoded as business
   Events.
3. The application codec decodes the entry into a declared CMeta Event.
4. CFlow receives the Event with `origin_token = entry.index` through
   `cflow_statechart_instance_try_send_tagged()`.
5. Successful mailbox admission transfers a payload copy only. It does not
   advance Core.
6. CFlow reports exactly one V5 external settlement after the macrostep reaches
   a terminal outcome.
7. When durable materialization is required, an app-owned worker commits state,
   identity, optional result/outbox data, and the durable applied marker.
8. Any uncertain host result is reconciled against the durable marker and exact
   entry identity before retry or Core acknowledgement.
9. Core advances only through the greatest contiguous index proven APPLIED.

A slow application macrostep does not hold `ready_outstanding`. ApplyRuntime
may accept subsequent Ready objects until `max_pending_entries` is reached.
Capacity exhaustion returns before storage/transport or Core acknowledgement
for the later Ready.

## Type identity

CMeta descriptors are compared semantically with `cmeta_type_equal()`; code
must not depend on cross-translation-unit descriptor pointer identity.

State, Event, guard, action, and result declarations therefore remain valid
across installed-package consumers and independently compiled application
modules as long as their CMeta identities and contracts match.

## Payload ownership

Ready entries are borrowed only for the runtime call. No Event may retain a raw
pointer into `tr_raft_entry_t`.

The current Statechart mailbox supports bounded trivial Event values. A codec
must either:

- produce a self-contained bounded trivial representation, such as
  `{length, inline_bytes[N]}`; or
- use an application-owned generation-fenced bounded slot whose trivial Event
  token remains valid through settlement.

On successful admission, CFlow copies exactly the declared trivial payload
before the call returns. Decoder scratch may then be reused. Oversized commands
must fail before mailbox ownership transfer.

The `turboraft.cflow_payload_ownership` test delays the SerialExecutor,
overwrites decoder scratch immediately after admission, and proves that the
later macrostep sees the original copied generation, length, and bytes.

## Effects and durable host work

CFlow actions/guards execute only on the borrowed SerialExecutor. Blocking
database/network persistence must not run on that executor.

Database-backed fixtures use an application-owned worker:

```text
CFlow macrostep
    -> exact COMPLETED settlement
    -> persistence worker
    -> durable commit/reconcile
    -> async APPLIED settlement
    -> Core ack_applied(index)
```

This separation is demonstrated by both supported fixture families:

- SQLite/Orm::C atomically commits domain state, exact entry identity, journal
  payload, and durable applied marker;
- Redis uses the existing Lua command-journal/identity/outbox contract on a
  dedicated coroutine worker. A replay of the same exact entry is proven
  REPLAYED without duplicating the outbox; a different payload at the same
  index is a conflict and Core remains unapplied.

PostgreSQL and TidesDB remain live backend qualification gates owned by the
application/ORM layer. They are not Core dependencies.

## Outcomes and error mapping

The async SPI keeps application outcomes distinct:

- `APPLIED`: exact entry is proven materialized; Core may advance.
- `PENDING`: proven not committed; exact retry is allowed.
- `GAP`: durable marker and expected index disagree; recovery is required.
- `CONFLICT`: the same index names a different term/command/payload identity.
- `UNKNOWN`: commit state cannot be proven; never retry blindly.

CFlow settlement maps as follows:

- COMPLETED -> APPLIED;
- DROPPED -> PENDING;
- CANCELLED -> UNKNOWN with `SALTS_ECANCELED`;
- FAILED -> UNKNOWN with the preserved failure class.

Important failure classes remain visible:

- bounded mailbox/executor/effect capacity -> `SALTS_ENOBUFS`;
- executor/instance closed -> `SALTS_ESHUTDOWN`;
- cancellation -> `SALTS_ECANCELED`;
- protocol/type/configuration mismatch -> `SALTS_EPROTO`;
- guard/action/host-hook failure -> `SALTS_EIO`;
- allocation failure -> `SALTS_ENOMEM`.

Only PENDING is automatically retryable. GAP, CONFLICT, and unresolved UNKNOWN
fault normal apply progress.

## Failure and shutdown contract

The combined CFlow matrix proves:

- FULL leaves the entry waiting for admission and Core unapplied;
- CLOSED and type mismatch fail before ownership transfer;
- executor rejection, guard failure, action failure, and host-hook failure
  produce exactly one terminal settlement;
- cancel/close races settle an already accepted token exactly once;
- terminal accounting has zero pending/in-flight Events after settlement;
- adapter destroy/unbind returns busy while a live binding or accepted token is
  still owned.

Shutdown order is:

1. stop new Raft application admission;
2. settle, reconcile, or explicitly cancel the in-flight token;
3. close/cancel the CFlow instance and wait for its executor-owned work;
4. destroy the CFlow instance;
5. unbind and destroy the TurboRaft CFlow adapter;
6. shut down application persistence workers;
7. shut down the CFlow executor;
8. release application descriptors, schemas/connections, and payload storage.

Destroying an object to manufacture completion is not a valid recovery path.

## Crash and recovery evidence matrix

The required cut-points are intentionally verified at the layer that owns the
fact being tested:

| Cut-point | Required post-restart fact | Regression evidence |
| --- | --- | --- |
| Before transition/admission completes | no application state or Core applied advance | `turboraft.cflow_state_machine` cancellation-before-macrostep case |
| Accepted Event while macrostep has not committed | exactly one cancelled/failed settlement; no payload leak | `turboraft.cflow_failure_matrix` close/cancel race |
| CFlow settled, before durable host commit | durable marker remains behind; replay is PENDING | `turboraft.cflow_orm_sqlite_recovery` fail-before-commit |
| Process dies before SQLite commit | transaction/journal/state are absent after reopen | `turboraft.orm_sqlite_crash` crash-before-commit child |
| Process dies after SQLite commit | durable state/identity survive and reconcile APPLIED | `turboraft.orm_sqlite_crash` crash-after-commit child |
| Host commit succeeds but reply is lost | durable identity proves APPLIED; no blind retry | `turboraft.cflow_orm_sqlite_recovery` fail-after-commit / unknown reply |
| Redis exact entry is replayed after restart | Lua identity proves REPLAYED; outbox is not duplicated | `turboraft.cflow_redis_recovery` |
| Durable app marker wins before Core applied ack | restart resumes after durable marker | `turboraft.apply_ack` durable-marker-before-Core-ack case |
| Ready admitted but application not proven | volatile admission is forgotten and suffix replays | `turboraft.apply_ack` admitted-but-unapplied restart case |
| Restart from snapshot plus committed suffix | reconstructed typed state and applied index equal uninterrupted execution | `turboraft.cflow_snapshot_replay` |

Together these tests prove that an acknowledged Ready is not mistaken for an
applied entry, and that an application commit which wins a crash race can be
reconciled without duplicate business effects.

## Snapshot and replay

Application snapshots are opaque application-owned bytes plus an applied
boundary. TurboRaft validates the Raft-side snapshot boundary/checksum and
replays the committed suffix; it does not define an ORM dump format.

Recovery order for an asynchronous CFlow application is:

1. recover WAL/snapshot state;
2. load the latest compatible application checkpoint/durable marker;
3. recreate CMeta declarations, Statechart definition, executor, and host
   bindings;
4. create Core with the durable applied boundary;
5. replay the committed suffix in index order through ApplyRuntime;
6. reconcile exact entry identity whenever the durable store is ahead of Core;
7. resume normal admission only after the suffix reaches a contiguous proven
   boundary.

The process-local admitted cursor is never trusted across restart.

## Capacity and ordering invariants

- application FIFO follows committed log index order;
- `applied_index <= admitted_index <= commit_index`;
- an index must be admitted before it can be acknowledged applied;
- applied acknowledgement is exact and contiguous;
- later success never skips an earlier PENDING/GAP/UNKNOWN index;
- `max_pending_entries`, Event size, payload bytes, and executor/mailbox
  capacities are hard bounds;
- capacity failure never falls back to an unbounded queue.

## Package and compatibility boundary

`TurboRaft::CFlowStateMachine` is an optional exported component. Core-only
consumers do not require CFlow, ORM, Redis, or TurboDB.

Installed-package CI independently configures both Core and CFlow consumers.
The V1 asynchronous SPI is versioned. Existing synchronous
`apply_batch()` applications retain their historical behavior.

No Raft wire, WAL entry, membership, or snapshot-transfer format change is
required by this integration.

## Qualification evidence

The v2 contract was closed incrementally:

- PR #50: split Ready/application acknowledgement and restart boundaries;
- PR #51: versioned ApplyRuntime, CFlow exact settlement, multi-Ready bounded
  ownership, package contract;
- PR #52: SQLite/Orm::C durable recovery and process crash cut-points;
- PR #54: variable-length payload ownership/generation reuse;
- PR #55: failure/shutdown settlement matrix;
- PR #57: real Redis Lua async persistence/replay/conflict path.

At PR #57 exact head
`ee8269350d2bfddc5d1fb9b011a0ff018caf3f24`:

- Full TurboRaft stack acceptance `35801125437`: 58/58 Release CTest PASS;
- CFlow ORM SQLite recovery `35801125405`: PASS;
- CFlow Redis Lua recovery `35801125406`: existing Redis compatibility test
  and new async recovery test both PASS against an isolated real Redis server.
