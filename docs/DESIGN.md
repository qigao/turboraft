# TurboRaft design

TurboRaft keeps consensus, persistence, transport, and administration as
separate ownership domains.

```text
application
    |
TurboRaft::Service ---- TurboRaft::WalStorage
    |
transport-neutral payloads
    +---- TurboRaft::CNet ---- caller-owned CNet poll loop
    +---- TurboRaft::FlowMQ -- caller-owned FlowMQ step loop

TurboRaft::ApplyRuntime ---- application state-machine SPI
    +---- TurboRaft::CFlowStateMachine -- dedicated CFlow Statechart

status snapshot provider ---- TurboRaft::ControlPlane ---- CHTTP/CRPC worker
```

## Consensus and storage

`TurboRaft::Core` is deterministic and contains no network, filesystem, or
threading API. `TurboRaft::Service` applies the persistence-before-send and
persistence-before-apply ordering around the core. WAL and snapshot storage use
Salts filesystem/buffer primitives and report durability failures.

Service treats `SALTS_ENOSPC` from a transport enqueue as bounded local
backpressure rather than a permanent fault. It retains only the unsent suffix
of the current Ready (at most `TR_RAFT_MAX_VOTERS` messages and
`TR_RAFT_MAX_MEMBERS` snapshot requests) and admits no new Core input until that
suffix drains. Already accepted messages are never replayed. A later owner call
returns `SALTS_ENOSPC` without consuming its input while the transport remains
full; every other transport error keeps the fail-fast Service fault behavior.

## State-machine settlement boundary

The legacy `tr_raft_runtime_process()` contract remains an atomic application
batch: its callback persists the whole application batch and applied marker,
then `tr_raft_core_advance()` confirms the complete Ready. Redis Lua adapters
that can make one batch atomic continue to use this path.

`tr_raft_apply_runtime_t` is the asynchronous alternative. `start()` copies at
most the configured `max_pending_entries` before invoking storage, transport,
or application code. After durability and reliable message handoff, it calls
`tr_raft_core_acknowledge_ready()` to transfer ownership of the copied entries
and close the Ready without changing `applied_index`. It admits one application
entry at a time and uses the log index as its nonzero settlement token.
Configuration entries remain Core-owned. A matching `APPLIED` settlement moves
the Core through exactly one contiguous index; `PENDING` retries only that same
entry; `GAP`, `CONFLICT`, `UNKNOWN`, a wrong token, or a callback error faults
the Runtime. A fault never advances the unproven suffix.

Split apply mode is permanent for one Core instance and cannot be mixed with
legacy `tr_raft_core_advance()`. Its volatile dispatched cursor starts at the
restored applied marker. After a crash, creating a Core from durable log,
commit index, and the application's durable applied marker re-emits only the
unapplied suffix.

`TurboRaft::CFlowStateMachine` adapts this SPI to a dedicated CFlow Statechart.
The application creates the adapter first, obtains V5 hooks for Statechart
initialization, binds the initialized instance, and copies the resulting SPI
into ApplyRuntime configuration. Its decoder produces a call-scoped
`cflow_event_view`; CFlow copies the Event on accepted admission. The V5 hook
copies only one fixed settlement record under a mutex and never calls Core or
blocks the SerialExecutor. A completed macrostep maps to `APPLIED`; a dropped
attempt maps to `PENDING` and retries the same token without advancing Core.
Failed or cancelled macrosteps map to `UNKNOWN` and require Core/application
recovery.

`tr_raft_apply_runtime_reconcile()` is the explicit in-process recovery
boundary for a matching terminal `GAP`, `CONFLICT`, or `UNKNOWN` settlement.
The Runtime never queries application storage. The caller must use a fresh
valid application context to compare its durable applied marker and complete
entry identity, then supply the exact WAL entry retained by the Runtime plus a
proven `APPLIED` or `PENDING` decision. Runtime compares index, term, command
ID, payload length, and payload bytes before changing state. `APPLIED`
acknowledges that index without replay; `PENDING` retries only that same entry.
Wrong identity, unresolved outcomes, callback errors, wrong settlement tokens,
and non-apply failures remain faulted and require process-level recovery.

An `APPLIED` decision also asserts that the application-owned CFlow state is at
the same durable boundary. If the failed macrostep left the live instance
unusable or unpublished, the application must destroy it, restore or rebuild a
new instance from its durable state, and rebind the adapter before reconciling.
TurboRaft does not infer that state from a database marker. Restart from the
application snapshot/checkpoint, durable applied marker, and WAL suffix remains
the backend-independent recovery path.

The SQLite/Orm conformance fixture exercises both sides of that decision using
an application-owned schema and state-machine decorator. The inner CFlow SPI
first settles a tagged macrostep and publishes its state. On the ApplyRuntime
owner thread, the decorator copies that state and exact Raft entry into its one
in-flight slot, then submits one serializable Orm transaction to a dedicated
single-worker Salts Coroutine Executor. That transaction atomically writes the
application state, exact `(index, term, command_id, payload)` journal identity,
and applied marker. ApplyRuntime sees `APPLIED` only after commit. A failure
before commit leaves both tables unchanged and surfaces `UNKNOWN`; a
deliberately lost result after commit also surfaces `UNKNOWN`. In both cases the
application reopens SQLite, checks the complete retained entry identity,
rebuilds CFlow from durable state, and explicitly reconciles `PENDING` or
`APPLIED` without guessing or replaying an already applied command.

Blocking persistence is forbidden in a CFlow Statechart host callback because
that callback runs on the SerialExecutor. The fixture's application decorator
demonstrates the production composition instead: one copied entry/state slot,
one persistence worker, a queue capacity of one, strict log-index order, and no
implicit retry after an uncertain commit. A rejected persistence submission is
terminal `UNKNOWN`. Shutdown first settles or faults ApplyRuntime, then
stops/waits/destroys the persistence executor, and only then closes the CFlow
instance. Orm remains an application/test dependency and is not part of Core or
CFlowStateMachine.

The process-crash fixture validates the same durable classification at hard
commit cut-points. A child exits without cleanup after transactional writes but
before commit; a fresh process must observe state index zero and no journal
entry. A second child exits after commit but before any ApplyRuntime
acknowledgement; a fresh process must observe the state and the complete exact
entry identity. Index-only recovery is intentionally rejected by the recovery
contract.

TurboDB::ORM exposes the transaction boundary; backend adapters supply its
implementation. SQLite is the mandatory local conformance backend here.
TidesDB and PostgreSQL are product gates only when the selected TurboDB install
was built with those backends, and PostgreSQL additionally requires an explicit
live connection. An unavailable requested backend is a configuration failure;
it is never treated as a passing or silently skipped durability test.

The bound Statechart reserves nonzero tagged external Events for Raft. Stop
Runtime admission before Statechart shutdown, drain or cancel accepted Events,
poll their terminal result, and destroy the Apply Runtime. Destruction fails
with `SALTS_EBUSY` while a non-faulted batch remains active. Then destroy the
Statechart instance, unbind the adapter, and destroy the adapter. Decoder
descriptors and callback users must remain alive through that sequence.

## Wire boundary

`raft_transport.h` owns length-prefix framing, the validated current wire
contract, monotonic message IDs, cluster/source/destination validation, and
borrowed decode callbacks. It requires a completed peer handshake and does not
own a socket or event loop.

`TurboRaft::CNet` adds `tr_raft_cnet_peer_t`, a bounded queue around a borrowed
`cnet_client`. The caller installs its observer while connecting or accepting,
polls CNet, and calls `tr_raft_cnet_peer_step()` on the same owner thread.

`TurboRaft::FlowMQ` uses one ROUTER plus one DEALER per peer. No connector
thread, mutex handoff, hidden poller, or second ingress queue exists. The
service processes bounded receive and send batches from `step()` and retains a
payload until FlowMQ copies it successfully.

## Snapshot boundary

SnapshotManager depends only on `tr_raft_transport_payload_t` and an enqueue
callback. Snapshot bytes are copied into managed Salts buffers when retained by
CNet or FlowMQ queues. Decoded bytes remain borrowed during the callback.

## Control boundary

`TurboRaft::ControlPlane` owns one CRPC server and registers `raft.status` at
`/raft/rpc`, plus `GET /raft/status`. Because CRPC owns a background CHTTP
worker, the application supplies a thread-safe status provider. When Raft is
owned by another thread, that provider must cross the boundary through a
bounded executor or mailbox; handlers never mutate Raft directly.

## Shutdown

1. Stop application producers.
2. Stop peer admission and close transport connections.
3. Continue driving CNet/FlowMQ until accepted work is terminal.
4. Stop ApplyRuntime admission, settle its accepted Event, and destroy the
   ApplyRuntime.
5. Destroy the CFlow instance, unbind and destroy its adapter.
6. Stop the CRPC/CHTTP server.
7. Destroy transport adapters, snapshot state, service, and storage.

Every capacity is explicit. Queue saturation is either retained in the bounded
Service Ready suffix or returned to the caller before consuming new input; it
is never converted into an unbounded allocation or silent drop.
