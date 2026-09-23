# CFlow replicated state-machine v2

Issue: #22
Depends on: #23 / PR #50

> Canonical architecture and closeout evidence now live in
> `docs/architecture/cflow-replicated-state-machine.md`. This file preserves
> the implementation-slice design context.

## Boundary

TurboRaft Core remains consensus-only. CFlow/CMeta support is an optional
integration layer:

```text
Core committed entries
        |
        v
ApplyRuntime (bounded ownership queue)
        |
        v
CFlowStateMachine adapter
        |
        v
CFlow V5 tagged external Event settlement
        |
        +--> app-owned durable host transaction / effects
```

The legacy synchronous `tr_raft_state_machine_t::apply_batch()` path is not
changed.

## Entry ownership and progress

Each Ready is persisted and transmitted exactly once, then its complete newly
committed suffix is transferred through `tr_raft_core_ack_ready()`.
ApplyRuntime copies every retained entry before any external side effect.

Unlike the historical implementation, one unsettled CFlow macrostep does not
own the entire Raft drive loop. `max_pending_entries` is a fixed hard bound
over the current entry plus later committed entries accepted from subsequent
Ready objects. Settled prefixes are compacted before a new Ready is accepted.

Therefore a slow application macrostep does not by itself block later
heartbeat/replication/storage Ready processing. If the application queue is
full, the later Ready remains outstanding and `SALTS_ENOBUFS` is returned
before storage, transport, or Core acknowledgement.

## CFlow correlation

Application entries are decoded by an app callback into a typed
`cflow_event_view`. The adapter calls:

```c
cflow_statechart_instance_try_send_tagged(instance, &event, entry.index);
```

Mailbox success means ownership transfer only. Core application progress moves
only after the V5 `on_external_settlement` callback reports the same token.

Settlement mapping:

- COMPLETED -> APPLIED
- DROPPED -> PENDING; staged CFlow state/events/effects were discarded, so the
  exact entry may be retried
- CANCELLED -> UNKNOWN / `SALTS_ECANCELED`
- FAILED -> UNKNOWN with a preserved failure class:
  - allocation -> `SALTS_ENOMEM`
  - bounded queue/executor capacity -> `SALTS_ENOBUFS`
  - executor closed -> `SALTS_ESHUTDOWN`
  - protocol/type/configuration failures -> `SALTS_EPROTO`
  - guard/action/host-hook failures -> `SALTS_EIO`

GAP, CONFLICT, and UNKNOWN stop normal apply progress and require explicit
reconciliation. Only a proven PENDING entry is automatically retried.

## Configuration entries

Raft configuration entries remain Core-owned. ApplyRuntime never sends them to
CFlow; it acknowledges their contiguous applied index directly after all prior
application entries are proven applied.

## Recovery and snapshots

The in-memory conformance fixture recreates a CFlow instance from a compatible
application snapshot and replays the committed suffix. The resulting typed
state and Core applied boundary must equal uninterrupted execution.

The admitted queue is process-local. Crash recovery starts from the app's
durable applied marker, so admitted-but-unsettled work is replayed and
reconciled rather than trusted.

## Package boundary

`TurboRaft::CFlowStateMachine` is an exported optional component linked to
`TurboRaft::Core` and `Salts::CFlow`. Core-only consumers do not include
CFlow headers.

Hosted CI installs TurboRaft, then independently configures a consumer with:

```cmake
find_package(TurboRaft CONFIG REQUIRED COMPONENTS CFlowStateMachine)
```

The consumer locks the V1 async SPI/config ABI constants and links the installed
symbols.

## Database integration

SQLite/Orm::C and process-crash fixtures are a second explicit integration
gate. They must not become an implicit dependency of Core or the normal hosted
build. PostgreSQL/TidesDB remain live backend qualification gates.
