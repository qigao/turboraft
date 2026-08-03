# CoroNet peer service architecture

## Decision background

The CoroNet adapter has separate ownership boundaries for authenticated
identity snapshots, inbound admission, outbound retry state, managed sessions,
and Runtime message enqueue. Applications otherwise need to reproduce creation,
dependency injection, identity rotation, and shutdown ordering.

## Options considered

1. Application-owned composition keeps the library small but duplicates
   security wiring and makes partial shutdown likely.
2. A global singleton hides dependencies and prevents isolated clusters or
   deterministic tests.
3. A bounded peer-service facade preserves the existing components and adds one
   explicit owner without moving protocol or socket logic into the facade.

The third option is selected. It does not change wire formats or handshake
semantics. The facade enqueue adapter intentionally implements Runtime's
reliable-local-queue boundary instead of performing socket I/O inline.

## Ownership and state

The service owns one peer manager, one immutable identity registry snapshot,
zero or one inbound admission service, one bounded queue per configured peer,
one reader per connected peer, one on-demand writer, and at most
`TR_RAFT_MAX_VOTERS - 1` outbound schedulers. The manager remains the sole owner
of admitted sessions and owned sockets. The service stores no session mirror.

The service is single-CoroNet-context state. Runtime enqueue copies a message
into a peer queue and returns without socket I/O. Queue capacity is explicit;
full queues return `TURBO_ENOSPC`. Reader and writer pumps are service-owned,
and an operation counter prevents identity mutation or destroy while they run.

## Shutdown and failure handling

For graceful shutdown, the application first waits for
`queued_message_count == 0`, closes its listener, and calls `stop()`. Stop
rejects new enqueue with `TURBO_EPIPE` and closes managed sockets to wake every
pump. The application runs the CoroNet context until
`active_operation_count == 0`, then calls destroy. Destroy returns
`TURBO_EBUSY` without changing ownership if this boundary is not met. Pending
queue entries are discarded only during final destruction.

Creation and identity replacement are transactional: all input is validated and
the replacement snapshot is fully allocated before the authoritative pointer is
swapped. Failure leaves the old snapshot active.

## Migration and rollback

Existing users may continue composing transport components directly. Migration
creates the facade, moves resolver wiring and scheduler ownership into it, then
uses its inbound handler and Runtime enqueue adapter. Rollback removes the
facade calls without changing persisted data or network protocol.

## Validation scope

Tests cover bounded ownership and queues, injected identity resolution, inbound
result delivery, outbound stepping, identity snapshot replacement, reentrant
destroy rejection, pump shutdown, and final quiescent destruction. The live
three-node mTLS suite validates all three authenticated sessions and six
bidirectional routed messages.
