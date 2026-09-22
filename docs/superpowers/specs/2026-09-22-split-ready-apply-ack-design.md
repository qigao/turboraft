# Split Ready durability from application apply acknowledgement

Issue: #23

## Context

TurboRaft currently has one Core acknowledgement, `tr_raft_core_advance()`. It
means all effects in the outstanding Ready are complete and, when the Ready
contains committed entries, it also moves `applied_index` to the entire
committed suffix.

That contract is correct for the existing atomic
`tr_raft_state_machine_t::apply_batch()`, but it cannot represent an
asynchronous state machine that has accepted an entry without having proved its
terminal durable application yet. It also cannot safely represent a successful
prefix followed by a failed or pending suffix.

## Decision

Introduce two independent Core facts:

1. **admitted cursor** — highest committed log index whose ownership has moved
   from the current Ready to the application driver;
2. **applied cursor** — the existing `applied_index`, meaning the highest
   contiguous index whose application outcome is terminal and proven durable.

The admitted cursor is process-local. It starts at `initial_applied_index` and
is intentionally not persisted. Crash recovery therefore reconstructs
admission from the durable applied boundary and replays the uncertain suffix.

### Additive Core APIs

```c
int tr_raft_core_ack_ready(tr_raft_core_t *core,
                           size_t admitted_entry_count);

int tr_raft_core_ack_applied(tr_raft_core_t *core,
                             tr_raft_index_t index);
```

`tr_raft_core_ack_ready()` acknowledges the non-application effects of the
current Ready and transfers ownership of an exact committed-entry prefix. The
count is relative to the current Ready. A count smaller than
`ready.committed_entry_count` leaves the unadmitted suffix eligible for the
next Ready without replaying the already acknowledged Ready side effects.

`tr_raft_core_ack_applied()` accepts exactly the next contiguous admitted
index. It never skips a gap and never acknowledges an index that was not first
admitted.

The existing `tr_raft_core_advance()` remains the compatibility wrapper. It
continues to acknowledge the whole outstanding Ready and move
`applied_index` through the whole emitted committed suffix exactly as before.

## Ready generation

Committed entries are generated from:

```text
apply_admitted_index + 1 ... commit_index
```

rather than:

```text
applied_index + 1 ... commit_index
```

This prevents an async entry from being admitted twice while its settlement is
still pending.

## Runtime compatibility behavior

The existing synchronous batch Runtime keeps its public ABI and normal fast path.

If `apply_batch()` returns `SALTS_EBUSY`:

1. storage and transport have already completed;
2. Runtime acknowledges the whole Ready as owned by its retained blocked suffix;
3. any prefix already completed before the blocked batch is acknowledged
   exactly through Core;
4. Runtime keeps the exact suffix and retries only application work;
5. later retry success advances the exact remaining applied indices without
   replaying storage or transport.

This fixes the successful-prefix / failed-suffix divergence while preserving
batching for existing state machines.

## Configuration entries

Configuration entries remain Core-owned. They are not sent to the application
state machine, but they participate in the same contiguous index sequence.
Runtime acknowledges them as applied when all preceding application entries are
proven applied.

## Recovery

Only the application durable marker is restored into
`initial_applied_index`. The admitted cursor is initialized to the same value.

Therefore:

- crash before admission: suffix is replayed;
- crash after admission but before settlement: suffix is replayed and the
  application reconciles its durable marker/identity;
- crash after settlement but before Core applied acknowledgement: suffix is
  replayed and reconciliation proves it already applied;
- crash after applied acknowledgement: recovery starts after that index.

No new WAL, wire, snapshot, or configuration format is introduced.

## Compatibility

- no change to `tr_raft_state_machine_t`;
- no enlargement of existing public Runtime/Service structs;
- no wire/WAL/snapshot format change;
- existing `tr_raft_core_advance()` callers keep their behavior;
- new APIs are additive and are the binding seam for #22.

## Invariants

- `applied_index <= apply_admitted_index <= commit_index`;
- Ready emits only indices greater than `apply_admitted_index`;
- applied acknowledgement is exact and contiguous;
- Ready acknowledgement cannot admit more entries than the current Ready
  exposed;
- snapshot creation still requires `applied_index == commit_index`;
- crash recovery never trusts the volatile admitted cursor.

## Verification

The implementation must cover:

- legacy `tr_raft_core_advance()` unchanged;
- partial Ready admission re-emits only the unadmitted suffix;
- admitted-but-unapplied entries are not re-emitted during normal Core
  progress;
- exact applied acknowledgement rejects gaps and unadmitted indices;
- restart reconstructs admission from durable `initial_applied_index`;
- Runtime success-prefix + `SALTS_EBUSY` suffix advances only the proven
  prefix and retry does not replay storage/transport;
- full Release CTest remains green.
