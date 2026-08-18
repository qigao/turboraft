# TurboRaft Dynamic Membership and Joint Consensus

## Status

Accepted design for the dynamic-membership implementation. Static voter and
learner behavior remains the compatibility baseline while the implementation is
introduced in independently testable layers.

## Context

TurboRaft currently copies a stable voter/learner configuration into Core at
startup. Learners replicate entries but do not vote, campaign, contribute to a
quorum, serve ReadIndex, or receive leadership transfer. This is sufficient for
static clusters, but changing voters in place would allow two disjoint
majorities to commit conflicting logs.

Dynamic membership affects Core quorum calculations, log semantics, Runtime
application, WAL recovery, snapshot metadata, transport peer ownership, and
the future RPC control plane. The committed Raft log remains the primary fact
source. RPC and UI code may request a transition, but may not directly mutate
membership.

## Decision

### Configuration entry representation

Normal proposals retain the public invariant `command_id > 0`. The previously
invalid value `command_id == 0` is reserved for TurboRaft configuration entries.
The entry data begins with a versioned binary configuration payload. Therefore:

- existing AppendEntries wire versions carry configuration entries unchanged;
- user command identifiers cannot collide with internal entries;
- malformed command-zero entries fail with `TURBO_EPROTO`;
- Runtime never forwards command-zero entries to the application state machine.

The version-1 payload is encoded explicitly in network byte order and does not
depend on C struct layout:

| Field | Bytes | Constraint |
| --- | ---: | --- |
| magic `TRCF` | 4 | exact value |
| codec version | 1 | `1` |
| phase | 1 | `JOINT` or `FINAL` |
| member count | 1 | `1..TR_RAFT_MAX_MEMBERS` |
| reserved | 1 | zero |
| transition ID | 8 | non-zero and stable across both entries |
| members | `9 * count` | node ID (`u64`) followed by role flags (`u8`) |

Member IDs are non-zero and strictly ascending. Role flags are:

- `OLD_VOTER = 0x01`
- `NEW_VOTER = 0x02`
- `LEARNER = 0x04`

Unknown bits are rejected. `NEW_VOTER | LEARNER` is invalid, while
`OLD_VOTER | LEARNER` represents a voter being demoted in the target state. A
stable `FINAL` entry has identical old/new voter flags. At 31 members the
payload is 295 bytes, below `TR_RAFT_MAX_ENTRY_BYTES == 512`.

### State model and ownership

Core owns exactly one committed configuration state:

- `STABLE`: one voter set and one learner set;
- `JOINT`: old voters, new voters, and target learners.

Core also tracks at most one uncommitted configuration transition. A second
request returns `TURBO_EBUSY`. The transition ID makes replay and duplicate
delivery idempotent while rejecting a conflicting transition.

The leader appends a `JOINT` entry containing the complete old/new state. That
entry commits under the old stable quorum. Once applied, the leader appends the
matching `FINAL` entry. The final entry commits under both old and new quorums.
Only then does Core return to `STABLE`.

Configuration entries are applied in log-index order when commit advances. No
configuration change becomes active merely because it was appended or stored.

### Joint quorum rules

While stable, the existing majority rule is unchanged. While joint, each of the
following independently requires a majority of old voters and a majority of
new voters:

- election and pre-vote success;
- log commit advancement;
- check-quorum leader lease renewal;
- ReadIndex acknowledgement;
- leadership-transfer target eligibility.

Replication targets are the union of old voters, new voters, and learners.
Duplicate node IDs share one progress record. The total union remains bounded
by `TR_RAFT_MAX_MEMBERS`.

The leader may be removed by the target configuration. It remains leader during
the joint phase, must not accept a new membership request, and steps down as soon
as the final configuration commits. A removed node retains its log but no longer
campaigns or votes.

### Ready and Runtime ordering

When a commit activates a configuration entry, Ready exposes the resulting
configuration together with the commit index. Runtime preserves this order:

1. persist hard state, log changes, commit index, and configuration metadata in
   one storage transaction;
2. enqueue resulting peer messages;
3. apply only normal command spans to the application state machine;
4. acknowledge Core advance.

Any persistence failure faults Runtime. Core cannot process another operation
while the Ready is outstanding, so an in-memory configuration cannot advance
past failed durable state.

### Recovery and snapshots

The WAL snapshot record adds versioned configuration metadata to the snapshot
record. Migration from version 2 creates an empty metadata value. Empty metadata
is accepted only for a legacy snapshot and uses the explicitly supplied
bootstrap configuration.

Recovery starts from the snapshot configuration, then replays committed
configuration entries after the snapshot index. Uncommitted configuration
entries are retained as pending log state but do not alter quorum rules.

Every newly created or installed snapshot must include the committed
configuration at exactly `last_included_index`. Snapshot application bytes stay
opaque and unchanged. Configuration metadata is transported and persisted as a
separate bounded field, not prepended to application data.

### RPC control-plane contract

The future TurboHTTP RPC layer exposes commands rather than mutable state:

- `raft.membership.get`
- `raft.membership.change`
- `raft.membership.status`

`raft.membership.change` accepts a complete target voter/learner set and a
client-generated idempotency key. Only the current leader accepts it; followers
return the leader's virtual mesh address when known. RPC completion means the
final entry committed, not merely that the joint entry was appended.

The HTMX UI reads the same status view and invokes the same command API. It does
not own separate membership state.

## Compatibility and migration

- Normal command behavior and Raft AppendEntries wire versions remain
  compatible.
- `command_id == 0` remains invalid for public normal proposals.
- WAL recovery validates snapshot configuration transactionally.
- A version-3 database is not writable by an older TurboRaft binary. Deployment
  must retain a pre-migration backup for binary rollback.
- Legacy snapshots can recover using bootstrap configuration, but the next
  snapshot must write explicit configuration metadata.
- Snapshot peers that do not support configuration metadata must fail the
  handshake; silently omitting it could recover with the wrong quorum.

## Rejected alternatives

### Replace voters immediately

Rejected because old and new majorities may be disjoint and commit conflicting
entries.

### Add an entry-type field and Raft wire v4

Rejected for this implementation because command zero is already outside the
public proposal domain and safely distinguishes internal entries. A wire schema
change would add migration cost without carrying additional information.

### Embed configuration in application snapshot bytes

Rejected because it couples Raft recovery to application serialization and
would change bytes observed by existing restore callbacks.

### Store membership only in durable storage

Rejected because storage would become an independent mutable fact source. The
committed log and snapshot configuration must be sufficient to reconstruct the
same state.

## Failure handling and rollback

- Invalid payload, overlapping roles, empty voter sets, unsorted IDs, or an
  oversized union fail before log append.
- A leader change does not discard an appended configuration entry; the new
  leader continues from the replicated log.
- A committed joint state must be finalized before another transition.
- Transport peer creation/removal is derived from committed configuration.
  Connection failure does not roll back membership.
- Snapshot configuration and its FSM payload become authoritative through one
  WAL transaction. Failure leaves the previous snapshot authoritative.
- Binary rollback across a WAL format change requires restoring a matching
  WAL and snapshot backup.

## Verification gates

Implementation is not complete until all gates pass:

1. Configuration codec rejects malformed and non-canonical payloads.
2. Static behavior remains unchanged for command-only logs.
3. Joint commit requires both old and new majorities.
4. Elections, check-quorum, ReadIndex, and transfer use both voter sets.
5. Learner add, promote, demote, voter removal, and leader removal converge.
6. Crash recovery works before joint commit, during joint state, and after final
   commit.
7. Snapshot install restores the exact configuration at the snapshot index.
8. Partition tests prove no two sides can commit conflicting commands during a
   transition.
9. WAL snapshot configuration recovery and corruption rejection pass.
10. `ctest --preset win-release-user` passes in full.
