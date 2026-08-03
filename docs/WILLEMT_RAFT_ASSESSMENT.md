# willemt/raft Adoption Assessment

Status: source-level pre-adoption assessment. No code has been vendored.

Upstream: [https://github.com/willemt/raft](https://github.com/willemt/raft)

## 1. Recommendation

Use `willemt/raft` as the first Phase 0 bootstrap-core candidate because it is small, pure C, BSD licensed, independent of networking, and already has simulation and property-based safety tests.

Do not use it unchanged as the production TurboRaft core. Its current integration contract performs durable storage through synchronous callbacks, it does not provide the required linearizable-read path, and it leaves snapshot transfer to the embedding application.

The adoption decision is therefore:

```text
audit pinned upstream commit
        |
run upstream tests unchanged
        |
prototype TurboRaft ABI and Ready boundary
        |
accept only if safety tests remain authoritative
        |
vendor pinned source plus reviewable patch series
```

## 2. Positive evidence

| Level | Evidence | Impact |
|---|---|---|
| HIGH | Fact: upstream uses a BSD three-clause style license. | Source and binary redistribution are practical if copyright, conditions, and disclaimer are preserved. Final project license compatibility still requires repository-level review. |
| HIGH | Fact: networking is explicitly outside the library. | CoroNet can own framing, TLS, connection lifecycle, and retries without removing an upstream network stack. |
| HIGH | Fact: upstream exposes callbacks for vote, term, log persistence, application, peer messages, snapshots, and membership events. | The core already has explicit integration seams, reducing the amount of consensus logic that TurboRaft must originate. |
| HIGH | Fact: upstream requires vote, term, and log changes to be flushed before persistence callbacks return. | The intended safety boundary is explicit and can be compared against TurboRaft's Ready/durable/send ordering. |
| MED | Fact: upstream is not thread-safe. | This matches a single-owner design if every API call is serialized through the TurboRaft owner. |
| MED | Fact: upstream documents a network simulator, invariant checks, property-based testing, and regression tests. | The existing test suite can serve as a no-regression baseline before TurboRaft extensions are accepted. |
| MED | Fact: learner-style non-voting nodes and snapshot lifecycle hooks exist. | Catch-up and compaction do not need to begin from an empty design, though semantics still require audit. |

## 3. Production gaps

### HIGH: synchronous durability callbacks conflict with CoroNet owner-loop latency

Evidence type: Fact.

`persist_vote`, `persist_term`, `log_offer`, `log_poll`, and `log_pop` must flush before returning. Calling them from the CoroNet context would block peer heartbeats and all groups sharing that context during storage latency.

Minimal acceptable direction:

- Add a deterministic `Ready` effect batch and `Advance` acknowledgement around upstream transitions.
- Keep messages private until associated hard state and log effects are durable.
- Preserve the original synchronous callback path only inside upstream compatibility tests, not as a production fallback.

Rejected direction:

- Do not hide asynchronous storage behind a callback that returns success before `fsync`.
- Do not block the shared CoroNet context on disk.
- Do not send first and repair persistence later.

Alternative if a Ready refactor proves too invasive:

- Run each Raft owner on a dedicated bounded worker and allow synchronous durability there.
- Treat this as a scalability tradeoff requiring explicit group-count and fsync-isolation tests.
- Do not silently switch between the two ownership models at runtime.

The Ready model remains the preferred architecture.

### HIGH: linearizable reads are not implemented

Evidence type: Fact.

The upstream roadmap lists linearizable semantics and efficient read-only queries as future work. TurboRaft requires quorum-confirmed `ReadIndex` and `last_applied >= read_index` before serving M3 namespace or ruleset reads.

Required addition:

- Add read contexts to heartbeat or append acknowledgements.
- Confirm current-term leadership with a quorum.
- Return a safe commit index to the requester.
- Bound pending read contexts and fail them on leadership or term change.
- Keep serializable local reads as a separately named API.

### HIGH: membership safety requires a focused audit

Evidence type: Inference from documented add-non-voter, promote, demote, and remove operations.

The upstream API enforces one voting change at a time and stages node addition, but the documentation does not establish that TurboRaft's required joint-consensus semantics are implemented. The dissertation also has an erratum for single-server membership changes.

Required gate:

- Trace quorum calculation at proposal, commit, apply, recovery, and snapshot boundaries.
- Test partition and crash at every membership transition point.
- Disable public membership mutation until the chosen protocol is proven and documented.
- Prefer learners followed by joint old/new voter configurations.

### HIGH: identity width and lifetime differ from TurboRaft

Evidence type: Fact.

Upstream uses integer node IDs while TurboRaft exposes stable UUID identities. IP addresses and hash-only UUID conversion are unacceptable because they can be reused or collide.

Required adaptation:

- Keep full cluster and node UUIDs in TurboRaft storage and protocol.
- Assign a persisted non-zero internal Raft integer ID from secure random bytes when required by the upstream core.
- Replicate and snapshot the UUID-to-internal-ID mapping.
- Never regenerate or reuse a removed internal ID.
- Hide all upstream ID types behind the TurboRaft C ABI.

### MED: snapshot transfer remains external

Evidence type: Fact.

Upstream provides snapshot lifecycle hooks but explicitly delegates serialization and transfer. TurboRaft must still implement whole-snapshot digesting, chunk transfer, resume, temporary publication, restore, and WAL compaction ordering.

This is compatible with the existing architecture because snapshot bytes belong to Service, Storage, and CoroNet rather than the pure core.

### MED: missing or unconfirmed election hardening

Evidence type: Inference from the documented public message set and APIs.

The reviewed public documentation exposes RequestVote and AppendEntries but does not establish support for pre-vote, check-quorum, or leadership transfer.

Required source audit:

- Confirm whether any feature exists under another name.
- Add pre-vote without incrementing the persisted term.
- Add check-quorum so an isolated leader stops accepting operations.
- Add explicit `TIMEOUT_NOW` leadership transfer after static replication is stable.

### MED: batching and resource bounds need extension

Evidence type: Fact and inference.

Upstream lists batch-friendly APIs on its roadmap and documents internal allocation when growing the log. TurboRaft requires configured hard limits for entry bytes, entry count, inflight traffic, pending proposals, pending reads, and in-memory log retention.

Required adaptation:

- Reject over-limit input before mutation.
- Use overflow-checked size arithmetic.
- Preserve borrowed and owned payload lifetimes across Ready and Advance.
- Add batched append effects without changing log order.
- Replace unbounded growth with explicit capacity outcomes.

## 4. Integration architecture

```text
TurboRaft public C ABI
          |
TurboRaft core adapter
          |
willemt/raft pinned core
          |
Ready effects: hard state, entries, messages, committed entries
          |
TurboUtils storage + CoroNet transport + application FSM
```

No upstream header is installed as a TurboRaft public header. Upstream callbacks terminate inside the adapter. TurboRaft owns message encoding, UUID identity, errors, storage formats, thread model, and lifecycle.

## 5. Vendoring policy

If adopted, place the pinned source under `vendor/willemt_raft` with:

- Exact upstream commit hash.
- Unmodified upstream `LICENSE`.
- `UPSTREAM.md` containing source URL and version date.
- `PATCHES.md` listing every semantic local change.
- Separate commits or patch files for Ready, ReadIndex, election hardening, bounds, and membership work.
- Upstream tests buildable against the vendored core.

The single-file amalgamation is not used because it obscures reviewable local changes and source-level test coverage.

## 6. Phase 0 spike

The spike contains no CoroNet socket implementation and no production WAL. It proves the adaptation boundary first.

Tasks:

1. Pin one upstream commit and import it without modifications.
2. Build upstream unit tests and virtraft simulator on Windows and Linux.
3. Wrap creation, tick, vote, append, and apply behind private TurboRaft adapter functions.
4. Record every synchronous callback and the state mutation that precedes or follows it.
5. Prototype one Ready batch for term/vote and one log append.
6. Prove that no dependent message is observable before durable acknowledgement.
7. Replay upstream simulation traces through the adapter.
8. Estimate the semantic patch surface for ReadIndex, pre-vote, bounds, and membership.

Acceptance criteria:

- Upstream tests pass before and after adapter introduction.
- The same trace produces the same Ready effects.
- Failed persistence leaves the service faulted without exposing dependent messages.
- No upstream type or allocator contract leaks into the public ABI.
- The patch surface is separated by feature and can be independently reviewed.

Rejection criteria:

- Ready adaptation requires maintaining two divergent state machines.
- Persistence failure cannot be represented without partially advancing visible state.
- Existing tests cannot run on supported platforms.
- Required membership changes cannot be reconciled with tested upstream invariants.
- Local modifications become an unreviewable rewrite of most core transitions.

## 7. Decision matrix

| Requirement | willemt/raft unchanged | TurboRaft adaptation required |
|---|---:|---:|
| C implementation | Yes | No |
| Permissive license | Yes | Preserve notices |
| Network independence | Yes | CoroNet adapter |
| Existing invariant simulation | Yes | Keep and extend |
| Non-blocking durability boundary | No | Ready/Advance |
| Linearizable `ReadIndex` | No | Required |
| Snapshot transfer | No | Storage/CoroNet service |
| Pre-vote and check-quorum | Unconfirmed | Audit and likely add |
| Joint membership | Unconfirmed | Audit and likely add |
| Stable UUID identity | No | Required |
| Explicit capacity bounds | Partial | Required |
| TurboHTTP administration | No | Separate optional adapter |

## 8. Final position

`willemt/raft` is a better licensing and language fit than `cowsql/raft` and a better event-system fit than NuRaft. It is also less feature complete than either production-oriented alternative.

The recommended path is to audit and prototype it first. Adoption is justified only if TurboRaft can retain its safety-test value while adding Ready-style durability and the missing production semantics as isolated, reviewable extensions. Otherwise the design falls through to `cowsql/raft`; it does not weaken the TurboRaft contracts to fit the candidate.

## 9. Sources

- [willemt/raft repository and integration guide](https://github.com/willemt/raft)
- [willemt/raft public header](https://github.com/willemt/raft/blob/master/include/raft.h)
- [willemt/raft BSD license](https://github.com/willemt/raft/blob/master/LICENSE)
- [Raft dissertation and membership errata](https://github.com/ongardie/dissertation)
- [etcd Raft Ready integration model](https://github.com/etcd-io/raft)
