# TurboRaft Implementation Plan

Status: proposed execution plan. Each phase has a release gate; later phases do not compensate for an unmet earlier safety gate.

## 1. Delivery strategy

The first production claim is not made when three processes exchange heartbeats. It is made only after deterministic safety tests, crash recovery, network fault injection, and repeatable build presets pass.

The implementation proceeds from pure state transitions outward:

```text
core model -> durable storage -> CoroNet transport -> static cluster
           -> ReadIndex/snapshot -> membership -> TurboHTTP -> hardening
```

## 2. Phase 0: repository and dependency decisions

Deliverables:

- CMake project with install/export package support.
- Local Windows and Linux presets derived from TurboNet conventions.
- vcpkg manifest with pinned baseline and direct dependencies only.
- Public C ABI naming, visibility, and error-domain conventions.
- License, source, test, and adapter report for `willemt/raft`.
- License and adapter report for `cowsql/raft` if the `willemt/raft` gate fails.
- Codec spike for TurboUtils TBE versus a local bounded codec.
- CRC32C provider selection and license record.
- Minimal TinyTest target and sanitizer presets.

Decision gate:

- Select `willemt/raft` only if its existing tests pass unchanged and the Ready/durability, ReadIndex, election, bounds, snapshot, and membership gaps have accepted designs.
- Select `cowsql/raft` only if the `willemt/raft` gate fails and license approval, Windows build, custom I/O integration, deterministic simulation, and ABI hiding all pass.
- Select a native core only with explicit acceptance of the larger correctness scope.
- Do not combine two Raft cores or maintain a runtime fallback.

Exit criteria:

- A clean configure can locate installed TurboUtils and TurboNet packages.
- TurboHTTP is optional and does not enter Core or Storage link interfaces.
- The selected core and all vendored material have source, version, license, and patch records.

## 3. Phase 1: deterministic core

Deliverables:

- Roles, terms, voting, pre-vote, log matching, commit calculation, and per-peer progress.
- Explicit ticks and randomized election timeout input.
- `Ready` and `Advance` protocol.
- Bounded proposal, message, and entry batches.
- Static configuration with three or five voters.
- In-memory storage model for tests only.

Required tests:

- Paper election, vote restriction, log replication, conflict, and old-term commit cases.
- Duplicate, delayed, reordered, and dropped messages.
- Split votes, partitioned old leader, term changes, and leader restart state.
- Determinism replay: identical input trace produces byte-identical effects.
- Invariants: election safety, log matching, leader completeness, and state-machine safety.
- Model-based traces compared with the Raft TLA+ model or a checked equivalent.
- Differential traces against etcd Raft behavior where semantics overlap.

Exit criteria:

- No network or filesystem code is linked into the core tests.
- Every growing core structure has a configured hard limit and tested rejection.
- Ten thousand seeded simulation traces can be reproduced by seed and trace file. The count is a validation workload, not a correctness proof.

## 4. Phase 2: durable storage

Deliverables:

- Exclusive storage ownership lock.
- Versioned identity file, segmented WAL, hard state, and snapshot metadata.
- CRC32C validation and bounded record decoding.
- Conflict truncation and segment compaction.
- Recovery planner that validates before mutating files.
- Snapshot temporary-file publication with file and directory durability.
- Single-writer storage executor and ordered completion path.

Required tests:

- Restart after every write, truncate, sync, rename, and directory-sync boundary.
- Torn final record recovery.
- Middle-record corruption rejection.
- Wrong cluster/node identity rejection.
- Missing segment, overlapping segment, index gap, stale snapshot, and invalid commit rejection.
- Windows and Linux durability adapter tests.
- Read-only recovery inspection tool that never repairs implicitly.

Exit criteria:

- Acknowledged hard state and entries survive process termination at every injected crash point.
- Corruption is either safely identified as a torn final append or fails fast.
- No successful recovery path invents a term, vote, entry, identity, or membership value.

## 5. Phase 3: CoroNet peer transport

Deliverables:

- TCP/TLS listener and outbound peer connection manager.
- HELLO identity and capability negotiation.
- Incremental bounded frame decoder and encoder.
- Peer authentication bound to cluster and node UUID.
- Per-peer request and byte windows.
- Reconnect policy with jitter, cancellation, and shutdown.
- Separate or prioritized snapshot path.

Required tests:

- Split and coalesced TCP frame delivery.
- Invalid header, length, checksum, flag, identity, and version.
- Slow reader, stalled writer, idle peer, reconnect storm, and duplicate connection.
- Backpressure at every configured capacity.
- Shutdown with pending connect, receive, send, and snapshot operations.
- Fuzzing of frame and message decoders.

Exit criteria:

- No untrusted length reaches allocation before bounds validation.
- No peer message reaches the wire before its associated durable barrier.
- Context shutdown leaves no managed coroutine, socket, callback, or receive buffer owned by the service.

## 6. Phase 4: static-cluster service

Deliverables:

- Owner-loop service driver.
- Durable Ready processing.
- Proposal handles and completion callbacks.
- Deterministic application adapter.
- Leader hints and explicit not-leader errors.
- Static three-node example using a small key/value state machine.

Required scenarios:

- Bootstrap three fresh nodes.
- Commit with one node offline.
- Stop the leader during proposal replication and elect a new leader.
- Restart every node from disk in different orders.
- Isolate the old leader, commit on the majority, then heal the partition.
- Saturate proposal and transport limits without unbounded growth.

Stage acceptance:

- All nodes apply the same command at each applied index.
- A client receives success only after commit and application.
- A proposal that times out remains unknown, not reported as failed commitment.
- Static-cluster M3 namespace adapter can replace the process-local adapter.

## 7. Phase 5: linearizable reads and snapshots

Deliverables:

- Quorum-confirmed `ReadIndex` on leader and followers.
- Applied-index waiting with bounded pending reads.
- Application snapshot writer and restore reader.
- Chunked resumable snapshot transfer.
- Log compaction after durable snapshot publication.

Required scenarios:

- Read during stable leadership, leader change, partition, and apply lag.
- Snapshot while proposals continue.
- Restart from snapshot plus WAL tail.
- Interrupt and resume snapshot transfer.
- Reject snapshot hash, identity, term, and membership mismatch.

Exit criteria:

- A linearizable read never executes before its safe index is applied.
- Compaction never removes the only durable copy needed for recovery.
- Snapshot install cannot expose partially restored application state.

## 8. Phase 6: membership and leadership operations

Deliverables:

- Learner add and catch-up.
- Joint-consensus voter transition.
- Learner promotion and member removal.
- Leadership transfer.
- Replicated endpoint update using stable node identity.

Required scenarios:

- Add learner, interrupt catch-up, resume, and promote.
- Replace one voter without losing quorum.
- Partition during each joint-configuration stage.
- Reject two overlapping membership transitions.
- Remove leader with and without a valid transferee.
- Verify removed IDs cannot rejoin or be reused.

Exit criteria:

- Quorum calculation uses the correct old, joint, or new configuration at every log position.
- Membership state is recoverable from snapshot and WAL alone.
- No management command can force-commit or bypass joint consensus.

## 9. Phase 7: TurboHTTP management adapter

Deliverables:

- Explicit Iris app integration, not hidden global initialization.
- Read-only status and progress RPCs.
- Authenticated membership, transfer, and snapshot RPCs.
- Bounded asynchronous request bridge to the owner loop.
- Audit records and stable JSON-RPC error mapping.

Required tests:

- Authentication and authorization denial.
- Request, batch, response, and pending-operation limits.
- Leader redirect/hint behavior.
- HTTP disconnect while an owner-loop operation remains pending.
- Service shutdown with active RPC requests.
- Redaction of credentials, certificates, commands, and application payloads.

Exit criteria:

- HTTP threads cannot directly access mutable Raft state.
- Public RPC cannot invoke peer-only protocol operations.
- Mutation success includes committed term and index.

## 10. Phase 8: production hardening

Deliverables:

- Multi-process chaos runner with deterministic fault scripts.
- Long-duration election, replication, snapshot, and restart tests.
- ASan, UBSan, TSan where supported, and MSVC diagnostics.
- WAL/protocol fuzz corpora.
- Upgrade compatibility matrix.
- Operator recovery and backup documentation.
- Performance benchmarks after correctness gates pass.

Fault matrix:

| Fault | Expected result |
|---|---|
| Packet loss, duplication, delay, reorder | Safety preserved; availability depends on majority and timing. |
| Minority partition | Minority rejects writes and linearizable reads. |
| Leader partition | Majority elects a new leader; old leader steps down after term/quorum evidence. |
| Disk full or fsync failure | Local service enters `FAULTED`; no dependent message is sent. |
| Torn final WAL write | Recovery truncates only the invalid final record. |
| Middle WAL corruption | Startup fails with exact segment and offset. |
| Process kill during snapshot | Previous snapshot remains authoritative. |
| Slow application apply | Commit may advance, applied index lags, reads wait with bounds. |
| Clock jump | Tick scheduling may be delayed, but safety does not depend on wall clock. |

Production exit criteria:

- All documented invariants have deterministic tests and trace artifacts.
- Crash and network fault matrices pass on Windows and Linux.
- No known path acknowledges data before the required durable barrier.
- Rolling upgrade and rollback work for every declared compatible version pair.
- M3 namespace, mesh rules, and task lease state machines pass application-specific fencing and replay tests.

## 11. Proposed repository layout

```text
turboraft/
  CMakeLists.txt
  CMakePresets.json
  vcpkg.json
  cmake/
  include/turboraft/
    raft.h
    raft_errors.h
    raft_state_machine.h
    raft_transport.h
    raft_storage.h
  src/core/
  src/storage/
  src/coronet/
  src/service/
  src/http/
  schemas/
  tests/core/
  tests/storage/
  tests/transport/
  tests/integration/
  tests/fuzz/
  examples/kv_cluster/
  tools/inspect/
  docs/
```

## 12. Initial configuration limits

Every limit is a named configuration field. Defaults are provisional and become stable only after representative tests.

| Resource | Provisional default | Full behavior |
|---|---:|---|
| Command bytes | 1 MiB | Reject proposal. |
| Append message bytes | 4 MiB | Split before transport. |
| Entries per append | 256 | Split before transport. |
| Per-peer inflight messages | 64 | Apply backpressure. |
| Per-peer inflight bytes | 32 MiB | Apply backpressure. |
| Pending proposal bytes | 64 MiB | Reject new proposal. |
| Pending read requests | 4096 | Reject new read. |
| WAL segment bytes | 64 MiB | Rotate after durable record boundary. |
| Snapshot chunk bytes | 1 MiB | Negotiate lower peer limit. |

These values are capacity hypotheses, not performance facts. Tests must cover exact-boundary, one-over-boundary, arithmetic-overflow, and recovery behavior.

## 13. Open implementation decisions

| Priority | Decision | Required evidence |
|---|---|---|
| HIGH | Adopt `willemt/raft`, fall through to `cowsql/raft`, or implement native core. | Upstream regression suite, license approval, Ready adapter spike, ReadIndex and membership gap audit, deterministic trace support, Windows build. |
| HIGH | Application `last_applied` durability contract. | M3 and task state-machine transaction model, snapshot recovery test. |
| HIGH | TLS node identity representation. | CoroNet TLS certificate APIs and deployment PKI contract. |
| MED | TBE or private codec implementation. | Canonical encoding, unknown-field behavior, fuzzability, generated-code ABI isolation. |
| MED | CRC32C provider. | vcpkg availability, license, Windows/Linux support, benchmark only after correctness. |
| MED | One storage worker per group or shared bounded pool. | Expected group count, fsync isolation test, shutdown ownership. |
| LOW | Lemon usage. | Introduce only if a future human-authored grammar requires it. |

## 14. First implementation slice

After Phase 0 decisions, the first coding slice should contain only:

- Public opaque core types and stable error domain.
- In-memory log implementation for tests.
- Follower, candidate, and leader transitions driven by explicit tick and message inputs.
- Static three-voter election and empty heartbeat replication.
- Deterministic simulation test with a reproducible seed.

It must not yet contain filesystem I/O, CoroNet, TurboHTTP, snapshots, dynamic membership, M3 integration, or production claims.

## 15. Primary references

- [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf)
- [Consensus: Bridging Theory and Practice](https://github.com/ongardie/dissertation)
- [Raft TLA+ specification](https://github.com/ongardie/raft.tla)
- [etcd Raft deterministic core and Ready integration](https://github.com/etcd-io/raft)
- [willemt/raft](https://github.com/willemt/raft)
- [cowsql/raft](https://github.com/cowsql/raft)
- [NuRaft](https://github.com/eBay/NuRaft)
