# TurboRaft Multi-Group Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make TurboRaft a database-grade Multi-Raft-capable consensus substrate by adding group-aware transport multiplexing, bounded per-group isolation, and fully streaming snapshot I/O while preserving existing single-group Core/Service semantics.

**Architecture:** One `tr_raft_service_t` remains one Raft group. `group_id` is added only to the shared wire/transport routing envelope, while FlowMQ/CNet multiplex many groups over one physical peer link with bounded fair scheduling. Snapshot payloads become source/sink streams so database-scale snapshots never require whole-payload residency.

**Tech Stack:** C11, Salts bounded buffers/filesystem primitives, FlowMQ, CNet, TBE/DataBind wire codec, TinyTest/CTest, multiprocess chaos harness.

**Spec:** `docs/superpowers/specs/2026-09-19-multi-group-foundation-design.md`

## Global Constraints

- `tr_raft_message_t` MUST NOT gain `group_id`.
- One `tr_raft_service_t` remains exactly one independently recoverable Raft group.
- Existing Raft Core election/replication/ReadIndex/membership semantics remain unchanged in R0.
- Existing Core/Service/WAL semantics remain stable, but the peer transport API is intentionally breaking: every payload requires a non-zero `group_id` and no legacy wire adapter is retained.
- All new queues, active-group tables, chunk windows, and retained payload budgets are explicitly bounded.
- Unknown/stopped group traffic must not fault the physical peer session.
- Malformed/identity-invalid wire traffic must continue to fail the session.
- Each group keeps an independent WAL/storage instance; no cross-group WAL is introduced.
- Snapshot bytes remain opaque to TurboRaft.
- No sharding, placement, SQL, KV semantics, SQLite/TidesDB semantics, or MapReduce enter TurboRaft.

## Review Focus

- Pre-R0 legacy peer frames must be rejected explicitly; there is no downgrade or dual-protocol path.
- Group saturation must not starve another group's heartbeat/election messages on the same peer.
- Unknown or stopped group IDs must be rejected without destroying a valid authenticated peer session.
- Snapshot retry after reconnect must keep immutable snapshot identity and cumulative-ACK semantics.
- Snapshot source/sink failure must never publish partial state or advance Core snapshot progress.

---

### Task 1: Replace the peer wire contract with mandatory group-aware transport (#26)

**Files:**
- Modify: `include/turboraft/raft_wire_codec.h`
- Modify: `src/wire/raft_wire_codec.c` or the repository's generated/codec implementation owning `tr_raft_wire_metadata_t`
- Modify: `include/turboraft/raft_peer_handshake.h`
- Modify: `src/transport/raft_peer_handshake.c`
- Modify: `include/turboraft/raft_transport.h`
- Modify: `src/transport/raft_transport.c`
- Test: `tests/core/test_raft_wire_codec.c`
- Test: `tests/core/test_raft_peer_handshake.c`
- Test: `tests/core/test_raft_transport_contract.c`
- Test: `tests/core/test_raft_wire_fuzz_corpus.c`

**Interfaces:**
- Produces: `typedef uint64_t tr_raft_group_id_t;`
- Produces: group-aware `tr_raft_wire_metadata_t { cluster_id, group_id, message_id }`
- Produces: one current wire version/header shared by Raft, snapshot, and data payloads
- Produces: one transport payload callback carrying validated group identity without modifying `tr_raft_message_t`

- [ ] **Step 1: Write RED tests for group identity in the wire envelope**

Add tests that encode two otherwise-identical frames with group IDs 100 and 101 and assert the decoded metadata preserves the exact group ID. Add rejection tests for group ID 0 on every current-wire encode/decode path. Add compile-time/API tests proving there is no legacy header/version selector surface.

Representative assertion:

~~~c
tr_raft_wire_metadata_t metadata = {
    .cluster_id = cluster_id,
    .group_id = 100U,
    .message_id = 7U,
};
CHECK(tr_raft_wire_encode(codec, &metadata, &message,
                          frame, sizeof(frame), &frame_size) == SALTS_OK);
CHECK(tr_raft_wire_decode(codec, frame, frame_size,
                          &decoded_metadata, &decoded) == SALTS_OK);
CHECK(decoded_metadata.group_id == 100U);
~~~

- [ ] **Step 2: Run focused tests and confirm RED**

Run:

~~~powershell
cmake --preset win-release-user
cmake --build --preset win-release-user --target turboraft_wire_tests turboraft_peer_handshake_tests turboraft_transport_contract_tests
ctest --preset win-release-user -R "turboraft\.(wire|peer_handshake|transport_contract)$" --output-on-failure
~~~

Expected: compile/test failure because group-aware metadata/capability does not yet exist.

- [ ] **Step 3: Collapse to one current wire contract**

Remove historical peer-wire compatibility from the public transport path:
- one current wire version,
- one header size,
- mandatory non-zero `group_id`,
- no `TR_RAFT_HANDSHAKE_FEATURE_GROUP_MULTIPLEX_V1`,
- no Raft/snapshot/data wire-version selector functions,
- no legacy single-group transport callback.

Handshake still validates cluster/node/process identity and negotiated bounds, but not historical payload versions.

- [ ] **Step 4: Extend the common wire metadata, not individual payload structures**

Add `group_id` to the common envelope metadata and update encoding/decoding for every payload kind through the shared metadata path.

Do **not** add group identity to:
- `tr_raft_message_t`
- `tr_raft_snapshot_chunk_t`
- `tr_raft_data_chunk_t`

The common envelope is the single routing fact source.

- [ ] **Step 5: Add transport session contract validation**

Update `tr_raft_transport_session_config_t` and session validation so every session requires the unified payload callback. Every outbound payload requires a non-zero `group_id`; group-less encode attempts fail.

- [ ] **Step 6: Extend fuzz/corruption coverage**

Add corpus cases for:
- zero group ID,
- altered group ID with otherwise-valid envelope,
- truncated current header,
- pre-R0 legacy header/version input,
- unknown future version.

Protocol corruption must remain fail-closed.

- [ ] **Step 7: Run focused and adjacent regression tests**

~~~powershell
cmake --build --preset win-release-user
ctest --preset win-release-user -R "turboraft\.(wire|wire_fuzz_corpus|peer_handshake|transport|transport_contract)$" --output-on-failure
~~~

Expected: PASS.

- [ ] **Step 8: Commit**

~~~bash
git add include/turboraft/raft_wire_codec.h         include/turboraft/raft_peer_handshake.h         include/turboraft/raft_transport.h         src/transport/raft_peer_handshake.c         src/transport/raft_transport.c         tests/core/test_raft_wire_codec.c         tests/core/test_raft_peer_handshake.c         tests/core/test_raft_transport_contract.c         tests/core/test_raft_wire_fuzz_corpus.c
git commit -m "feat(transport): require raft group identity"
~~~

---

### Task 2: Add shared-link group multiplexing, bounded fairness, and isolation (#27)

**Files:**
- Modify: `include/turboraft/raft_flowmq_peer_service.h`
- Modify: `src/transport/raft_flowmq_peer_service.c`
- Modify: `include/turboraft/raft_cnet_peer.h`
- Modify: `src/transport/raft_cnet_peer.c`
- Modify: `src/transport/raft_transport_payload_storage.h`
- Test: `tests/core/test_raft_flowmq_peer_service.c`
- Test: `tests/core/test_raft_cnet_peer.c`
- Test: `tests/core/test_raft_transport.c`

**Interfaces:**
- Consumes: Task 1 group-aware transport envelope.
- Produces: group-tagged enqueue API for FlowMQ/CNet.
- Produces: bounded per-group queue accounting.
- Produces: group-aware inbound callback/dispatch metadata for embedding runtimes.
- Produces: deterministic bounded round-robin send scheduling.

- [ ] **Step 1: Write RED tests for two groups sharing one peer adapter**

Create tests that enqueue alternating payloads for group 100 and 101 over one peer service and verify both group identities survive encode/decode and dispatch.

Also add a saturation test:
- fill group 100 to its per-group limit,
- enqueue heartbeat/replication work for group 101,
- verify group 101 still makes progress within a bounded number of `step()` calls.

- [ ] **Step 2: Write RED test for unknown-group isolation**

Configure the inbound test router to reject group 999 with the typed group-routing error, then send a valid group 100 frame immediately afterward over the same physical connection. Verify group 100 still dispatches.

- [ ] **Step 3: Run focused tests and confirm RED**

~~~powershell
cmake --build --preset win-release-user --target turboraft_flowmq_peer_service_tests turboraft_cnet_peer_tests turboraft_transport_tests
ctest --preset win-release-user -R "turboraft\.(flowmq_peer_service|cnet_peer|transport)$" --output-on-failure
~~~

- [ ] **Step 4: Introduce bounded group queue storage**

Use existing Salts/CSTL bounded container primitives. Each queued item must carry:
- `group_id`,
- encoded/copied payload ownership exactly as today,
- retained byte accounting.

Configuration must include explicit:
- max active groups,
- per-group message capacity,
- per-group retained byte capacity,
- existing global peer HWM.

Creation fails if required capacities are zero or inconsistent.

- [ ] **Step 5: Implement deterministic bounded round-robin scheduling**

Each `step()`:
- visits active groups from the saved cursor,
- admits at most the configured send batch,
- sends at most a bounded amount per group before advancing,
- preserves FIFO order within each group,
- never drains one group unboundedly.

Do not add priority weights in R0.

- [ ] **Step 6: Keep group-routing rejection below session-failure boundary**

Inbound transport validation still owns malformed frame/TLS/cluster/source/destination failures.

After a valid frame is decoded:
- unknown/stopped group -> return/report group-routing rejection,
- do not mark the peer transport session faulted,
- continue processing later valid frames.

- [ ] **Step 7: Add bounded diagnostics**

Expose snapshot-style status for:
- active group count,
- queued global items/bytes,
- per-group queued items/bytes,
- blocked group count,
- unknown-group rejection count.

Do not store unbounded history.

- [ ] **Step 8: Run focused tests and mTLS loopback regression**

~~~powershell
cmake --build --preset win-release-user
ctest --preset win-release-user -R "turboraft\.(flowmq_peer_service|cnet_peer|transport|snapshot_peer)$" --output-on-failure
~~~

Expected: PASS, including existing mTLS behavior.

- [ ] **Step 9: Commit**

~~~bash
git add include/turboraft/raft_flowmq_peer_service.h         include/turboraft/raft_cnet_peer.h         src/transport/raft_flowmq_peer_service.c         src/transport/raft_cnet_peer.c         src/transport/raft_transport_payload_storage.h         tests/core/test_raft_flowmq_peer_service.c         tests/core/test_raft_cnet_peer.c         tests/core/test_raft_transport.c
git commit -m "feat(transport): multiplex raft groups fairly"
~~~

---

### Task 3: Replace whole-snapshot APIs with a streaming source/sink contract (#28)

**Files:**
- Modify: `include/turboraft/raft_snapshot_sender.h`
- Modify: `src/snapshot/raft_snapshot_sender.c`
- Modify: `include/turboraft/raft_snapshot_manager.h`
- Modify: snapshot-manager implementation under `src/`
- Modify: `include/turboraft/raft_service.h`
- Modify: Service snapshot-policy implementation under `src/service/`
- Modify: `include/turboraft/raft_wal_storage.h`
- Modify: `src/storage/raft_wal_storage.c`
- Preserve/extend: `include/turboraft/raft_snapshot_receiver.h`
- Test: `tests/core/test_raft_snapshot_sender.c`
- Test: `tests/core/test_raft_snapshot_receiver.c`
- Test: `tests/core/test_raft_snapshot_manager.c`
- Test: `tests/core/test_raft_snapshot_policy.c`
- Test: `tests/core/test_raft_wal_storage.c`

**Interfaces:**
- Produces: immutable snapshot manifest carrying index, term, configuration, total size, and digest/identity.
- Produces: source `describe/read_at` contract.
- Produces: sink `begin/write_at/commit/abort` contract.
- Preserves: receiver cumulative ACK and digest verification.
- Removes from the primary database-scale path: requirement that full snapshot bytes exist as one `uint8_t *`.

- [ ] **Step 1: Write RED sender test using a synthetic source larger than the working buffer**

Create a deterministic source whose logical size is multiple MiB but whose implementation generates bytes from offset without allocating the whole payload.

Verify sender chunks reconstruct the exact logical stream and never request more than configured chunk size.

- [ ] **Step 2: Write RED WAL/storage test using a file-backed staged snapshot**

Exercise:
- begin staged snapshot,
- write chunks at exact offsets,
- commit,
- reopen/recover metadata,
- verify the recovered snapshot can be streamed without returning a full allocated `snapshot_data` buffer.

Also test abort leaves the prior snapshot authoritative.

- [ ] **Step 3: Run focused snapshot/WAL tests and confirm RED**

~~~powershell
cmake --build --preset win-release-user --target turboraft_snapshot_sender_tests turboraft_snapshot_receiver_tests turboraft_snapshot_manager_tests turboraft_snapshot_policy_tests turboraft_wal_storage_tests
ctest --preset win-release-user -R "turboraft\.(snapshot_sender|snapshot_receiver|snapshot_manager|snapshot_policy|wal_storage)$" --output-on-failure
~~~

- [ ] **Step 4: Define manifest/source/sink public contracts**

Add public types with explicit ownership:
- manifest is copied/immutable for one transfer identity,
- source descriptor/context ownership transfers to TurboRaft after a successful create/begin admission,
- TurboRaft invokes source.release exactly once on completion/reset/destroy (when non-NULL),
- failed create/provider calls retain ownership with the provider; no partially returned source is consumed,
- `read_at` fills caller-owned bounded memory,
- sink staging remains non-authoritative until `commit`,
- `abort` is idempotent for incomplete staging.

The manifest must include the exact Raft snapshot point and application payload identity.

- [ ] **Step 5: Convert SnapshotSender to source-based chunking**

The sender retains only:
- immutable manifest,
- source callback/context,
- bounded in-flight chunk storage/metadata,
- acknowledged/next offsets.

Reconnect/resume resets speculative claims to cumulative ACK without regenerating snapshot identity.

- [ ] **Step 6: Convert SnapshotManager/Service policy to source-based production**

Replace the full snapshot buffer contract with a provider that yields a durable immutable snapshot source for the selected applied boundary.

Service ordering remains:
1. capture exact snapshot point,
2. create/finalize application snapshot source,
3. make snapshot durable,
4. optional derived-journal compaction,
5. compact Core.

No Core compaction occurs before durable snapshot publication.

- [ ] **Step 7: Convert WAL snapshot storage/recovery to streaming/file-backed access**

Keep WAL as Raft metadata fact source, but do not load the entire payload into `tr_raft_wal_recovery_t`.

Recovery returns:
- snapshot identity/metadata,
- a bounded reader/handle or path-owned abstraction,
- retained log suffix.

Compatibility helpers may materialize small snapshots if required, but database-scale primary APIs must not.

- [ ] **Step 8: Preserve receiver atomic staging**

Reuse the existing streaming sink direction:
- validate exact offsets,
- hash incrementally,
- call sink commit only after full digest validation,
- abort on reset/protocol/install error.

- [ ] **Step 9: Add a large logical snapshot regression**

Use a generated logical snapshot larger than the old 64/256 MiB whole-payload assumptions without allocating that logical size. Verify memory retained by sender/manager remains bounded by configured chunk/window plus metadata.

- [ ] **Step 10: Run all snapshot/storage regressions**

~~~powershell
cmake --build --preset win-release-user
ctest --preset win-release-user -R "turboraft\.(snapshot_.*|wal_storage|recovery|service)$" --output-on-failure
~~~

Expected: PASS.

- [ ] **Step 11: Commit**

~~~bash
git add include/turboraft/raft_snapshot_sender.h         include/turboraft/raft_snapshot_manager.h         include/turboraft/raft_snapshot_receiver.h         include/turboraft/raft_service.h         include/turboraft/raft_wal_storage.h         src/snapshot         src/service         src/storage/raft_wal_storage.c         tests/core/test_raft_snapshot_sender.c         tests/core/test_raft_snapshot_receiver.c         tests/core/test_raft_snapshot_manager.c         tests/core/test_raft_snapshot_policy.c         tests/core/test_raft_wal_storage.c
git commit -m "feat(snapshot): stream database-scale raft snapshots"
~~~

---

### Task 4: Add multi-process Multi-Group acceptance/chaos coverage

**Files:**
- Modify: `tests/chaos/raft_multiprocess_protocol.h`
- Modify: `tests/chaos/raft_multiprocess_node.c`
- Modify: `tests/chaos/test_raft_multiprocess_chaos.c`
- Modify: `docs/CHAOS_TESTING.md`
- Modify: `tests/core/CMakeLists.txt` only if a distinct acceptance target is needed.

**Interfaces:**
- Consumes: Tasks 1-3.
- Produces: reproducible acceptance evidence for R0 issues #26, #27, #28.

- [x] **Step 1: Extend the child-node harness to host three independent groups**

Each child process owns three independent:
- `tr_raft_service_t`,
- WAL prefixes,
- application-state fixtures.

The parent network routes frames by `group_id`.

- [x] **Step 2: Add deterministic leader-diversity setup**

Drive elections so the stable state demonstrates:

~~~text
group 100 leader = node 1
group 101 leader = node 2
group 102 leader = node 3
~~~

Assert each group commits and applies independently.

- [ ] **Step 3: Add group-isolation fault scripts**

Focused evidence completed:
- [x] block group 100 traffic while group 101/102 continue and converge;
- [x] perform one group's WAL backup handoff while sibling groups stay healthy;
- [x] kill/restart a physical host and recover all independent group WALs;
- [x] unknown/stopped-group rejection followed by valid traffic is covered by #27 transport isolation;
- [x] stream a G100 snapshot through the shared per-peer scheduler while G101/G102 protocol traffic is scheduled between snapshot chunks and the receiver commits the verified snapshot;
- [ ] run the authoritative production FlowMQ/mTLS snapshot-catch-up case while sibling groups continue commits.

- [ ] **Step 4: Run the targeted acceptance test**

Focused three-process acceptance is green:

~~~text
run 35501109360
head c53bd888d088145f1585c4b2aa2fe509733fef9d
result SUCCESS
~~~

It runs real Core/Service/WAL in three OS processes with three groups per
process; the focused executable substitutes only a test wire shim. The
authoritative project gate remains:

~~~powershell
cmake --build --preset win-release-user
ctest --preset win-release-user -R "turboraft\.multiprocess_chaos" --output-on-failure
~~~

Expected: PASS using the production wire codec and complete dependency set.

- [ ] **Step 5: Run full Release suite**

~~~powershell
ctest --preset win-release-user --output-on-failure
~~~

Expected: zero failures.

- [ ] **Step 6: Commit**

~~~bash
git add tests/chaos docs/CHAOS_TESTING.md tests/core/CMakeLists.txt
git commit -m "test: verify multi-group raft isolation"
~~~

---

## Completion Gate

R0 is ready for review only when:

- #26, #27, and #28 acceptance criteria are satisfied.
- The multi-process acceptance test proves three groups share physical peer links safely.
- Existing single-group public behavior is covered by regression tests.
- Full Release CTest is green.
- No production API introduces shard/database/provider semantics.
- PR #24 design claims match the implemented public contracts.
