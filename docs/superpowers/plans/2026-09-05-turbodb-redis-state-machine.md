# TurboDB Redis State Machine Implementation Plan

> **For implementation agents:** execute this plan task-by-task, preserving the
> test order and review points below.

**Goal:** provide an opt-in TurboRaft state machine that atomically persists
each committed Raft batch to a Redis command journal and Stream outbox through
TurboDB.

**Architecture:** keep TurboRaft consensus targets independent of TurboDB.
Extend TurboDB first with one bounded Lua batch transaction that owns journal,
identity, metadata, and Stream writes. The TurboRaft leaf adapter translates
each runtime apply_batch call into that transaction and propagates every
uncertain or inconsistent result as an error.

**Tech Stack:** C11, CMake presets, TurboRaft Runtime state machine API,
TurboDB Redis Lua client, TinyTest, Redis Cluster-compatible hash tags.

---

## Task 1: Add TurboDB atomic batch journal primitive

**Files:**
- Create: redis/redis_lua_apply_batch.h
- Create: redis/redis_lua_apply_batch.c
- Modify: redis/CMakeLists.txt
- Modify: public/export headers and package installation lists
- Create: redis/tests/test_redis_lua_apply_batch.c

1. Add a bounded descriptor for contiguous records containing index, term,
   command ID, and payload views. Define APPLIED, REPLAYED, GAP, CONFLICT, and
   COMMIT_UNKNOWN result states.
2. Implement one EVAL script that validates all inputs, verifies replay
   identities, writes the full batch journal and identity hashes, emits the
   Stream records, and advances metadata only inside the same script.
3. Compare and increment all uint64 identifiers as canonical decimal strings;
   do not use Lua number conversion.
4. Add real-Redis tests for success, full replay, gap, conflict, atomicity
   after injected script failure, same-tag validation, and the UINT64_MAX
   boundary.
5. Build and run TurboDB Redis tests. Install the resulting current package to
   the selected TURBODB_ROOT.

## Task 2: Add TurboRaft optional Redis state-machine adapter

**Files:**
- Create: include/turboraft/turbodb_redis_state_machine.h
- Create: src/integrations/turbodb_redis_state_machine.c
- Modify: CMakeLists.txt
- Modify: CMakeUserPresets.json
- Modify: tests/core/CMakeLists.txt
- Create: tests/core/test_turbodb_redis_state_machine.c

1. Define a small factory/configuration API with explicit Redis key names,
   maximum batch size, and ownership/destruction semantics.
2. Implement the Runtime apply_batch callback by translating a full contiguous
   Raft batch into one TurboDB batch call. Reject configuration entries and
   invalid payloads at the adapter boundary.
3. Map only APPLIED and REPLAYED to success. Return a clear error for every
   other result, including COMMIT_UNKNOWN.
4. Define the target only when TURBORAFT_ENABLE_TURBODB_REDIS_STATE_MACHINE is
   enabled. Use direct find_package(TurboDB CONFIG REQUIRED); preserve a
   fail-fast configuration failure if the package is absent.
5. Add TURBODB_ROOT and its bin directory to CMakeUserPresets PATH. Do not add
   DLL copying or compatibility helpers.
6. Test Runtime behavior with a real Redis server: a committed batch creates
   an all-or-nothing journal prefix, replay succeeds, and conflicts fault the
   Runtime without advancing Core.

## Task 3: Document recovery, retention, and TurboFlow handoff

**Files:**
- Modify: docs/architecture/redis-command-journal-state-machine.md
- Modify: README.md only if the optional feature becomes user-facing
- Update: TurboDB issue 13 and TurboFlow issue 13 with implementation status

1. Document key layout, hash-tag constraint, data ownership, error results,
   recovery after COMMIT_UNKNOWN, and snapshot-aware compaction.
2. State that the Redis Stream is an at-least-once derived outbox and cannot
   acknowledge or advance Raft.
3. Record the future TurboFlow connector boundary: group consumption,
   settlement before XACK, duplicate handling, and journal-based repair.
4. Run the closest adapter tests, the full TurboRaft preset tests, and the
   relevant TurboDB Redis tests. Record the exact commands and results in the
   implementation handoff.
