# CFlow Snapshot Replay Conformance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove that an app-owned in-memory CMeta state checkpoint plus a TurboRaft snapshot boundary and committed suffix reconstruct the same CFlow state as uninterrupted execution.

**Architecture:** The test owns CFlow definition, executor, instance, copied integer state, and restoration. TurboRaft contributes only the exact `tr_raft_snapshot_point_t`, restored Core base/commit/applied indexes, and ordered suffix delivery through ApplyRuntime; no new production API or database dependency is introduced.

**Tech Stack:** C11, TurboRaft Core/ApplyRuntime/CFlowStateMachine, Salts CFlow/CMeta, TinyTest, CMake Presets.

**Spec:** GitHub issue `#22`, acceptance item “In-memory snapshot/replay conformance passes without any database dependency.”

## Global Constraints

- State bytes and serialization remain application-owned.
- Snapshot point index, term, and configuration are the Core-owned boundary.
- Restored Core starts with `initial_applied_index == snapshot.index` and replays only the contiguous suffix.
- The test uses fixed capacities and no TurboDB/ORM symbols.
- CFlow instance and adapter shutdown follow the documented owner order.

---

### Task 1: Add executable conformance coverage

**Files:**
- Create: `tests/core/test_raft_cflow_snapshot_replay.c`
- Modify: `tests/core/CMakeLists.txt`

**Interfaces:**
- Consumes: `tr_raft_core_snapshot_point()`, restored `tr_raft_core_config_t`, `tr_raft_apply_runtime_*()`, CFlow Statechart state copy/init.
- Produces: CTest target `test_raft_cflow_snapshot_replay`.

- [x] **Step 1: Build an app-owned CFlow fixture**

  Define one deterministic integer state machine whose command Events add literal payloads, with bounded one-entry mailboxes and a serial executor. Its decoder borrows the entry only during admission; CFlow owns the accepted copy.

- [x] **Step 2: Execute and capture the checkpoint**

  Apply entries `(1,+7)` and `(2,+11)`, assert state `18`, copy that state as the app checkpoint, and capture Core snapshot point `(index=2, term=1)`.

- [x] **Step 3: Restore and replay only the suffix**

  Recreate CFlow with initial state `18`, recreate Core at the snapshot point with only entry `(3,+13)` as its retained suffix and applied marker `2`, drive ApplyRuntime, and assert state and Core applied index are `31` and `3`.

- [x] **Step 4: Compare uninterrupted execution**

  Independently apply all three entries from state `0` and assert its final CMeta-semantic state equals the restored result.

- [x] **Step 5: Register and run the focused test**

  Use `cmake_add_test()` with `TurboRaft::CFlowStateMachine` and `Salts::TinyTest`; run the Release and Debug/ASan target plus CTest name and require zero failures.

### Task 2: Verify the new acceptance test does not widen dependencies

**Files:**
- Verify: `tests/core/test_raft_cflow_snapshot_replay.c`
- Verify: `tests/core/CMakeLists.txt`

**Interfaces:**
- Consumes: existing optional CFlow target only.
- Produces: evidence for issue `#22` without Core/ORM coupling.

- [x] **Step 1: Run Release full regression**

  Build through `win-release-user` and run all CTest tests with `--output-on-failure`.

- [x] **Step 2: Run source and dependency checks**

  Run `git diff --check`, search the new fixture for TurboDB/ORM/Redis references, sync CodeGraph, and inspect `git status --short`.

- [x] **Step 3: Preserve the branch for explicit integration**

  Do not commit, push, merge, or remove the isolated worktree until the user gives the corresponding instruction.
