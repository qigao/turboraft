# CFlow SQLite/Orm Recovery Fixture Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove that an application can atomically persist CFlow state, exact Raft entry identity, and its applied marker through Orm::C/SQLite, then resolve both rolled-back and commit-unknown settlements through the explicit ApplyRuntime reconciliation API.

**Architecture:** The fixture is test-only and application-owned. TurboRaft Core and `TurboRaft::CFlowStateMachine` remain independent of TurboDB/Orm. A CMeta-described Event carries the decoded command plus Raft identity into a dedicated CFlow Statechart; the application host transaction writes its candidate state and journal marker in one SQLite transaction. Recovery closes and reopens SQLite, rebuilds the CFlow instance from the durable state, proves the exact journal identity, and only then calls `tr_raft_apply_runtime_reconcile()`.

**Tech Stack:** C11, TurboRaft Core/ApplyRuntime/CFlowStateMachine, Salts CFlow/CMeta, Orm::C SQLite driver, TinyTest, CMake Presets.

**Spec:** GitHub issue `#22`, application-owned durable state-machine acceptance coverage.

## Global Constraints

- No Orm/TurboDB include, symbol, or target enters a TurboRaft production library.
- `TURBODB_ROOT` is resolved only from the active `CMakeUserPresets.json` environment and package lookup is `NO_DEFAULT_PATH`.
- The fixture uses synchronous SQLite only as bounded conformance coverage; CFlow host callbacks remain unsuitable for unbounded/blocking production I/O.
- The database owns one durable state row and an exact per-index journal; neither is independently advanced.
- Every failed transaction is explicitly rolled back; no fallback or implicit repair is permitted.

---

### Task 1: Add the real Orm/SQLite acceptance test

**Files:**
- Create: `tests/core/test_raft_cflow_orm_sqlite_recovery.c`
- Modify: `tests/core/CMakeLists.txt`

**Interfaces:**
- Consumes: `Orm::C`, CFlow host transaction V5, `tr_raft_apply_runtime_reconcile()`.
- Produces: CTest target `test_raft_cflow_orm_sqlite_recovery` only when `BUILD_TESTS` is enabled.

- [x] **Step 1: Write and run the failing integration test**

  Build a file-backed SQLite fixture whose first apply fails after the journal write but before commit. Assert that neither the journal, state, nor marker advances. The initial failure must demonstrate that a non-transactional implementation would leak a partial write.

- [x] **Step 2: Implement one atomic application transaction**

  Begin `ORM_ISOLATION_SERIALIZABLE`, validate the current applied index, insert exact `(index, term, command_id, payload)`, update the state and marker, and commit once. Any error rolls back and returns `FATAL` to CFlow.

- [x] **Step 3: Recover a rolled-back attempt as PENDING**

  Destroy and rebuild the failed CFlow instance at the durable state, verify the durable marker did not advance, reconcile the exact retained entry as `PENDING`, and let ApplyRuntime retry the same token.

- [x] **Step 4: Recover a committed-but-unsettled attempt as APPLIED**

  Inject a one-shot failure after SQLite commit. Reopen the database, load the durable state and journal identity, compare every identity byte with the retained entry, rebuild CFlow at that state, and reconcile `APPLIED` without replay.

### Task 2: Keep dependency and build boundaries explicit

**Files:**
- Modify: `tests/core/CMakeLists.txt`
- Verify: `CMakeLists.txt`, `src/`, `include/`

- [x] **Step 1: Add fail-fast test-only Orm package discovery**

  Under `BUILD_TESTS`, require a valid `TURBODB_ROOT`, call `find_package(Orm CONFIG REQUIRED ... NO_DEFAULT_PATH)`, verify `Orm::C`, and link it only to the new test.

- [x] **Step 2: Run focused Release and Debug/ASan verification**

  Reconfigure through `win-release-user` and `win-dev-user`, build the new target, and run the focused CTest in both profiles using the preset runtime `PATH`.

- [x] **Step 3: Run regression and dependency checks**

  Run Release full CTest, `git diff --check`, confirm production Core/CFlow sources contain no Orm/TurboDB reference, sync CodeGraph, and inspect status. Do not commit, push, or merge without a separate instruction.
