# Apply Runtime Reconciliation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an explicit, backend-neutral in-process recovery boundary that lets an application resolve the current faulted apply token as exactly `APPLIED` or exactly `PENDING` without replaying storage or transport effects.

**Architecture:** `tr_raft_apply_runtime_t` remains the single owner of one copied committed-entry batch and remembers whether a fault came from a matching terminal `GAP`, `CONFLICT`, or `UNKNOWN` settlement. The application reconciles its own durable fact source and supplies the exact WAL entry plus a proven `APPLIED`/`PENDING` outcome; Runtime compares index, term, command ID, length, and payload before either advancing Core once or retrying that same entry. CFlow/CMeta remain the semantic state-machine layer, and no database type enters Core or Runtime.

**Tech Stack:** C11, TurboRaft Core/ApplyRuntime, Salts error codes, TinyTest, CMake Presets.

**Spec:** GitHub issue `#22` and `docs/DESIGN.md` state-machine settlement boundary.

## Global Constraints

- Preserve the existing synchronous `tr_raft_state_machine_t` ABI and behavior.
- Do not change Raft wire, WAL, or snapshot formats.
- Core and ApplyRuntime must not include or link TurboDB/ORM.
- Only `PENDING` authorizes replay; `APPLIED` authorizes one contiguous Core acknowledgement.
- Invalid identity, invalid state, and unresolved outcomes fail fast without changing Runtime or Core.
- The state-machine attempt must already be terminal before reconciliation; rebuilding CFlow/application state remains application-owned.
- Runtime owns a fixed-capacity copied batch; the reconciliation entry is borrowed only for the call.

---

### Task 1: Lock the explicit reconciliation contract with failing tests

**Files:**
- Modify: `tests/core/test_raft_apply_runtime.c`
- Modify: `include/turboraft/raft_apply_runtime.h`

**Interfaces:**
- Consumes: `tr_raft_apply_runtime_poll()`, `tr_raft_core_status()`, `tr_raft_entry_t`.
- Produces: `tr_raft_apply_runtime_reconcile(tr_raft_apply_runtime_t *, const tr_raft_entry_t *, tr_raft_apply_outcome_t, tr_raft_apply_runtime_result_t *)`.

- [x] **Step 1: Write a failing APPLIED reconciliation test**

  Add a test that obtains a matching `UNKNOWN` settlement for index 1, asserts the fault result retains token 1, calls `tr_raft_apply_runtime_reconcile()` with the exact entry and `APPLIED`, and expects Runtime completion plus Core `applied_index == 1` without a second `try_apply()` call.

- [x] **Step 2: Write a failing PENDING reconciliation test**

  Add a test that resolves the same fault with `PENDING`, expects exactly one retry of token 1, then supplies a normal `APPLIED` settlement and observes completion.

- [x] **Step 3: Write fail-fast identity and eligibility tests**

  Mutate term, command ID, payload length, and payload one at a time and expect `SALTS_EPROTO` with the original fault preserved. Verify a wrong-token settlement/callback failure is not eligible and `GAP`, `CONFLICT`, or `UNKNOWN` cannot be passed as the resolution.

- [x] **Step 4: Run the focused test and observe RED**

  Run `cmake --build --preset win-release-user --target test_raft_apply_runtime` followed by `ctest --preset win-release-user -R test_raft_apply_runtime --output-on-failure`; compilation must fail because the new function is not defined.

### Task 2: Implement the minimal single-token recovery state machine

**Files:**
- Modify: `include/turboraft/raft_apply_runtime.h`
- Modify: `src/runtime/raft_apply_runtime.c`
- Test: `tests/core/test_raft_apply_runtime.c`

**Interfaces:**
- Consumes: the exact Runtime-owned entry at `entry_cursor`, `tr_raft_core_acknowledge_applied_entry()`, and `tr_apply_drive_admission()`.
- Produces: the public additive `tr_raft_apply_runtime_reconcile()` function and a retained recoverable-token diagnostic in `tr_raft_apply_runtime_result_t::in_flight_token`.

- [x] **Step 1: Document the public contract**

  Declare the additive function with borrowed-entry lifetime, exact identity comparison, allowed outcomes, CFlow/application restoration precondition, and failure semantics.

- [x] **Step 2: Record only matching terminal apply faults as recoverable**

  Add private `recovery_pending` state and retain the current index only when a matching settlement is `GAP`, `CONFLICT`, or `UNKNOWN`. Callback errors, wrong tokens, storage/transport faults, and Core errors remain non-recoverable.

- [x] **Step 3: Implement exact identity validation before mutation**

  Compare index, term, command ID, data length, and payload bytes; reject invalid resolution outcomes and all mismatches with no Core advance, retry, or fault clearing.

- [x] **Step 4: Apply the proven decision**

  For `APPLIED`, acknowledge exactly the current Core index, advance the cursor, clear the old fault, and drive the next entry. For `PENDING`, clear the old fault and re-enter admission for exactly the current entry. If subsequent work fails, preserve that new first error.

- [x] **Step 5: Run focused tests and observe GREEN**

  Run `cmake --build --preset win-release-user --target test_raft_apply_runtime` and `ctest --preset win-release-user -R test_raft_apply_runtime --output-on-failure`; all focused tests must pass.

### Task 3: Add integration and recovery documentation

**Files:**
- Modify: `tests/core/test_raft_cflow_state_machine.c`
- Modify: `docs/DESIGN.md`
- Modify: `README.md`

**Interfaces:**
- Consumes: CFlow V5 tagged settlement, adapter rebind lifecycle, and `tr_raft_apply_runtime_reconcile()`.
- Produces: an executable CFlow lost-result recovery example and documented application responsibilities.

- [x] **Step 1: Add a CFlow recovery integration test**

  Drive a terminal failed macrostep to `UNKNOWN`, rebuild or rebind the app-owned CFlow instance to a state already proven durable for that exact entry, reconcile `APPLIED`, and assert Core and copied CMeta state share index 1 semantics.

- [x] **Step 2: Run the integration test**

  Run `cmake --build --preset win-release-user --target test_raft_cflow_state_machine` and `ctest --preset win-release-user -R test_raft_cflow_state_machine --output-on-failure`; the integration test must pass without TurboDB.

- [x] **Step 3: Document ownership and migration**

  Explain that Runtime never queries a database, a caller must reconcile fresh durable identity and restore/rebuild its CFlow state before reporting `APPLIED`, and `PENDING` is the only replay decision. State that process restart from snapshot plus durable applied marker remains the universal fallback.

### Task 4: Verify compatibility and package boundaries

**Files:**
- Verify: `tests/package/main.c`
- Verify: `tests/package/CMakeLists.txt`
- Verify: `CMakeLists.txt`

**Interfaces:**
- Consumes: `TurboRaft::Core` and optional `TurboRaft::CFlowStateMachine` package exports.
- Produces: repeatable evidence that the additive API does not pull CFlow or ORM into Core-only consumers.

- [x] **Step 1: Run Release regression**

  Run `cmake --fresh --preset win-release-user`, `cmake --build --preset win-release-user`, and `ctest --preset win-release-user --output-on-failure` from a VS developer environment.

- [x] **Step 2: Run Debug/ASan focused regression**

  Run `cmake --fresh --preset win-dev-user`, build the two apply targets, and run their two CTest names with `--output-on-failure`.

- [x] **Step 3: Verify install and package consumers**

  Run `cmake --build --preset install-win-release-user`, then configure/build the repository package-consumer tests through their existing preset/CTest path. Verify Core-only linkage has no CFlow/ORM requirement and the optional CFlow consumer links the exported target.

- [x] **Step 4: Run source checks**

  Run `git diff --check`, `codegraph sync -q .`, and inspect `git status --short`; `.codegraph/` and build products must remain untracked/ignored.

- [x] **Step 5: Commit only on explicit integration instruction**

  Stage only the reviewed TurboRaft source, tests, and documentation for this feature. Do not commit, push, or merge until the user explicitly requests that integration action for this batch.
