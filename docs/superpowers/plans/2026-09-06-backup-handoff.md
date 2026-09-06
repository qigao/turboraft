# Quiesced Service/WAL Backup Handoff Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Allow one owner thread to freeze a quiescent TurboRaft Service, close and copy its caller-owned WAL files, then bind a reopened WAL adapter and resume safely.

**Architecture:** The Service gains a small explicit `backup_prepared` lifecycle state; it is not a new backup subsystem and owns neither file handles nor copied files. `tr_raft_service_prepare_backup()` checks Core and Service-owned pending state before freezing all mutating operations. `tr_raft_service_resume_backup()` validates and swaps only the borrowed storage adapter by constructing a replacement Runtime against the unchanged Core.

**Tech Stack:** C11, TurboRaft Service/Runtime/WAL storage, Salts error codes, TinyTest, CMake user presets.

**Spec:** https://github.com/qigao/turboraft/issues/10

## Global Constraints

- Single owner thread; no internal worker, queue, or implicit progress loop.
- WAL storage and its files remain caller-owned; Service never closes, copies, or reopens them.
- Fail fast with `SALTS_EBUSY` for non-quiescent state and preserve `backup_prepared` after an unsuccessful resume.
- No automatic retry or fallback for storage open/bind failures.
- Use `win-release-user` CMake user presets; do not change presets or add DLL copy rules.

---

### Task 1: Specify and test the backup lifecycle

**Files:**

- Modify: `include/turboraft/raft_service.h`
- Modify: `tests/core/test_raft_service.c`
- Modify: `tests/core/test_raft_snapshot_policy.c`

**Interfaces:**

- Produces `int tr_raft_service_prepare_backup(tr_raft_service_t *service)`.
- Produces `int tr_raft_service_resume_backup(tr_raft_service_t *service, const tr_raft_storage_t *storage)`.
- Extends `tr_raft_service_status_t` with `backup_prepared` and `journal_compaction_pending` as read-only lifecycle observation.

- [x] **Step 1: Write failing Service lifecycle tests**

Add a TinyTest case that creates the existing in-memory Service fixture, calls `tr_raft_service_prepare_backup(service)`, asserts `status.backup_prepared`, then verifies `tick`, `step`, `propose`, `poll`, `trigger_snapshot`, `snapshot_completed`, `reload`, and `take_read_state` return `SALTS_EBUSY`. Call `tr_raft_service_resume_backup(service, &replacement_storage)`, assert `backup_prepared == false`, and prove a valid `tick` is accepted again.

Add a second case that prepares successfully, calls `resume_backup` with a zeroed `tr_raft_storage_t`, expects `SALTS_EINVAL`, then verifies the Service remains prepared and rejects a subsequent `tick` with `SALTS_EBUSY`.

- [x] **Step 2: Write failing quiescence tests**

Extend the snapshot policy fixture with a journal compactor returning `SALTS_EIO`; after `poll` leaves its durable snapshot pending, assert `tr_raft_service_prepare_backup(service) == SALTS_EBUSY` and `status.journal_compaction_pending == true`. Add a Service test that leaves a completed read state available and asserts `prepare_backup` returns `SALTS_EBUSY` until `tr_raft_service_take_read_state` consumes it.

- [x] **Step 3: Run the focused tests and observe missing-interface failure**

Run: `cmake --fresh --preset win-release-user`, `cmake --build --preset win-release-user --target turboraft_service_tests turboraft_snapshot_policy_tests`, and `ctest --preset win-release-user -R "^turboraft\\.(service|snapshot_policy)$" --output-on-failure`.

Expected: the new test target fails to link because the two backup lifecycle symbols and status fields do not yet exist.

### Task 2: Implement the minimal Service state transition

**Files:**

- Modify: `include/turboraft/raft_service.h`
- Modify: `src/service/raft_service.c`

**Interfaces:**

- Consumes the tests and lifecycle contract from Task 1.
- `prepare_backup` must require: Service is not faulted, Core has no outstanding Ready, no journal compaction is pending, and no unread read-state is retained.
- `resume_backup` must retain the old runtime and prepared state unless a fully initialized replacement Runtime has been created using the supplied storage adapter.

- [x] **Step 1: Add the public declarations and status fields**

Document that prepare freezes only Service mutation, while the caller subsequently closes/copies/reopens WAL. Document that resume borrows the already-open replacement storage adapter and only clears the freeze after Runtime initialization succeeds.

- [x] **Step 2: Add `backup_prepared` to the opaque Service state and one shared admission helper**

The helper returns `SALTS_EPROTO` for a faulted Service and `SALTS_EBUSY` while backup-prepared. Use it from every mutating entry point named in Task 1; leave status, configuration, progress, and operation-status queries readable.

- [x] **Step 3: Implement `prepare_backup`**

Read Core status. Return `SALTS_EBUSY` when Ready is outstanding, a journal compaction retry is pending, or an unread read state is retained. On success set `backup_prepared = true` without changing Core, Runtime, storage, transport, state machine, WAL, or snapshot bytes.

- [x] **Step 4: Implement `resume_backup` transactionally**

Reject a null storage or a Service that is not prepared. Copy the candidate storage into a local replacement Service configuration, call the existing Runtime initialization helper against the current Core, and only then replace `service->storage` and `service->runtime` and clear `backup_prepared`. Failure leaves the original storage/runtime and prepared state untouched.

- [x] **Step 5: Run focused tests and commit**

Run the Task 1 CTest filter. Commit with `feat(backup): add quiesced WAL handoff`.

### Task 3: Document the operational contract and verify regressions

**Files:**

- Modify: `docs/RECOVERY.md`
- Modify: `docs/SNAPSHOT_DESIGN.md`

- [x] **Step 1: Document the owner-loop sequence**

Document: stop ingress; consume any readable read state and pending derived-journal compaction; call prepare; close the caller-owned WAL storage; copy the exact WAL/snapshot prefix; reopen and bind a new adapter; call resume; restart ingress. State that errors leave the Service prepared and require an explicit successful resume or shutdown/recovery.

- [x] **Step 2: Document state ownership and no-fallback behavior**

Record that WAL/snapshot remain the only durable fact source, the Service owns only its in-memory freeze bit, and copying while unprepared or resuming with an invalid adapter is rejected rather than repaired.

- [x] **Step 3: Run regression verification**

Run `cmake --build --preset win-release-user`, then the full CTest preset. If the multi-process chaos test exceeds the command wait window, run it separately to completion and retain its terminal result. Run `git diff --check` before review.

- [x] **Step 4: Commit documentation and request review**

Commit with `docs(recovery): document quiesced backup handoff`, then obtain a read-only review before creating a pull request.

## Self-Review

- Spec coverage: Tasks 1–2 define the quiesce/rebind API, block every mutable Service transition, preserve pending state on failure, and verify read-state and snapshot-pending boundaries. Task 3 documents the caller-owned storage sequence and rollback condition.
- Placeholder scan: no task contains TBD/FIXME or an unspecified test/action.
- Type consistency: both tasks use the exact `tr_raft_service_prepare_backup` and `tr_raft_service_resume_backup` declarations and `tr_raft_storage_t` replacement contract.

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-09-06-backup-handoff.md`. Execution will proceed inline with the required task-by-task review checkpoints because this session is not delegating implementation work.
