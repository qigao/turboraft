# Chaos WAL Backup Handoff Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Verify online WAL prepare/close/reopen/resume on both a leader and a follower while the deterministic three-process Raft chaos scenario remains safe and live.

**Architecture:** Add one internal chaos protocol command. The child node remains the sole owner of its WAL storage: it prepares Service, closes storage, reopens and binds the identical prefix, then resumes Service. The parent schedules deterministic leader and follower handoffs only between command turns, and continues the existing tick, network delivery, safety, and convergence checks. A test-only mode injects reopen failure after close; the child reports the original error and terminates because its Service adapter is no longer usable.

**Tech Stack:** C11, TurboRaft Service/WalStorage, Salts process API, TinyTest, CMake user presets.

**Spec:** https://github.com/qigao/turboraft/issues/12

## Global Constraints

- No Service file ownership or fallback adapter; only the child owner closes/reopens WAL.
- Any prepare, close, open, bind, or resume failure is returned to the parent unchanged. A failure after the original WAL is closed terminates the child owner loop.
- Keep the protocol version unchanged: the command is an additive enum value.
- Execute handoff outside a partially processed command; use the existing command/response turn boundary.

---

### Task 1: Specify the process-level handoff command

**Files:**
- Modify: `tests/chaos/raft_multiprocess_protocol.h`
- Modify: `tests/chaos/raft_multiprocess_node.c`
- Modify: `tests/chaos/test_raft_multiprocess_chaos.c`

**Interfaces:**
- Produces `TR_CHAOS_COMMAND_BACKUP_HANDOFF` with an empty payload for normal handoff and a four-byte test-only failure mode for deterministic fault injection.
- Child command returns the first failure from prepare, close, open, bind, or resume.

- [x] **Step 1: Write the failing runner assertion**

Add a parent helper that sends `TR_CHAOS_COMMAND_BACKUP_HANDOFF`, asserts transport and operation results are `SALTS_OK`, and reuses `tr_chaos_collect_command()` so status tracking and outbound-message collection stay real.

- [x] **Step 2: Verify RED**

Build `turboraft_multiprocess_chaos_tests`; expected compilation failure because the protocol command is absent.

- [x] **Step 3: Implement child-owned WAL handoff**

Store the immutable WAL config in `tr_chaos_node_t`. On the new command: call `tr_raft_service_prepare_backup(node->service)`, close and null `node->storage`, reopen into a local pointer, bind a local `tr_raft_storage_t`, call `tr_raft_service_resume_backup()`, then assign `node->storage` only after resume succeeds. Close the reopened storage on bind/resume failure.

- [x] **Step 4: Verify GREEN**

Run the multiprocess CTest filter after building both the node and runner targets.

### Task 2: Exercise leader and follower handoffs

**Files:**
- Modify: `tests/chaos/test_raft_multiprocess_chaos.c`
- Modify: `docs/CHAOS_TESTING.md`

- [x] **Step 1: Schedule deterministic handoffs**

After leadership has stabilized and before process termination, hand off the current leader at round 12 and one alive non-leader at round 24. Each uses the existing command/response lifecycle, then normal ticks and delivery continue.

- [x] **Step 2: Add observability assertions**

Track leader and follower handoff completion counts in `tr_chaos_run_seed`; fail liveness if either remains zero. The existing safety tracker continues to reject faults, non-monotonic terms/commits, two leaders per term, and divergent applied hashes.

Record an accepted proposal index after the follower handoff. Run a bounded fault-free recovery phase and require all three nodes to reach that target with the same applied index and application hash.

- [x] **Step 3: Document scope and run regression**

Document the exercised owner-loop sequence in `docs/CHAOS_TESTING.md`, then run the focused multiprocess CTest filter and full CTest preset.

## Self-Review

- Spec coverage: Task 1 covers real owner-side WAL lifecycle and fail-fast propagation; Task 2 exercises leader/follower positions with the existing safety and liveness facts.
- Placeholder scan: no unowned TODO/FIXME or unspecified behavior.
- Type consistency: the protocol command has zero payload in parent and child, and the child uses the public Service prepare/resume API from PR #11.

## Execution Handoff

Plan saved for inline execution because the user explicitly requested continuation.
