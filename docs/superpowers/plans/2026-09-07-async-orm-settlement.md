# Async ORM Settlement Implementation Plan

> **For Codex:** Execute this plan inline with TDD; repository policy forbids exposing incomplete public APIs.

**Goal:** Keep blocking ORM transactions off the CFlow SerialExecutor while preserving exact-entry recovery through the existing ApplyRuntime reconciliation API.

**Architecture:** A test/application-owned state-machine decorator first delegates admission and settlement to `TurboRaft::CFlowStateMachine`. Once CFlow publishes a completed macrostep, the decorator copies the resulting application state and exact Raft entry into one fixed-capacity slot, then submits a blocking ORM transaction to a dedicated one-worker Salts coroutine executor. ApplyRuntime observes APPLIED only after durable commit; rollback and commit-unknown both surface UNKNOWN and require rebuilding CFlow from durable state before explicit reconciliation.

**Tech Stack:** C11, TurboRaft ApplyRuntime/CFlow adapter, Salts Coroutine Executor, TurboDB ORM, TinyTest, CMake presets.

---

## Protocol

- Data unit: one copied `tr_raft_entry_t`, one copied application state, and its token.
- Fact source: committed Raft WAL; the ORM journal is the durable materialized-state marker used for reconciliation.
- Ownership: the decorator owns the copied slot from successful admission until terminal settlement; the executor borrows that stable slot until task finalization.
- Topology: ApplyRuntime owner -> CFlow SerialExecutor -> ApplyRuntime poll owner -> one persistence worker.
- Capacity/order: exactly one in-flight entry, worker count 1, queue capacity 1, strict log-index order.
- Backpressure/failure: submission failure is explicit UNKNOWN; no unbounded allocation and no implicit retry after an uncertain commit.
- Shutdown: settle or fault ApplyRuntime, stop/wait/destroy the persistence executor, then close/unbind/destroy CFlow.

### Task 1: Add a failing non-blocking boundary test

- [x] Record the required ORM executor identity.
- [x] Assert ORM persistence executes only on its dedicated executor.
- [x] Remove the synchronous host-transaction persistence path.

### Task 2: Compose asynchronous persistence using the existing SPI

- [x] Remove ORM calls from the CFlow host transaction.
- [x] Add the single-slot decorator and bounded one-worker executor to the application fixture.
- [x] Map successful commit to APPLIED and rollback/commit-unknown to UNKNOWN.
- [x] Rebuild CFlow from the durable ORM snapshot before PENDING/APPLIED reconciliation.
- [x] Run focused sanitizer tests.

### Task 3: Document the supported production composition

- [x] Document ownership, ordering, backpressure, shutdown, and recovery requirements.
- [x] State that blocking persistence is forbidden in CFlow host callbacks.
- [x] Keep ORM outside TurboRaft public dependencies.
