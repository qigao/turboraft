# ORM Crash Cut-points and Backend Gates Plan

> **For Codex:** Execute after asynchronous settlement is green; keep live backend checks explicit and fail-fast.

**Goal:** Verify durable recovery across process death and make PostgreSQL/TidesDB readiness observable without adding hidden compatibility paths.

**Architecture:** A dedicated helper process runs the same application-owned asynchronous settlement protocol and exits at named boundaries. A TinyTest parent restarts against the same store and verifies the exact `(index, term, command_id, payload)` marker. Embedded backends run locally when the installed TurboDB package exposes them; PostgreSQL remains an explicit opt-in live gate using the existing TurboDB connection contract.

**Tech Stack:** Salts Process, TurboDB ORM, TinyTest, CTest.

---

### Task 1: Add deterministic process cut-points

- [x] Cover process death inside the transaction before commit and after commit before ApplyRuntime acknowledgement.
- [x] Reopen the database in a fresh process and classify the exact entry as PENDING or APPLIED.
- [x] Verify complete identity instead of inferring success from index alone.

### Task 2: Add backend capability gates

- [x] Inspect installed TidesDB support rather than assuming driver availability.
- [x] Reuse TurboDB's explicit PostgreSQL live connection contract; never print credentials.
- [x] Treat an unavailable requested backend as a failed product gate, not a pass.

### Task 3: Verify and report external limits

- [x] Run focused Release/Debug sanitizer tests and the Release regression suite.
- [ ] Run TidesDB locally after installing an `ORM_WITH_TIDESDB=ON` TurboDB profile.
- [x] Report PostgreSQL as not executed when its opt-in connection is absent.
