# FlowMQ mTLS Loopback Integration Test Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove the TurboRaft FlowMQ adapter delivers real Raft frames over a mutually authenticated loopback connection and rejects a peer without a client certificate.

**Architecture:** Keep production APIs and ownership unchanged. Extend the existing FlowMQ TinyTest target with two live caller-driven peer services; node 1 sends through an mTLS DEALER to node 2's TLS ROUTER while both services are advanced by bounded `step()` loops. A CNet probe without a client certificate provides an explicit terminal TLS observation for the negative case because FlowMQ deliberately maps connection failure to reconnect state. Test-only helpers select loopback ports through FlowMQ's ephemeral-bind API and build explicit protocol, TLS, and capacity configuration.

**Tech Stack:** C11, TurboRaft FlowMQ adapter and wire transport, FlowMQ and CNet socket APIs, Salts TinyTest/process utilities, CMake user presets.

**Spec:** https://github.com/qigao/turboraft/issues/14

## Global Constraints

- Do not change the public TurboRaft or FlowMQ API.
- Do not add threads, hidden pollers, unbounded queues, plaintext fallback, or retry-on-configuration-error behavior.
- Use the existing `tests/fixtures` CA, node 1 client certificate, and node 2 server certificate only for loopback tests.
- All progress loops have a named hard iteration limit and fail with observable test assertions when the limit is reached.
- Remove the FlowMQ test target's Windows DLL copy command; runtime DLL resolution comes only from the active user preset `PATH`.

---

### Task 1: Configure the live test boundary

**Files:**
- Modify: `tests/core/CMakeLists.txt`
- Modify: `tests/core/test_raft_flowmq_peer_service.c`

**Interfaces:**
- Consumes: `TURBORAFT_TEST_FIXTURE_DIR` as a target-private absolute source-tree path.
- Produces: test-only `reserve_loopback_endpoint()`, `protocol_config(node_id)`, `handshake_result(local_id, peer_id)`, and message-capture helpers.

- [x] **Step 1: Remove copied first-party runtime artifacts**

Delete only the existing `POST_BUILD copy_if_different` block from `turboraft_flowmq_peer_service_tests`. Add one target-private compile definition for `TURBORAFT_TEST_FIXTURE_DIR`; do not add another CMake helper or compatibility branch.

- [x] **Step 2: Remove stale local copies and verify preset PATH**

Delete the exact copied artifacts from `build/Msvc-Release/bin` after confirming their paths: `salts.dll`, `data_bind.dll`, `salts_cnet.dll`, `flowmq.dll`, and `json_parser.lib`. Reconfigure with `cmake --fresh --preset win-release-user`, build the existing FlowMQ target, and run its existing CTest. Expected: 2 tests pass using DLLs from the Release dependency roots in preset `PATH`.

- [x] **Step 3: Add bounded test-only setup helpers**

Use a temporary FlowMQ `PAIR` socket bound to `tcp://127.0.0.1:0`, query its exact address through `flowmq_last_endpoint()`, close it, and use the selected address immediately. Convert only the node 2 listener scheme from `tcp` to `tls`. Helpers return Salts errors and have one cleanup path; tests own all service lifetimes.

### Task 2: Prove authenticated delivery

**Files:**
- Modify: `tests/core/test_raft_flowmq_peer_service.c`

**Interfaces:**
- Consumes: two reserved endpoints and fixture paths for `ca.pem`, `node1-cert.pem`, `node1-key.pem`, `node2-cert.pem`, and `node2-key.pem`.
- Produces: one TinyTest case named `delivers one Raft frame over mutual TLS`.

- [x] **Step 1: Write the live delivery test**

Configure node 2's TLS ROUTER with its server certificate and `require_client_certificate = 1`. Configure node 1's peer TLS settings with the CA, node 1 client certificate/key, and literal server name `node-2.mesh`. Enqueue a literal heartbeat with `from = 1`, `to = 2`, and `term = 7`.

- [x] **Step 2: Run the focused test against current production code**

Build `turboraft_flowmq_peer_service_tests`, then run its TinyTest filter. If the live path fails, preserve the failing output and diagnose the production boundary before changing it; do not loosen TLS or identity validation.

- [x] **Step 3: Assert real boundary effects**

Drive both services for at most `TR_FLOWMQ_TEST_PROGRESS_LIMIT` iterations. Require exactly one node 2 callback with the literal message fields, node 1 `frames_sent >= 1`, node 2 `frames_received >= 1`, and both `last_error == SALTS_OK`. Continue a small bounded drain window and assert the callback count remains one.

### Task 3: Prove client-certificate enforcement

**Files:**
- Modify: `tests/core/test_raft_flowmq_peer_service.c`
- Modify: `docs/FLOWMQ_PEER_TRANSPORT.md`

**Interfaces:**
- Produces: one TinyTest case named `rejects a peer without a client certificate`.

- [x] **Step 1: Write the negative mTLS test**

Reuse the same real TLS ROUTER and drive a CNet TLS probe without a client certificate while retaining the trusted CA and server name. Also start node 1 without its client certificate and enqueue the same literal heartbeat; neither path may reach node 2's Raft callback.

- [x] **Step 2: Verify the test detects authentication failure**

Require the CNet probe's explicit `SALTS_ECONNABORTED` TLS read failure before the hard limit and require node 2's callback count to remain zero. FlowMQ does not expose reconnecting authentication failures through its public poll API, so a bounded non-delivery observation is not treated as a peer error. A test mutation that disables `require_client_certificate` must fail.

- [x] **Step 3: Document and verify**

Document the live mTLS coverage and fixture roles. Run the two focused TinyTest filters, the FlowMQ CTest target, the full Release build, all CTest tests, and `git diff --check`.

## Self-Review

- Spec coverage: Tasks 1-3 cover live socket delivery, exact identity/wire dispatch, certificate enforcement, bounded progress, status evidence, cleanup, preset PATH, and full regression.
- Placeholder scan: every failure path either returns an existing Salts error or becomes a concrete TinyTest assertion; there are no deferred code placeholders.
- Type consistency: the plan uses existing `tr_raft_flowmq_peer_service_config_t`, `tr_raft_flowmq_tls_config_t`, `tr_raft_handshake_result_t`, and `tr_raft_flowmq_peer_service_step_result_t` without extending public structs.

## Execution Handoff

Plan saved for inline execution because the user explicitly requested continuation and this session is not delegating implementation work.
