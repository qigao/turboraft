# RPC Mutation Completion and Audit Design

Status: accepted implementation design.

## 1. Background

The control plane currently executes commands on the single Service owner loop,
but an accepted Raft proposal is not necessarily committed. Reporting command
acceptance as mutation completion would violate the Phase 7 requirement that a
successful mutation identify its committed term and index. Holding an HTTP
request open until quorum completion would also couple consensus progress to an
HTTP connection and complicate disconnect and shutdown behavior.

This design adds an operation receipt and a status query without introducing a
second writable operation registry. The Raft log, commit index, applied index,
and durable snapshot boundary remain the only facts used to determine progress.

## 2. Requirements

- Preserve existing camelCase RPC methods and their response compatibility.
- New intent-level mutation methods return an explicit acceptance receipt.
- A client can distinguish `PENDING`, `COMMITTED`, `APPLIED`, `LOST`, and
  `EXPIRED`.
- A committed result contains the term and index of the accepted log entry.
- HTTP disconnect does not cancel or roll back an accepted Raft entry.
- Service shutdown does not leave heap-owned pending control-plane operations.
- Operation queries and mutation submissions are bounded by existing owner-loop
  and Raft proposal limits.
- Audit events contain identifiers and outcomes but never credentials,
  certificates, command bytes, application payloads, or snapshot bytes.

## 3. Candidate comparison

### 3.1 Block the HTTP handler until commit

This gives one synchronous result, but consumes a request and owner-bridge slot
for an unbounded quorum delay. Client disconnect and service shutdown need
additional cancellation state. This option is rejected.

### 3.2 Maintain a control-plane operation registry

A bounded registry can assign opaque operation identifiers, but duplicates
facts already held by the log and requires expiration, restart recovery, leader
handoff, synchronization, and persistence rules. It can incorrectly disagree
with Raft after overwrite or compaction. This option is rejected as the primary
state source.

### 3.3 Use a Raft receipt

The selected receipt contains the accepted entry term and index plus an
operation kind. `raft.operation.status` evaluates that receipt against a
read-only Core view on the owner loop. No control-plane mutable operation state
is required.

## 4. Public protocol

An accepted log mutation returns:

```json
{
  "state": "PENDING",
  "kind": "PROPOSAL",
  "term": 12,
  "index": 481
}
```

`raft.operation.status` accepts positive `term` and `index` values and returns:

```json
{
  "state": "COMMITTED",
  "term": 12,
  "index": 481,
  "commit_index": 486,
  "applied_index": 480
}
```

States have the following meanings:

- `PENDING`: the matching entry is retained but `commit_index < index`.
- `COMMITTED`: the matching entry is committed but not yet applied.
- `APPLIED`: the matching entry is committed and `applied_index >= index`.
- `LOST`: the retained entry at `index` has a different term, or the active log
  ended before the receipt after leadership or recovery changed the suffix.
- `EXPIRED`: `index` is older than the durable compacted log base, so its term
  can no longer be verified without a separate operation ledger.

Malformed values and indexes that were never observable locally fail with a
stable invalid-operation error instead of returning a fabricated state.

An index exactly equal to the compacted log base is `APPLIED` only when its term
matches the retained base term. An older index is `EXPIRED`: compaction proves
that the prefix was applied, but no longer proves the term identity supplied by
the caller. TurboRaft therefore refuses to fabricate receipt identity.

The existing `raft.propose` method remains available. A new
`raft.propose_async` method uses the receipt response. The already additive
`raft.member.add_learner`, `raft.member.promote`, and `raft.member.remove`
methods adopt receipt responses. `raft.snapshot.trigger` completes only after
snapshot storage and core compaction succeed, so it returns the completed
snapshot term/index directly rather than a pending receipt.

Leadership transfer does not append a log entry. Its accepted response reports
the observed term, current leader, and requested target; clients confirm the
result through `raft.status`.

## 5. Core and Service boundaries

Core exposes a read-only operation probe with this data:

- requested term and index;
- current log-base, last-log, commit, and applied indexes;
- retained entry term when the index is not compacted;
- derived operation state.

The probe does not tick, send messages, apply entries, or mutate membership.
Service forwards the probe, and the control plane invokes it only through the
Service owner loop. HTTP worker threads never read mutable Core state directly.

Time complexity is `O(1)` for retained entries because log lookup is indexed.
Space complexity is `O(1)` because no receipt registry is retained.

## 6. Error mapping and leader hints

The RPC adapter owns a stable mapping from TurboRaft errors to JSON-RPC errors.
The error payload contains a stable symbolic category, the local node, and the
known leader when relevant. Internal storage, TLS, and platform error text is
not copied into public responses.

`NOT_LEADER` includes `leader_id` when the local service has a nonzero current
leader. It is a hint, not an authorization to bypass virtual-address routing or
mTLS peer identity checks.

When no leader is known, the stable hint is `leader_id=0`. The control plane
never returns a real peer IP address or port in this error.

## 7. Audit boundary

The control plane gains an optional instance-scoped audit sink. It receives a
fixed-size event after authorization and again when the command result is known.
The event contains:

- event version and operation kind;
- method identifier;
- local node and target node when applicable;
- accepted or committed term/index when available;
- authorization outcome and command outcome;
- a caller identity fingerprint supplied by the embedding application.

The event does not contain bearer tokens, raw headers, certificate material,
command data, application payloads, or snapshot data. Sink failure cannot turn
a failed command into success. Deployments that require mandatory audit use a
fail-closed configuration; optional diagnostics use best-effort audit with an
explicit dropped-event counter.

The sink callback runs outside Core mutation and storage critical sections. It
must not call back into the same control-plane instance.

## 8. Limits and shutdown

- Owner-loop command capacity bounds concurrent RPC-to-Service work.
- Core pending proposal bytes and entry limits bound accepted mutations.
- RPC request, batch, response, and body limits remain owned by TurboHTTP/Iris.
  TurboRaft configures 16 KiB requests, 8 batch items, and 64 KiB serialized
  non-streaming responses. Oversized responses fail as a whole and are never
  truncated into invalid JSON.
- `raft.operation.status` allocates no persistent state and therefore needs no
  operation-expiration fallback.
- Once accepted, an entry remains governed by Raft if the HTTP client
  disconnects.
- During shutdown, new owner-loop commands fail fast; already accepted entries
  remain recoverable from durable storage after restart.

The control plane accepts a borrowed `iris_app_t` through
`tr_raft_control_plane_config_t.app`. Production applications create and own an
explicit app, stop its listener, destroy the control plane, and then destroy the
app. A null field preserves the legacy default-app integration for source
compatibility; migrating or rolling back changes no route or RPC protocol.
Production listeners use TLS with both `h2` and `http/1.1` in the ALPN list.
The same app therefore serves both transports; there are no H1-only control
routes. The read-only `/raft/ws` route likewise maps H1 RFC 6455 and H2 RFC 8441
to one handler and never accepts mutation commands.

## 9. HTMX behavior

Mutation forms render the acceptance receipt and poll
`raft.operation.status`. Polling stops on `APPLIED`, `LOST`, authorization
failure, or an explicit UI timeout. A browser timeout does not alter consensus
state. The page renders text content rather than injecting untrusted command or
error strings as HTML.

## 10. Compatibility, migration, and rollback

- Existing camelCase method names and legacy response shapes remain available.
- New clients should use snake/dotted methods and operation receipts.
- No storage format, wire protocol, or Raft log entry format changes.
- Mixed-version peer clusters are unaffected because this is an administrative
  HTTP protocol change, not a peer protocol change.
- Rollback removes the additive methods and UI polling while leaving all Raft
  data valid. Receipts are client-side values and require no cleanup.

## 11. Verification

Required deterministic tests:

- receipt is `PENDING`, then `COMMITTED`, then `APPLIED` as indexes advance;
- overwritten uncommitted receipt becomes `LOST`;
- exact compacted-base receipt remains `APPLIED` and older receipts expire;
- invalid and future receipts fail without mutation;
- non-leader mutation returns a bounded leader hint;
- disconnect after acceptance does not cancel the proposal;
- shutdown rejects new commands without leaking a pending operation;
- exact owner/request/proposal limits and one-over-limit cases;
- credentials and command bytes never appear in responses or audit events;
- mandatory audit failure prevents command submission;
- HTMX polling terminates on every terminal state.
