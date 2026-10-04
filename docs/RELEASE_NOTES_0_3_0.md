# TurboRaft 0.3.0 release notes

TurboRaft 0.3.0 is a pre-1.0 release. The public C source surface may still
make explicitly documented minor-line changes, while wire, WAL, snapshot,
membership, and other durable contracts follow the stricter compatibility
policy in `docs/PRE_1_0_COMPATIBILITY.md`.

## Upgrade from 0.2.0

Upgrade from the released TurboRaft 0.2.0 durable/wire boundary to 0.3.0 is
qualified.

Hosted release compatibility uses the real released 0.2.0 package to produce
WAL, snapshot, persisted membership, and wire fixtures, then verifies current
0.3.0 code can consume them. Mixed-version live-peer qualification also covers
old/current traffic with both versions taking the leader role.

The old verifier runs with the released 0.2.0 dependency graph. Current
producer/preflight qualification runs with the current release dependency
graph. This is a durable/wire compatibility guarantee, not a promise that
external Salts, SaltsUtils, FlowMQ, or CHttp binaries from different release
epochs may be mixed in one process.

## Rollback to 0.2.0

In-place rollback from 0.3.0 to 0.2.0 is **conditional**.

Before starting the 0.2.0 binary on 0.3.0 durable state, stop normal mutation
and run the installed current-side preflight with the actual replay limits used
by that 0.2.0 deployment:

```text
turboraft_compat_check downgrade \
  --target v0.2.0 \
  --path-prefix <raft-wal-prefix> \
  --legacy-max-segments <v0.2.0 max_segments> \
  --legacy-segment-bytes <v0.2.0 segment_bytes> \
  --legacy-max-transaction-bytes <v0.2.0 max_transaction_bytes> \
  --legacy-max-log-entries <v0.2.0 max_log_entries> \
  --legacy-max-snapshot-bytes <v0.2.0 max_snapshot_bytes>
```

Direct in-place rollback is supported only when the command exits successfully
with `"compatible":true`.

The preflight is read-only. It validates the v0.2.0 visible WAL namespace,
transaction chain/checksums and operation replay rules, legacy transaction/log
limits, persisted configuration encoding, referenced snapshot size/header,
XXH3 checksum and SHA-256 digest. It preserves v0.2.0's tolerated final
torn-tail behavior. Hosted qualification proves positive state with the real
released v0.2.0 verifier and proves negative cases do not change durable bytes
with before/after SHA-256 inventories.

If preflight rejects the state, starting v0.2.0 directly is outside the
supported rollback contract. Use one of these supported recovery paths instead:

- restore a backup created at a v0.2.0-compatible durable boundary; or
- rejoin the node from a healthy compatible peer.

TurboRaft does not promise arbitrary future-to-0.2.0 rollback.

## Source/API migration notes

The 0.3 line remains pre-1.0. Notable intentional source/package changes
include:

- `tr_raft_runtime_t` is an opaque create/destroy handle rather than a
  caller-sized runtime object;
- ControlPlane uses the standalone CHttp package boundary;
- first-party source integration gates track the current canonical contracts
  rather than introducing source-version fallbacks.

These source-level changes do not change the qualified Raft wire, WAL,
snapshot, or persisted membership formats.

## Release-qualified platforms

The 0.3.0 native SDK release gate qualifies:

- Linux x64;
- Windows x64.

The tag-only native SDK pipeline must pass the complete package qualification
matrix before publishing `TurboRaft.Native 0.3.0`.
