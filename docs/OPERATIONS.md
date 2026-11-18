# TurboRaft operations runbook

This document defines the operator-facing recovery and
incident-diagnostic contract. It complements the build-focused
[Diagnostics](DIAGNOSTICS.md).

## Backup handoff and copy boundary

A durable backup copy must be taken only after
`tr_raft_service_prepare_backup()` returns `SALTS_OK`.

The required owner sequence is:

1. stop new local proposals and application-side mutation producers;
2. drain/take outstanding read results and let pending transport/journal work
   reach the Service quiescence boundary;
3. call `tr_raft_service_prepare_backup()`;
4. close the owner WAL handle;
5. copy the authoritative durable files for every group;
6. reopen the original WAL, bind a new storage adapter, and call
   `tr_raft_service_resume_backup()`;
7. only then re-enable producers.

While backup is prepared, Service mutation entry points return `SALTS_EBUSY`.
Do not copy a live WAL before this boundary and call it a TurboRaft backup.

For a WAL prefix `<prefix>`, the copy set is the authoritative files matching:

- `<prefix>.manifest`;
- live `<prefix>.<sequence>.wal` segments;
- the snapshot file referenced by the durable WAL boundary,
  `<prefix>.snapshot.<index>.<term>`.

Never copy `*.tmp` staging files or `<prefix>.lock`. The manifest is
mandatory for high-sequence stores introduced by the live-range manifest
format. A backup with high-sequence WAL files but no manifest must be rejected
rather than interpreted as a fresh store.

A fresh restore must configure `create_if_missing = false` on its first open.
Failure to open/recover the copied set is a restore failure; do not retry by
silently creating an empty store. After recovery, compare term, commit index,
applied snapshot boundary, and persisted membership before allowing the node to
rejoin peer traffic.

The [WAL reload fixture](../tests/core/test_raft_service_wal_reload.c) and
[multiprocess chaos fixture](../tests/chaos/test_raft_multiprocess_chaos.c) cover
the prepare/close/reopen/resume handoff, including injected reopen failure.
The chaos fixture also copies a quiesced durable set to a distinct path, opens
with `create_if_missing=false`, rejects an incomplete copy, and checks quorum
progress plus restored-node catch-up. Run the formal tests with the selected
current SDK inputs before claiming that a deployment has passed this drill.

## Metrics inventory

Collect these status surfaces at the same cadence and tag them with node,
cluster, and dependency-build identity.

### Raft Service

From `tr_raft_service_status()`:

- role, term, leader ID;
- last-log, commit, and applied indexes;
- membership phase/voter/learner counts;
- `faulted` and `cause`;
- `backup_prepared`;
- completed/read-state pressure;
- journal-compaction state.

From `tr_raft_service_get_peer_delivery_status()` per peer:

- peer pause state;
- staged message count and bytes;
- staged snapshot state;
- capacity rejection count;
- paused logical ticks;
- last peer-delivery error.

### Snapshot / durable state

Record sender/receiver progress while snapshot transfer is active. For WAL
incidents, capture file metadata and hashes for the manifest, live segments,
and snapshot boundary. Do not attach WAL contents by default.

## Alert policy

Some signals are correctness/failure alerts and do not require a performance
baseline:

- `service.faulted == true`;
- nonzero persistent Service/transport `last_error`;
- a peer remaining paused while the healthy quorum is otherwise progressing;
- backup remaining prepared beyond the operator's bounded copy window;
- restore/open failure, missing manifest for a high-sequence copy, or durable
  metadata checksum failure.

Latency/throughput/lag thresholds need measured deployment evidence. The
[performance policy](PERFORMANCE_SLOS.md) distinguishes executable correctness
checks from measurements; current benchmarks do not enforce numeric
performance SLOs. Page on the correctness/fault conditions above and report
lag/queue/latency distributions unless a deployment-specific threshold has its
own measured production rationale.

## Incident evidence

The standalone incident-bundle utility has been removed from the SDK. Collect
status, storage metadata and logs using the deployment's existing operational
process. Select sanitized configuration files explicitly and exclude private
keys and environment credentials.

For a support/incident handoff, retain:

- status output and storage file metadata with hashes;
- TurboRaft and first-party package/repository versions;
- the exact deployment config with secrets removed;
- the relevant chaos/recovery seed or reproduction command when one exists;
- logs covering the first fault transition, not only the final failure.
