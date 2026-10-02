# TurboRaft operations runbook

This document defines the operator-facing recovery, certificate-rotation, and
incident-diagnostic contract. It complements the build-focused
`DIAGNOSTICS.md`.

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

The in-process and multiprocess chaos suites already qualify the
prepare/close/reopen/resume handoff, including injected reopen failure. Issue
#88 remains open until a retained release-qualified drill copies the durable
set into a distinct fresh-process path and proves rejoin/catch-up.

## Certificate rotation

FlowMQ peer identity is bound to the tuple of HELLO identity and verified
client-certificate SHA-256 fingerprint. The policy is immutable for one
service lifetime, so rotation is intentionally restart-based.

Use this sequence:

1. add the new fingerprint next to the old fingerprint for the same peer
   identity;
2. restart listeners with the overlap policy;
3. verify both the old and new client certificates can authenticate under the
   same FlowMQ identity;
4. deploy the new client certificate/key;
5. remove the old fingerprint;
6. restart listeners again;
7. verify the new certificate is accepted and the retired certificate
   increments `tls_identity_rejections` without delivering a Raft frame.

The hosted `turboraft.flowmq_peer_service` test exercises this exact
old/overlap/new/retired sequence using two independent test CAs. Hot reload is
not part of the contract.

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

### FlowMQ peer service

From `tr_raft_flowmq_peer_service_get_status()` and the per-peer/group
status accessors:

- active group count;
- queued payload count and bytes;
- frames sent/received;
- group-routing rejections;
- TLS identity rejections;
- blocked/failed peer observations;
- transport/group queue occupancy;
- last error.

### Snapshot / durable state

Record sender/receiver progress while snapshot transfer is active. For WAL
incidents, capture file metadata and hashes for the manifest, live segments,
and snapshot boundary. Do not attach WAL contents by default.

## Alert policy

Some signals are correctness/failure alerts and do not require a performance
baseline:

- `service.faulted == true`;
- nonzero persistent Service/transport `last_error`;
- any unexpected increase in `tls_identity_rejections`;
- a peer remaining paused while the healthy quorum is otherwise progressing;
- backup remaining prepared beyond the operator's bounded copy window;
- restore/open failure, missing manifest for a high-sequence copy, or durable
  metadata checksum failure.

Latency/throughput/lag thresholds must not be invented here. Numeric p95/p99
proposal, ReadIndex, snapshot, and commit/applied-lag thresholds are owned by
the measured baseline work in #87. Until that evidence exists, alert on
correctness faults and report lag/queue distributions without a fabricated
fixed SLO.

## Incident bundle

The installed SDK contains:

`share/turboraft/tools/turboraft_incident_bundle.py`

It is read-only. It can query the ControlPlane status endpoint, record
WAL/snapshot/manifest metadata and SHA-256 hashes, and copy only configuration
files explicitly selected by the operator.

Example:

```sh
python3 "$TURBORAFT_ROOT/share/turboraft/tools/turboraft_incident_bundle.py" \
  --output incident-2026-10-02.tar.gz \
  --status-url http://127.0.0.1:9000/raft/status \
  --wal-prefix /srv/turboraft/group-7/raft \
  --config /etc/turboraft/node-sanitized.json \
  --label node=node-7 \
  --label release=0.3.0
```

The bundle contains metadata, the status JSON (or a status-fetch error), a
storage inventory with size/mtime/hash, and explicitly selected config files.
`*.tmp` staging files are excluded. The tool does not discover or copy
private keys, environment variables, WAL payload contents, or arbitrary
configuration automatically.

For a support/incident handoff, retain:

- the incident bundle;
- TurboRaft and first-party package/repository versions;
- the exact deployment config with secrets removed;
- the relevant chaos/recovery seed or reproduction command when one exists;
- logs covering the first fault transition, not only the final failure.
