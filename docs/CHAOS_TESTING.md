# Multi-process chaos testing

## R0 Multi-Group acceptance

The multiprocess harness uses three physical child processes. Each child now
hosts three independent Raft groups (100, 200, and 300), each with its own
Service instance, WAL prefix, applied hash, and recovery state. The process
still owns one codec/stdin/stdout IPC link, so all group frames share the same
simulated physical node-to-node network owned by the parent.

The IPC protocol is version 2. Every group-scoped command and response carries
an explicit non-zero group ID; STOP is process-scoped and uses group ID zero.
The parent preserves group identity on queued wire frames and tracks term,
commit, leader, and application-hash safety independently for every
(group,node) pair.

The dedicated R0 acceptance scenario requires:

- group 100 to elect node 1, group 200 node 2, and group 300 node 3 under
  deterministic group-specific initial election timeouts;
- an unknown group request to return a routing rejection while the same child
  process continues serving a valid group;
- group-100 frames to remain intentionally queued while groups 200 and 300
  continue ticking, replicating, committing, and applying independent
  proposals over the same simulated physical link;
- one physical child restart to reopen all three independent WAL prefixes and
  recover the committed state of the active groups.

Each child derives WAL prefixes from its physical-node base path, for example:

```text
multigroup-node-1.db.g100
multigroup-node-1.db.g200
multigroup-node-1.db.g300
```

The R0 acceptance source is intentionally part of the existing chaos target so
the final project-level gate exercises both the original deterministic
single-group fault campaign (group 100 compatibility path) and the new
Multi-Group scenario. A syntax-only verifier is useful during development but
does not substitute for the real multiprocess CTest run.

## Existing deterministic fault campaign

`turboraft.multiprocess_chaos` runs three independent TurboRaft child
processes. Each child owns a Service instance and a distinct segmented WAL.
The parent process owns the only simulated network queue and routes actual
TurboRaft wire frames over bounded stdin/stdout IPC.

The deterministic scripts cover:

- packet loss, duplication, delay, and reorder;
- a minority partition;
- termination of the observed leader;
- restart from the same WAL prefix;
- online WAL backup handoff on an elected leader and follower: Service
  prepares, the node owner closes and reopens its WAL, binds the new adapter,
  then resumes before the next command turn;
- an injected WAL reopen failure after prepare/close: the child returns the
  original I/O error and exits instead of serving with a detached adapter;
- stable recovery periods before and after faults.

For every child response the runner checks term and commit monotonicity,
`applied <= commit <= last_log`, one leader per observed term, and identical
application hashes at equal applied indexes. Every seed records an accepted
proposal after the follower handoff, then runs a bounded fault-free recovery
window and requires all three nodes to reach that index with an identical final
applied index and hash.

Run the test with the normal preset:

```powershell
ctest --preset win-release-user -R turboraft.multiprocess_chaos --output-on-failure
```

With no additional environment configuration, the runner executes seeds 1
through 4. A bounded campaign can select a different contiguous range:

```powershell
$env:TURBORAFT_CHAOS_FIRST_SEED = "17"
$env:TURBORAFT_CHAOS_SEED_COUNT = "8"
try {
  ctest --preset win-release-user -R "^turboraft\.multiprocess_chaos$" --output-on-failure
} finally {
  Remove-Item Env:TURBORAFT_CHAOS_FIRST_SEED -ErrorAction SilentlyContinue
  Remove-Item Env:TURBORAFT_CHAOS_SEED_COUNT -ErrorAction SilentlyContinue
}
```

`TURBORAFT_CHAOS_FIRST_SEED` accepts decimal values from 1 through
`4294967295`. `TURBORAFT_CHAOS_SEED_COUNT` accepts 1 through 16, and the final
seed must remain within that same 32-bit range. Empty, signed, non-decimal,
zero, oversized, or overflowing values fail before the runner creates a chaos
workspace or starts child nodes. Split longer campaigns into ranges of at most
16 seeds so each CTest invocation remains bounded.

The runner prints a `reproduce_env` line before each seed. Use those two values
to reproduce only that seed:

```powershell
$env:TURBORAFT_CHAOS_FIRST_SEED = "23"
$env:TURBORAFT_CHAOS_SEED_COUNT = "1"
ctest --preset win-release-user -R "^turboraft\.multiprocess_chaos$" --output-on-failure
```

This runner validates process isolation, WAL restart, Service durability
ordering, and deterministic network faults. It does not replace the CNet/FlowMQ
mTLS tests: the parent deliberately owns routing so every fault decision is
reproducible and independent of operating-system packet timing.
