# Multi-process chaos testing

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
