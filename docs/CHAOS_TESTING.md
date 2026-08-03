# Multi-process chaos testing

`turboraft.multiprocess_chaos` runs three independent TurboRaft child
processes. Each child owns a Service instance and a distinct SQLite database.
The parent process owns the only simulated network queue and routes actual
TurboRaft wire frames over bounded stdin/stdout IPC.

The deterministic scripts cover:

- packet loss, duplication, delay, and reorder;
- a minority partition;
- termination of the observed leader;
- restart from the same SQLite database;
- stable recovery periods before and after faults.

For every child response the runner checks term and commit monotonicity,
`applied <= commit <= last_log`, one leader per observed term, and identical
application hashes at equal applied indexes. At least one user proposal must be
accepted and applied for every seed.

Run the test with the normal preset:

```powershell
ctest --preset win-release-user -R turboraft.multiprocess_chaos --output-on-failure
```

This runner validates process isolation, SQLite restart, Service durability
ordering, and deterministic network faults. It does not replace the CoroNet
mTLS tests: the parent deliberately owns routing so every fault decision is
reproducible and independent of operating-system packet timing.
