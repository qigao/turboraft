# Performance benchmarks

TurboRaft benchmarks are correctness-checked executables, not CTest gates.
`BUILD_BENCHMARKS` is off by default so normal builds and test counts do not
change.

## Build and run

```powershell
cmake --preset win-release-user --fresh -DBUILD_BENCHMARKS=ON
cmake --build --preset win-release-user --target turboraft_benchmarks
.\build\msvc-release\bin\turboraft_benchmarks.exe
```

Run benchmarks only in a Release build on an otherwise idle host. Record CPU,
storage device, filesystem, power mode, compiler, TurboUtils/TurboRaft revision,
and filesystem version with every result. Compare repeated runs on the same host;
cross-host numbers are not a regression signal.

## Baselines

| Benchmark | Work in one timed sample | Reported units |
| --- | --- | --- |
| `wire v3 encode 8x256B` | Encode one maximum-count v3 AppendEntries batch | operations/s and MiB/s |
| `wire v3 decode 8x256B` | Decode the same validated frame | operations/s and MiB/s |
| `segmented WAL fsync single-entry commit` | Begin, hard state, one log entry, commit index, one sequential frame write and fsync | transaction latency and operations/s |

Setup, allocation, initial correctness checks, and cleanup remain outside timed
blocks. Relaxed filesystem durability is not a valid production comparison.

The first captured release result is observational, not a pass/fail threshold.
Set a regression threshold only after at least five stable runs establish host
variance. Investigate a repeatable change before optimizing; do not weaken
durability, bounds, validation, or negotiated protocol behavior to improve a
number.
