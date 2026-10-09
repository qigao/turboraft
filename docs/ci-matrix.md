# CI build matrix and compiler caching

TurboRaft keeps functional correctness, sanitizer, performance, compatibility and release qualification in separate GitHub Actions workflows. This avoids mixing release publishing with routine pull-request checks.

| Lane | Workflow | Toolchain | Coverage |
| --- | --- | --- | --- |
| Linux full stack | `full-stack-acceptance.yml` | GCC / Ninja, Release | CHttp + FlowMQ integration, CTest and installed package consumers |
| Windows full stack | `windows-msvc-release.yml` | MSVC / Ninja, Release | CTest and installed package consumers |
| Linux sanitizers | `linux-sanitizers.yml` | GCC / Ninja, Debug | ASan + UBSan together; independent TSan |
| Native SDK | `native-sdk-qualification.yml` and `native-sdk-pipeline.yml` | Linux GCC and Windows MSVC | Build, pack and installed SDK consumer qualification |
| Specialized | `cflow-orm.yml`, `extended-chaos.yml`, `durable-fsync-benchmark.yml`, `release-compat-v020.yml`, `v020-live-mixed-peer.yml`, `v020-upgrade-compat.yml` | Linux GCC / Ninja | Recovery, long-running chaos, retained performance and prior-release compatibility |

The sanitizer matrix runs **two jobs**, `asan-ubsan` and `tsan`. CMake's sanitizer module supports the combined AddressSanitizer/UndefinedBehaviorSanitizer flags and explicitly prohibits combining ThreadSanitizer with AddressSanitizer.

## Cache boundaries

- The shared `qigao/vcpkg-cache` action continues to provide **read-only binary package inputs**. No source fallback or dependency resolution is introduced by compiler caching.
- `.github/actions/setup-ccache` uses `actions/cache@v4` with separate scope keys per workflow/configuration, Linux runner architecture and CMake/vcpkg metadata. Each cache is capped at 500 MB.
- Linux builds use `CMAKE_C_COMPILER_LAUNCHER` and `CMAKE_CXX_COMPILER_LAUNCHER` environment initialization, so CMake presets and nested CHttp/FlowMQ consumer builds inherit ccache without CI listing individual build targets.
- Only **compiler objects** are cached. CMake build directories, test results, runtime state, packaged SDKs and benchmark results are rebuilt or exercised on every applicable run.
- MSVC builds remain uncached by ccache pending a validated Windows compiler-cache lane; they are not removed from coverage.

Changes affecting `src/**`, `include/**`, and CMake/SDK contracts must continue through the full acceptance and native SDK qualification gates. Performance and compatibility evidence remains separate rather than being treated as cached build output.
