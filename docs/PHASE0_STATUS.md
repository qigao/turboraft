# Phase 0 Status

> Historical record: this document preserves the initial Phase 0 baseline and is not the current feature-status source. Use the repository README, implementation plan, and executable tests for current status.

## Completed

- Pinned `willemt/raft` commit `e428eeb921a014192d1d703dd317f3f29f5916c5`.
- Imported source, headers, tests, scripts, README, package metadata, and BSD license.
- Added a private static CMake target named `TurboRaft::WillemtCore`.
- Added Windows MSVC/Ninja and Linux GCC/Ninja configure, build, and test presets.
- Excluded `.codegraph`, build, and output directories from source control.
- Kept upstream algorithm sources and headers unmodified.

## Current gates

| Priority | Gate | Status |
|---|---|---|
| HIGH | Build and run upstream regression tests unchanged. | Pending because upstream test-only `CLinkedListQueue` is not yet pinned or audited. |
| HIGH | Map callback ordering around term, vote, log, message, and apply effects. | Source read complete; adapter not implemented. |
| HIGH | Prove coroutine-aware storage callback resumes only after durable completion. | Pending. |
| HIGH | Keep all upstream types private to the TurboRaft public ABI. | Pending public adapter. |
| MED | Confirm Windows warnings and signed-width assumptions. | Pending focused build. |
| MED | Add CoroNet and TurboUtils package discovery. | Deferred until adapter ownership is implemented. |

## Next slice

The next implementation slice creates a private TurboRaft core adapter with one owner, explicit callback ports, strict configuration validation, and no CoroNet types in the consensus boundary. It then introduces one TurboUtils-backed term/vote durability operation before log persistence or networking is added.
