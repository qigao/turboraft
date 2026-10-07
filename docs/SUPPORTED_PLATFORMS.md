# Supported platform and dependency matrix

This document defines the **release-qualified** TurboRaft 0.3.x build matrix.
It is intentionally narrower than the set of platforms on which the source may
compile. A platform or compiler not listed here is not necessarily unsupported
by the code; it is simply not a production-readiness gate until a hosted
workflow proves the same build, runtime, recovery, and package contracts.

The executable source of truth is the workflow named in each row. Integration gates restore current stable Salts and SaltsUtils SDKs;
FlowMQ and CHttp source integrations still follow their current branches. Native
package qualification records the actual resolved SDK versions in each manifest. Changes to a
qualified compiler, operating system, dependency source, sanitizer, or package
contract must update both this document and the corresponding workflow in the
same change.

## Release-qualified matrix

| Profile | Runner / toolchain | Dependency contract | Required evidence |
| --- | --- | --- | --- |
| Linux Release | `ubuntu-latest`, GCC (current hosted image) | latest published Salts.Native / SaltsUtils.Native SDKs, FlowMQ `main`, CHttp `master`; x64-linux vcpkg baseline below | configure/build, full CTest once (including production integration), SDK install, installed Core/CFlow/FlowMQ/ControlPlane consumers |
| Linux ASan | `ubuntu-latest`, GCC Debug + AddressSanitizer | latest published Release Salts / SaltsUtils SDKs; source-built CHttp / FlowMQ Debug SDKs; dependencies remain uninstrumented | complete CTest inventory once under ASan |
| Linux UBSan | `ubuntu-latest`, GCC Debug + UndefinedBehaviorSanitizer | latest published Release Salts / SaltsUtils SDKs; source-built CHttp / FlowMQ Debug SDKs; dependencies remain uninstrumented | complete CTest inventory once under UBSan |
| Linux TSan | `ubuntu-latest`, GCC Debug + ThreadSanitizer | latest published Release Salts / SaltsUtils SDKs; source-built CHttp / FlowMQ Debug SDKs; dependencies remain uninstrumented | complete CTest inventory once under TSan |
| Windows Release | `windows-2025`, x64 MSVC via `VsDevCmd` + Ninja using the shared v145 triplet | latest published Salts.Native / SaltsUtils.Native roots, current FlowMQ `main`, CHttp `master`, canonical shared-cache triplet | MSVC configure/build, full CTest once, SDK install, installed Core/FlowMQ/ControlPlane consumers |

Qualification evidence on 2026-09-23:

- Linux ASan/UBSan: PR #60, sanitizer run `35808420578`.
- Linux TSan: PR #122 initial qualification run `37168121161`, focused production gates and full CTest PASS with the hosted GCC/TSan runtime.
- Windows MSVC Release: PR #61, Windows run `35810071848`.
- Windows qualification includes the real FlowMQ/mTLS peer-service gate and
  the multi-process chaos test.
- The Windows full Release CTest completed 58/58, including the
  multi-process chaos test.

## First-party dependency contract

Hosted source integrations and benchmarks consume the latest stable Salts and
SaltsUtils Native SDKs through `.github/actions/setup-native-sdks`. Restore uses
`--no-cache --force-evaluate` and the existing floating package references;
`export_native_sdks.py` resolves exact roots and versions from NuGet assets and
exports tool/runtime paths. Versions appear in the restore logs and benchmark
evidence. Lua and QuickJS are neither built nor restored by these jobs.

These jobs no longer rebuild the producers' optional bindings. Missing or
incompatible published SDKs fail explicitly; they do not trigger a source-build
fallback. SaltsUtils 4.2.0 keeps runtime discovery with binding consumers, so
TurboRaft can use DataBind without installing the VM runtimes.

Redis qualification selects TurboDB's standalone Redis build with ORM, dbtools,
SQL parser and TidesSQL disabled. SQLite qualification uses the current ORM
build, which includes all native drivers; its cache restore therefore includes
the producer's `libpq[core,zstd]` contract even though the exercised database is
SQLite. The removed `ORM_BUILD_*_DRIVER` options no longer select that graph.

The scheduled `extended-chaos.yml` campaign consumes the latest stable
`Salts.Native`, `SaltsUtils.Native`, and `FlowMQ.Native` packages through the
same floating references as native SDK packaging. Each run restores with
`--no-cache --force-evaluate`, resolves SDK roots from NuGet assets, and records
the selected versions in `artifacts/campaign-metadata.txt`. This replaces the
incompatible combination of pinned Salts 1.8.3 / SaltsUtils 4.1.3 and moving
FlowMQ source. It uses `ci-linux-release-user` for manifest-mode configuration,
the complete build graph, and CTest execution. Dependency restore logs are
retained even when configuration or compilation never starts.

TurboRaft configure requires explicit active-profile roots:

- `SALTS_ROOT`
- `SALTS_UTILS_ROOT`
- `FLOWMQ_ROOT`

TurboRaft normalizes environment-provided roots to CMake paths before package
lookup. This is required on Windows because raw backslash paths passed through
`find_package(PATHS ...)` can be reinterpreted by newer CMake policy/macro
parsing.

The first-party dependency policy is:

| Dependency | Linux hosted gates | Windows hosted gate | Public contract |
| --- | --- | --- | --- |
| Salts | latest published Salts.Native SDK | latest published Salts.Native SDK | `find_package(Salts CONFIG REQUIRED)` |
| SaltsUtils | latest published SaltsUtils.Native SDK | latest published SaltsUtils.Native SDK | `find_package(SaltsUtils CONFIG REQUIRED)`; supplies `salts-idlc` |
| FlowMQ | current `main` source for source integration; Native package qualification restores the latest stable released FlowMQ.Native package | current `main` source for source integration; Native package qualification restores the latest stable released FlowMQ.Native package | `find_package(FlowMQ 1.2.1 CONFIG REQUIRED)`; TLS certificate/HELLO identity contract is required |
| TurboDB | not a Core dependency | not a Core dependency | only opt-in Redis/SQLite application qualification workflows |

TurboRaft integrations use the published Salts / SaltsUtils dependency graph.
FlowMQ and CHttp source qualification remains separate from their published
Native package qualification.
Resolved package versions are release evidence, not a reason to add source-side
compatibility fallbacks.

## Toolchain and generated-code contract

- C language: C11.
- Minimum project CMake version: 3.23.
- vcpkg builtin baseline:
  `b1b19307e2d2ec1eefbdb7ea069de7d4bcd31f01`.
- re2c host package: `Qigao.Re2c.Binary 4.6.3` (binary reports re2c 4.6).
- TurboRaft wire generation uses the SaltsUtils `salts-idlc` from the
  active host SDK/root.
- The build-host Lemon parser generator is deliberately excluded from global
  product sanitizer instrumentation. Generated TurboRaft product code remains
  instrumented.

The hosted workflows consume the shared `qigao/vcpkg-cache` and shared re2c
tooling. Native release and package-qualification dependency restores are
`--only-binarycaching`; a cache miss is an infrastructure failure and must not
silently rebuild a third-party dependency inside a consumer workflow.

## Package qualification

CMake requires Salts 2.1+, SaltsUtils 4.2+, and FlowMQ 1.2.1+ when
FlowMQ is selected. The SDKs must use a compatible dependency generation.

TurboRaft 0.3.x publishes `TurboRaft.Native` for the release-qualified
`linux-x64` and `windows-x64` SDKs only. The release pipeline restores the
latest stable released Salts.Native, SaltsUtils.Native, FlowMQ.Native, and
CHttp.Native packages from a fresh package directory and records the exact
resolved producer versions in each SDK manifest. These producer packages are
build/qualification inputs rather than pinned transitive NuGet dependencies of
`TurboRaft.Native`; installed consumers supply the explicit dependency roots
required by their selected components. macOS and Android remain intentionally
absent until they gain hosted release qualification.

A build is not qualified merely because the repository itself compiles.

Linux and Windows gates also verify installed-package consumption. Current
contracts include:

- `TurboRaft::Core`
- `TurboRaft::CFlowStateMachine`
- `TurboRaft::FlowMQ`

Installed consumers normalize `TURBORAFT_ROOT` before
`find_package(TurboRaft ... PATHS ...)`, so the same package tests are valid
on POSIX and Windows paths.

`TurboRaft::FlowMQ` requires a compatible FlowMQ SDK through the explicit
`FLOWMQ_ROOT` package boundary. The Native qualification project restores the
latest stable released FlowMQ.Native package instead of pinning a producer
version in TurboRaft.

## Sanitizer policy

ASan, UBSan, and TSan are release-blocking Linux gates.

Salts and SaltsUtils are uninstrumented published Release SDKs. CHttp and
FlowMQ retain their source-built Debug SDKs with sanitizer options disabled.
Only the TurboRaft Debug build receives the matrix-selected sanitizer. This prevents an UBSan process from loading an
ASan-instrumented first-party SDK and makes sanitizer failures attributable to
the product under test.

The hosted GCC/ThreadSanitizer combination is qualified by PR #122. TSan uses
the same full CTest inventory as ASan/UBSan, with
all first-party dependency SDKs left uninstrumented so reports are
attributable to TurboRaft rather than mixed sanitizer runtimes.

## Not currently release-qualified

The following may compile or have component-level SDK coverage elsewhere in
the ecosystem, but TurboRaft 0.3.x does not currently claim them as production
merge gates:

- macOS;
- Android;
- compilers/toolchain versions older than the hosted matrix;
- 32-bit targets;
- non-x64 Windows targets.

Adding one of these platforms requires a hosted gate with the same standard:
configure/build, relevant production integration, full supported CTest
inventory, and installed-package verification where installation is supported.

## CI ownership

The matrix maps directly to these workflows. Each Release or sanitizer job
runs its complete CTest inventory once; the focused production tests are
included in that inventory.

- `.github/workflows/full-stack-acceptance.yml` — Linux Release and installed
  package contracts.
- `.github/workflows/linux-sanitizers.yml` — Linux ASan/UBSan/TSan.
- `.github/workflows/windows-msvc-release.yml` — Windows x64 MSVC Release.
- `.github/workflows/cflow-orm-sqlite.yml` — SQLite/Orm application recovery
  qualification.
- `.github/workflows/cflow-redis-lua.yml` — Redis/Lua application recovery
  qualification.

A pull request that changes a supported dependency version, runner family,
compiler profile, package root contract, or sanitizer policy must update this
document together with the workflow change. Historical local runs do not
override a failing hosted gate.

### Workflow and test deduplication

The FlowMQ, multiprocess chaos, apply acknowledgement, and CFlow gates are
already part of full CTest. Separate focused invocations duplicated 18 test
executions across the two Release jobs and three sanitizer variants.
Different operating systems, sanitizers, source dependencies, and restored
release SDKs remain separate qualification boundaries.

The obsolete `multigroup-wire-verify.yml` compiled inline test programs and
source stubs outside the normal build. Its feature-branch triggers now use
`full-stack-acceptance.yml`, which builds production libraries and runs the
maintained tests. The authoritative coverage is:

| Retired inline scenario | Maintained CTest coverage / owner |
| --- | --- |
| Wire/group API and baseline handshake | `turboraft.multigroup_wire`, `turboraft.transport_contract`, `turboraft.peer_handshake` |
| Bounded group scheduling and routing rejection | `turboraft.group_queue`, `turboraft.transport` |
| Apply retry and completed ReadIndex queue | `turboraft.apply_runtime`, `turboraft.apply_ack`, `turboraft.service`, `turboraft.read_index` |
| Streaming data | `turboraft.data_stream` |
| Streaming snapshot source, sink, manager and policy | `turboraft.snapshot_sender`, `turboraft.snapshot_receiver`, `turboraft.snapshot_manager`, `turboraft.snapshot_manager_runtime`, `turboraft.snapshot_policy` |
| Streaming WAL recovery | `turboraft.wal_storage` |
| Three-process multi-group faults and sibling progress | `turboraft.multiprocess_chaos`, `turboraft.snapshot_group_isolation` |
| Proposal batching | `turboraft.proposal_batch` |
| Durability benchmark | `durable-fsync-benchmark.yml` |

`native-sdk-release.yml` is the single release entry for version tags and
`release:` commits on master; both reuse `native-sdk-pipeline.yml`. PR package
qualification keeps its read-only entry and uses the same pipeline. The
one-time v0.2.0 bootstrap was removed after the published release was verified;
compatibility workflows still consume that release and retain their distinct
fixtures. SDK root/version exports now share `tools/ci/export_native_sdks.py`,
which reads the resolved NuGet assets rather than selecting a directory by
name. Missing or ambiguous SDKs fail before any environment values are written.

The maintained C/C++ test cases, native job IDs, and artifact names remain
unchanged. If repository rules still require the retired `syntax` or bootstrap
checks, remove those obsolete requirements when adopting this change. Revert
the workflow refactor to restore the former entry points; no data or protocol
migration is involved.
