# Supported platform and dependency matrix

This document defines the **release-qualified** TurboRaft 0.3.x build matrix.
It is intentionally narrower than the set of platforms on which the source may
compile. A platform or compiler not listed here is not necessarily unsupported
by the code; it is simply not a production-readiness gate until a hosted
workflow proves the same build, runtime, recovery, and package contracts.

The executable source of truth is the workflow named in each row. Source
integration gates follow current first-party branches; native package
qualification may temporarily remain on the last mutually compatible
published SDK epoch until its producer publishes the next release. Changes to a
qualified compiler, operating system, dependency source, sanitizer, or package
contract must update both this document and the corresponding workflow in the
same change.

## Release-qualified matrix

| Profile | Runner / toolchain | Dependency contract | Required evidence |
| --- | --- | --- | --- |
| Linux Release | `ubuntu-latest`, GCC (current hosted image) | current Salts `master`, SaltsUtils `master`, FlowMQ `main`, CHttp `master`; x64-linux vcpkg baseline below | configure/build, focused production integration, full CTest, SDK install, installed Core/CFlow/FlowMQ/ControlPlane consumers |
| Linux ASan | `ubuntu-latest`, GCC Debug + AddressSanitizer | current Salts / SaltsUtils / CHttp source branches; dependency Debug SDKs are intentionally **not** ASan-instrumented so TurboRaft owns the sanitizer runtime | focused production gates + complete CTest inventory under ASan |
| Linux UBSan | `ubuntu-latest`, GCC Debug + UndefinedBehaviorSanitizer | current Salts / SaltsUtils / CHttp source branches; dependency Debug SDKs are intentionally uninstrumented to avoid mixed sanitizer runtimes | focused production gates + complete CTest inventory under UBSan |
| Linux TSan | `ubuntu-latest`, GCC Debug + ThreadSanitizer | current Salts / SaltsUtils / CHttp source branches; dependency Debug SDKs remain uninstrumented so TurboRaft owns the TSan runtime | focused production gates + complete CTest inventory under TSan |
| Windows Release | `windows-2025`, x64 MSVC via `VsDevCmd` + Ninja using the shared v145 triplet | latest published Salts.Native / SaltsUtils.Native roots, current FlowMQ `main`, CHttp `master`, canonical shared-cache triplet | MSVC configure/build, focused production gates, full CTest, SDK install, installed Core/FlowMQ/ControlPlane consumers |

Qualification evidence on 2026-09-23:

- Linux ASan/UBSan: PR #60, sanitizer run `35808420578`.
- Linux TSan: PR #122 initial qualification run `37168121161`, focused production gates and full CTest PASS with the hosted GCC/TSan runtime.
- Windows MSVC Release: PR #61, Windows run `35810071848`.
- Windows qualification includes the real FlowMQ/mTLS peer-service gate and
  the multi-process chaos test.
- The Windows full Release CTest completed 58/58, including the
  multi-process chaos test.

## First-party dependency contract

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
| Salts | current `master` source | latest published Salts.Native SDK | `find_package(Salts CONFIG REQUIRED)` |
| SaltsUtils | current `master` source | latest published SaltsUtils.Native SDK | `find_package(SaltsUtils CONFIG REQUIRED)`; supplies `salts-idlc` |
| FlowMQ | current `main` source for source integration; Native package qualification restores the latest stable released FlowMQ.Native package | current `main` source for source integration; Native package qualification restores the latest stable released FlowMQ.Native package | `find_package(FlowMQ CONFIG REQUIRED)`; TLS certificate/HELLO identity contract is required |
| TurboDB | not a Core dependency | not a Core dependency | only opt-in Redis/SQLite application qualification workflows |

TurboRaft source integration intentionally follows current first-party
branches. The native package pipeline remains on the last mutually compatible
published SDK graph until all producers have published matching releases.
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

Dependency Debug SDKs are built with their default ASan options explicitly
disabled in the sanitizer workflow. Only the TurboRaft build receives the
matrix-selected sanitizer. This prevents an UBSan process from loading an
ASan-instrumented first-party SDK and makes sanitizer failures attributable to
the product under test.

The hosted GCC/ThreadSanitizer combination is qualified by PR #122. TSan uses
the same focused production gates and full CTest inventory as ASan/UBSan, with
first-party dependency Debug SDKs left uninstrumented so reports are
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

The matrix maps directly to these workflows:

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
