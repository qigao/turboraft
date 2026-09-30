# Supported platform and dependency matrix

This document defines the **release-qualified** TurboRaft 0.2.x build matrix.
It is intentionally narrower than the set of platforms on which the source may
compile. A platform or compiler not listed here is not necessarily unsupported
by the code; it is simply not a production-readiness gate until a hosted
workflow proves the same build, runtime, recovery, and package contracts.

The executable source of truth is the workflow named in each row. Changes to a
qualified compiler, operating system, dependency source, sanitizer, or package
version must update both this document and the corresponding workflow in the
same change.

## Release-qualified matrix

| Profile | Runner / toolchain | Dependency contract | Required evidence |
| --- | --- | --- | --- |
| Linux Release | `ubuntu-latest`, GCC (current hosted image; GCC 13.3 observed at qualification) | Salts `master`, SaltsUtils `master`, FlowMQ `main`; x64-linux vcpkg baseline below | configure/build, focused production integration, full CTest, SDK install, installed Core/CFlow/FlowMQ consumers |
| Linux ASan | `ubuntu-latest`, GCC Debug + AddressSanitizer | Salts `master` / SaltsUtils `master`; dependency Debug SDKs are intentionally **not** ASan-instrumented so TurboRaft owns the sanitizer runtime | focused production gates + complete CTest inventory under ASan |
| Linux UBSan | `ubuntu-latest`, GCC Debug + UndefinedBehaviorSanitizer | Salts `master` / SaltsUtils `master`; dependency Debug SDKs are intentionally uninstrumented to avoid mixed sanitizer runtimes | focused production gates + complete CTest inventory under UBSan |
| Windows Release | `windows-latest`, x64 MSVC via `VsDevCmd` + Ninja (MSVC 19.51 observed at qualification) | latest Salts.Native, latest SaltsUtils.Native, current FlowMQ `main`, x64-windows vcpkg baseline below | MSVC configure/build, focused production gates, full CTest, SDK install, installed Core/FlowMQ consumers |

Qualification evidence on 2026-09-23:

- Linux ASan/UBSan: PR #60, sanitizer run `35808420578`.
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
| Salts | current `master`, built in the workflow | latest Salts.Native Windows SDK | `find_package(Salts CONFIG REQUIRED)` |
| SaltsUtils | current `master`, built in the workflow | latest SaltsUtils.Native Windows SDK | `find_package(SaltsUtils CONFIG REQUIRED)`; supplies `salts-idlc` |
| FlowMQ | current `main` in integration gates; latest FlowMQ.Native in package qualification | current `main` in integration gates; latest FlowMQ.Native in package qualification | current FlowMQ package; TLS certificate/HELLO identity contract is required |
| TurboDB | not a Core dependency | not a Core dependency | only opt-in Redis/SQLite application qualification workflows |

TurboRaft qualifies against the current first-party dependency line: source-based gates build current repository heads and package-based gates restore the latest published SDK packages. FlowMQ remains a current-source integration dependency and is built against those exact roots. CI therefore validates TurboRaft against the latest available first-party contracts instead of pinning an obsolete dependency epoch.

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

TurboRaft 0.2.x publishes `TurboRaft.Native` for the release-qualified
`linux-x64` and `windows-x64` SDKs only. The package has exact NuGet
dependencies on Salts.Native 1.8.3, SaltsUtils.Native 4.1.3, and
latest FlowMQ.Native. macOS and Android remain intentionally absent until they
gain hosted release qualification.

A build is not qualified merely because the repository itself compiles.

Linux and Windows gates also verify installed-package consumption. Current
contracts include:

- `TurboRaft::Core`
- `TurboRaft::CFlowStateMachine`
- `TurboRaft::FlowMQ`

Installed consumers normalize `TURBORAFT_ROOT` before
`find_package(TurboRaft ... PATHS ...)`, so the same package tests are valid
on POSIX and Windows paths.

`TurboRaft::FlowMQ` transitively requires current FlowMQ package.

## Sanitizer policy

ASan and UBSan are release-blocking Linux gates.

Dependency Debug SDKs are built with their default ASan options explicitly
disabled in the sanitizer workflow. Only the TurboRaft build receives the
matrix-selected sanitizer. This prevents an UBSan process from loading an
ASan-instrumented first-party SDK and makes sanitizer failures attributable to
the product under test.

TSan is **not yet a release-blocking profile**. It may become one only after a
hosted compiler/runtime combination proves stable enough that failures are
actionable rather than runtime/toolchain noise.

## Not currently release-qualified

The following may compile or have component-level SDK coverage elsewhere in
the ecosystem, but TurboRaft 0.2.x does not currently claim them as production
merge gates:

- macOS;
- Android;
- Linux TSan;
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
- `.github/workflows/linux-sanitizers.yml` — Linux ASan/UBSan.
- `.github/workflows/windows-msvc-release.yml` — Windows x64 MSVC Release.
- `.github/workflows/cflow-orm-sqlite.yml` — SQLite/Orm application recovery
  qualification.
- `.github/workflows/cflow-redis-lua.yml` — Redis/Lua application recovery
  qualification.

A pull request that changes a supported dependency version, runner family,
compiler profile, package root contract, or sanitizer policy must update this
document together with the workflow change. Historical local runs do not
override a failing hosted gate.
