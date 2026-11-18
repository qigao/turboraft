# Platforms, dependencies and qualification

This document describes the matrix declared by the current workflows. A
workflow definition is not evidence that the current revision passed; report
actual run results with their source revision and resolved dependency versions.

## Continuous integration matrix

The [CI workflow](../.github/workflows/ci.yml) prepares native dependency inputs
and configures each lane's complete graph before building and running CTest.
Source, header, grammar, schema and Python test changes trigger CI on the
selected branches. Use workflow dispatch to verify a CI configuration change.

| Lane | Runner / toolchain | Configure, build and test preset |
| --- | --- | --- |
| Linux Release | `ubuntu-24.04`, GCC, Ninja | `ci-gcc-release-user` |
| Linux ASan + UBSan | `ubuntu-24.04`, Clang Debug, Ninja | `ci-llvm-native-asan-ubsan-user` |
| Linux TSan | `ubuntu-24.04`, Clang Debug, Ninja | `ci-llvm-native-tsan-user` |
| Linux arm64 Release | `ubuntu-24.04-arm`, Clang, Ninja | `ci-llvm-native-release-user` |
| macOS arm64 Release | `macos-15`, Apple Clang, Ninja | `ci-llvm-native-release-user` |
| Windows x64 Release | `windows-2025`, MSVC through `VsDevCmd`, Ninja | `ci-msvc-release-user` |

The matching Salts SDK releases also determine which native architectures can
be tested. Linux arm64 builds CHttp and TurboDB on the ARM runner. macOS x64
and Windows arm64 require additional Salts SDK profiles.

The presets also describe other compiler and cross-compilation configurations.
Their presence does not establish equivalent runtime or release qualification.
Android build/install checks do not establish device execution. See the
[user presets](../CMakeUserPresets.json) and [preset definitions](../presets/)
for the actual available configurations.

## Native dependency inputs

Follow the [upstream dependency instructions](../README.md#upstream-dependency-versions)
before configuring. CMake does not implicitly load a local `.env`. The selected
compiler, target triplet, host RID, dependency installation and SDK roots must
belong to the same restore.

The [native-input action](../.github/actions/setup-native-inputs/action.yml)
uses the selected vcpkg-cache release resolver to produce URL/SHA-256 locks.
The locked restore supplies an installation root in its receipt.
Missing release inputs, unavailable assets or digest mismatches fail instead of
selecting another dependency source.

Target libraries and host tools are distinct inputs. re2c and Lemon come from
the selected vcpkg-cache host packages via `RE2C_ROOT` and `LEMON_ROOT`; Lemon
uses its paired `lempar.c`. The installed SaltsUtils SDK supplies `salts-idlc`.
The [wire schema](../schema/turboraft_wire.schema) generates a C
binding in the build tree. See the
[schema documentation](../schema/README.md) and [text grammar build](TEXT_SYNTAX.md#build).

The CI runner writes its local `.env` from the repository Actions variable
`TURBORAFT_CI_ENV`. Synchronize the seven non-secret version assignments from
the local project environment when updating SDKs:

```powershell
$keys = 'VCPKG_CACHE_VERSION','SALTS_BASE_VERSION','CMETA_VERSION','SALTS_CNET_VERSION','SALTS_UTILS_VERSION','CHTTP_VERSION','TURBODB_VERSION'
$versions = Get-Content .env | Where-Object { ($_ -split '=', 2)[0] -cin $keys }
$versions -join "`n" | gh variable set TURBORAFT_CI_ENV --repo qigao/turboraft
```

Each native job verifies the selected Salts release manifests, producer revisions
and SDK archive digests, builds the selected CHttp and TurboDB tags against
those SDKs, and runs the complete TurboRaft build, CTest and install presets.

After changing compiler, triplet or SDK roots, explicitly configure with
`cmake --fresh --preset <preset>` to remove stale package lookup results.
Compiler/payload caches accelerate work but do not select SDK versions or
substitute for missing dependency inputs.

## Local build and test

After loading the intended compiler environment and dependency inputs:

```powershell
cmake --fresh --preset msvc-release-user
cmake --build --preset msvc-release-user
ctest --preset msvc-release-user --output-on-failure
```

Linux x64 Release uses `gcc-release-user`. The matrix above lists the
Linux sanitizer presets. Tests are controlled by
`BUILD_TESTS`; benchmarks by `BUILD_BENCHMARKS`; examples by `BUILD_EXAMPLES`.
Modules are assembled unconditionally. The switches are declared in
[CMakeOptions.cmake](../CMakeOptions.cmake). There are no source-profile,
legacy-compatibility or standalone-tool switches.

## Native SDK packaging

The [release entry point](../.github/workflows/native-sdk-release.yml) calls
[the Native SDK pipeline](../.github/workflows/native-sdk-pipeline.yml). The
pipeline builds, tests and installs the complete Linux and Windows graph,
then packs both SDKs. It captures installed public headers, enum values,
callback contracts and archive symbols, compares platform inventories, and
checks the unpacked package against those inventories.

These are package-content and ABI-inventory checks. Runtime evidence comes
from the project's formal CTest suite; the pipeline does not generate
independent installed-consumer validation projects. The
[release workflow](../.github/workflows/native-sdk-release.yml) defines the
publication steps.

## Reporting validation

Retain the exact revision, resolved package versions/source refs, compiler,
triplet, preset, configure/build output and CTest results. Keep sanitizer,
package-content, host runtime and device runtime evidence distinct. A failed
restore or configure prevents the corresponding build/test claim; a targeted
local test does not establish that the complete suite passed.
