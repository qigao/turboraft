# Platforms, dependencies and qualification

This document describes the matrix declared by the current workflows. A
workflow definition is not evidence that the current revision passed; report
actual run results with their source revision and resolved dependency versions.
Only the current source and SDK contract is maintained. Older-release restores,
mixed-version peers and downgrade qualification are not supported; see
[the compatibility policy](PRE_1_0_COMPATIBILITY.md).

## Continuous integration matrix

The [CI workflow](../.github/workflows/ci.yml) prepares native dependency inputs
and configures each lane's complete graph before building and running CTest.

| Lane | Runner / toolchain | Configure, build and test preset |
| --- | --- | --- |
| Linux Release | `ubuntu-24.04`, GCC, Ninja | `ci-gcc-release-user` |
| Linux ASan + UBSan | `ubuntu-24.04`, Clang Debug, Ninja | `ci-llvm-native-asan-ubsan-user` |
| Linux TSan | `ubuntu-24.04`, Clang Debug, Ninja | `ci-llvm-native-tsan-user` |
| Windows Release | `windows-2025`, MSVC through `VsDevCmd`, Ninja | `ci-msvc-release-user` |

The [Windows workflow](../.github/workflows/windows-msvc-release.yml) is reusable
and also installs the configured SDK. It explicitly builds CHttp
from its selected source refs; those builds are declared inputs, not a fallback
from a failed SDK restore. The Linux lanes restore the published dependency
SDKs. Exact source refs and package selection belong to the workflows and
[Native package project](../packaging/nuget/TurboRaft.Native.csproj), rather than
a second version list in this document.

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
uses the shared vcpkg-cache release resolver and locked restoration through
[native_dependencies.py](../cmake/native_dependencies.py). Platform descriptions
resolve into URL/SHA-256 locks before restoration; restored roots come from
receipts.
Missing release inputs, unavailable assets or digest mismatches fail instead of
selecting another dependency source.

Target libraries and host tools are distinct inputs. re2c and Lemon come from
the selected vcpkg-cache host packages via `RE2C_ROOT` and `LEMON_ROOT`; Lemon
uses its paired `lempar.c`. The installed SaltsUtils SDK supplies `salts-idlc`.
The current [wire schema](../schema/turboraft_wire.schema) generates one C
binding in the build tree; generated sources are not checked in. See the
[schema documentation](../schema/README.md) and [text grammar build](TEXT_SYNTAX.md#build).

The [SDK action](../.github/actions/setup-native-sdks/action.yml) restores
published packages and [exports SDK roots](../cmake/export_native_sdks.py) from
the resolved NuGet assets. The root build requires the selected SaltsBase,
CMeta, SaltsUtils, SaltsCNet, CHttp and third-party packages.
`BUILD_TESTS=ON` also requires TurboDB ORM and its SQLite fixture driver.
[User presets](../presets/UserEnvironments.json) pass the loaded
`*_VERSION` / `*_ROOT` selectors for SaltsBase, CMeta, SaltsUtils and SaltsCNet
to [CMakeLists.txt](../CMakeLists.txt), which uses exact versions and explicit
roots for those SDKs. CHttp and test-only TurboDB use required CMake
config-package discovery through `CMAKE_PREFIX_PATH` or their `<Package>_DIR`
cache entries. Missing packages, mismatched split-SDK versions or a different
SDK contract fail configuration.

The existing NuGet/source workflow inputs still describe the former monolithic
SDK generation. They need matching split-SDK producer/restoration inputs before
those lanes can qualify the new configuration. Their declarations above do not
establish a passing run of the environment-selected graph.

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

Linux Release uses `gcc-release-user`; sanitizer commands and instrumentation
limits are in [Diagnostics](DIAGNOSTICS.md). Tests are controlled by
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
[package documentation](../packaging/nuget/README.md) defines artifacts and
consumption, and the [compatibility policy](PRE_1_0_COMPATIBILITY.md) explains
the current inventory's limits.

## Reporting validation

Retain the exact revision, resolved package versions/source refs, compiler,
triplet, preset, configure/build output and CTest results. Keep sanitizer,
package-content, host runtime and device runtime evidence distinct. A failed
restore or configure prevents the corresponding build/test claim; a targeted
local test does not establish that the complete suite passed.
