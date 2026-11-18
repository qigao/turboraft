# TurboRaft

TurboRaft is a C11 Raft library built on the Salts package family. It separates
the deterministic consensus core from transport, durable WAL storage, snapshot
transfer, application state machines, and a standalone CHttp JSON-RPC/HTTP
control plane.

The repository includes pre-vote and check-quorum elections, batched
replication, ReadIndex, leadership transfer, learners, Joint Consensus,
segmented WAL recovery, resumable snapshots, durable snapshot configuration,
live log compaction, and a caller-driven CNet adapter.

An optional [multicore runtime](include/turboraft/raft_multicore.h) assigns
multiple groups to fixed owner threads, with bounded request/completion queues.
[NodeConfig](include/turboraft/raft_node_config.h) loads strict, versioned JSON;
see the [design and configuration contract](docs/DESIGN.md#optional-multicore-orchestration-130)
and [durable counter example](examples/multicore_node.c).

Application database writes use [TurboDB ORM](docs/architecture/turbodb-orm-state-machine.md)
for SQL execution and transactions. Applications own the schema and atomically
commit business rows, entry identity and the applied marker through `Orm::C`.
Database drivers and dialects belong to TurboDB. Raft Core owns consensus.

## Ownership model

- `TurboRaft::Core` and `TurboRaft::Service` are single-owner state machines.
- `TurboRaft::Multicore` optionally creates owner threads. Each group stays on
  one owner; storage and state-machine callbacks run on that thread.
- `TurboRaft::CNet` exposes transport framing plus a bounded adapter around a
  caller-owned CNet client. It never starts an I/O thread.
- `TurboRaft::SnapshotManager` emits transport-neutral payloads through a
  bounded enqueue callback.
- `TurboRaft::ControlPlane` owns a JSON-RPC server from standalone
  `CHttp::App`. Its status provider runs on Application workers;
  use an executor or mailbox when Raft belongs to another thread.

Successful send admission keeps bytes in bounded local storage. CNet retains
immutable Salts buffer slices until terminal completion. Admission does not mean the remote peer received or persisted
the bytes.

## Dependencies

Source builds require installations provided through:

- `SALTS_BASE_VERSION` / `SALTS_BASE_ROOT`
- `CMETA_VERSION` / `CMETA_ROOT`
- `SALTS_CNET_VERSION` / `SALTS_CNET_ROOT`
- `SALTS_UTILS_VERSION` / `SALTS_UTILS_ROOT`

Load the required non-secret assignments from local `.env` into the invoking
process. [User environments](presets/UserEnvironments.json) pass those version
and root values through to CMake. CMake does not parse `.env` or choose another
SDK generation. Dependency versions use `EXACT`, and roots use
`NO_DEFAULT_PATH` / `NO_CMAKE_FIND_ROOT_PATH`; missing inputs and version
mismatches fail configuration. Do not configure duplicate locations for these
split SDKs through `<Package>_DIR`.

Select CHttp and the test-only TurboDB release with `CHTTP_VERSION` and
`TURBODB_VERSION` in `.env`. Their SDKs use the `CHTTP_ROOT` and `TURBODB_ROOT` assignments
from the same file. Their CMake config packages and runtime library paths
come from those roots. Third-party libraries come from the selected
`VCPKG_INSTALLED_DIR` / `VCPKG_TARGET_TRIPLET` prefix. No duplicate `<Package>_DIR`
selectors or alternate dependency sources are configured.

Configure, build, test and install through the supplied user presets. Use
forward slashes in Windows root values. After changing an SDK, compiler or
triplet, run `cmake --fresh --preset <user-preset>` to discard previous package
discoveries. The project version remains TurboRaft's release version; it is
independent of dependency selectors.

The source build uses the split SaltsBase, CMeta, SaltsUtils and SaltsCNet
packages. CMeta exports `CMeta::CMeta`, `CMeta::CSTL` and `CMeta::CFlow`;
SaltsBase exports the Core, NativeIO, Coroutine and TinyTest targets.
SaltsCNet exports `Salts::Crypto`; snapshot transfers, data streams and WAL
snapshot verification use its `<crypto/sha256.h>` API.
CHttp and TurboDB must be built against the matching SDK generation and
compiler/triplet. The installed TurboRaft config imports its targets only; the
consuming project selects and finds its upstream dependencies from its own
loaded `.env` before importing those targets.

### Upstream dependency versions

The parent environment selects `VCPKG_CACHE_VERSION` (an exact release without
`v`) and `VCPKG_CACHE_REPOSITORY_ROOT` (a checkout of that tag). The current
selection in [.env.example](.env.example) is `0.1.3`. CMake does not load `.env`.
Keep credentials private and load only the required assignments in the invoking
process. The native CI matrix reads the version assignments synchronized from
that file through `TURBORAFT_CI_ENV`.

[Platform descriptors](cmake/dependencies/) are inputs to vcpkg-cache's shared
`resolve-release` command. Use that checkout's
`scripts/restore-native-dependency.py` to resolve an exact URL/SHA-256 lock,
then restore it with `locked`. Copy the returned `root` and matching triplet
into the existing `.env` assignments; CMake consumes those selections directly.
The removed repository-local `cmake/native_dependencies.py` wrapper must not be
restored. The [CI native-input action](.github/actions/setup-native-inputs/action.yml)
calls the shared CLI directly for resolution, download and restore.

After loading the selected `.env` assignments in the parent process:

```powershell
cmake --fresh --preset native-release-user
cmake --build --preset native-release-user
ctest --preset native-release-user
```

The selected `.env` supplies `VCPKG_INSTALLED_DIR`, `VCPKG_TARGET_TRIPLET`,
`RE2C_ROOT`, `LEMON_ROOT`, and the explicit host `QIGAO_HOST_RID`. User presets expose the
selected target prefix through `CMAKE_PREFIX_PATH`; TurboRaft configure does
not run vcpkg or use its toolchain to search other installations. Installation
directories include release, triplet/host and digest, preserving older SDKs.
When updating local `.env`, copy only these non-secret receipt values and keep
the matching compiler/triplet selection. Always fresh-configure after SDK,
compiler or triplet changes.

Both re2c and Lemon come from the pinned vcpkg-cache binary packages in
[re2c.lock.json](cmake/re2c.lock.json) and [lemon.lock.json](cmake/lemon.lock.json).
The Lemon executable and template are validated as one package for the selected
host. There is no repository-built Lemon or standalone command-line tools.

The split SDK selectors and the discovered CHttp/TurboDB packages must
describe one matching SDK closure. [.env.example](.env.example) lists the split
SDK selectors. Neither the removed monolithic `Salts` package nor a historical
SDK is a fallback for a missing split package. CI restores the native Salts SDKs and builds CHttp and TurboDB from their
selected tags using the same compiler and SDK roots. See the
[native matrix and version synchronization](docs/SUPPORTED_PLATFORMS.md).

CI also uses [sccache](https://github.com/mozilla/sccache) 0.16.0 (Apache-2.0)
through the shared [setup action](.github/actions/setup-sccache/action.yml).
It supports GCC, Clang and MSVC via CMake's C/C++ compiler launchers and stores
individual compilation results in the GitHub Actions cache backend. Cache
partitions include OS, architecture, compiler family, build configuration
(including sanitizer mode), and job profile. They remain stable across commits;
sccache checks compiler identity, flags, source and included headers before
reusing an object. CMake configuration, linking and the existing tests still run
on every invocation, and the same-run SDK artifact flow is preserved.

The upstream action prints cache statistics in the job log and summary. Compare
hits, misses, non-cacheable calls and build duration between a cold run and a
later commit to measure reuse of unaffected compilation units. GitHub cache
scope, eviction and service rate limits can reduce reuse; there is no promised
speedup. See the [backend documentation](https://github.com/mozilla/sccache/blob/v0.16.0/docs/GHA.md).
Current MSVC CI profiles use Release. Any future MSVC Debug/RelWithDebInfo cache
profile must select embedded debug information (`/Z7`, via CMP0141 and
`CMAKE_MSVC_DEBUG_INFORMATION_FORMAT` with CMake 3.25+) to avoid shared compiler
PDB files; see the [MSVC integration notes](https://github.com/mozilla/sccache#usage).
To invalidate the compiler cache, increment `turboraft-v1` in the setup action.
To roll back caching, remove the `setup-sccache` steps from the workflows;
the project presets and compiler settings require no changes.

Published producer SDK downloads use a separate
[NuGet payload cache](.github/actions/cache-native-packages/action.yml).
The shared SDK restore action resolves floating versions on every run with
`--no-cache --force-evaluate` and exports roots from that run's NuGet assets.
Only producer package contents are cached; dependency assets, authenticated
configs, build trees and the newly packed TurboRaft SDK are excluded. Matrix
jobs have independent save keys and compatible restore prefixes.

[ci.yml](.github/workflows/ci.yml) runs the complete Linux Release/sanitizer
and Windows Release graphs. The [native SDK pipeline](.github/workflows/native-sdk-pipeline.yml)
builds and tests before packaging, then checks the packed headers, callback
contract and archive symbol inventories. Existing source-tree CTest suites
provide runtime validation; no independent consumer validation projects are generated.

Snapshot and WAL SHA-256 use Salts `cmeta_crypto.h`, backed by GmSSL, with no
OpenSSL/BoringSSL link dependency.

### Source builds

All library modules are assembled unconditionally. [CMakeOptions.cmake](CMakeOptions.cmake)
provides `BUILD_TESTS`, `BUILD_BENCHMARKS`, and `BUILD_EXAMPLES`. Tests and
benchmarks register with CTest. Test builds require Python and the TurboDB ORM
SDK; required dependencies fail configuration instead of reducing the graph.

SaltsUtils supplies DataBind and `salts-idlc`, the build-time generator for the
the current wire schema. Generated sources live in the preset's build directory
and explicitly depend on their schema and compiler.

The production-qualified OS/compiler/dependency boundary is documented in
[`docs/SUPPORTED_PLATFORMS.md`](docs/SUPPORTED_PLATFORMS.md). A platform that
is not in that matrix may still compile, but it is not a TurboRaft 0.3.x
release gate until hosted CI proves the same runtime and package contracts.

Only the current source/SDK contract is supported; older releases and
mixed-version peers are not maintained. The pre-1.0 source/API policy and the
objective criteria for a future stable 1.0 C ABI are documented in
[`docs/PRE_1_0_COMPATIBILITY.md`](docs/PRE_1_0_COMPATIBILITY.md).

Operator backup, metrics, alerting, and incident evidence
procedures are documented in
[`docs/OPERATIONS.md`](docs/OPERATIONS.md).

```powershell
# Run inside a Visual Studio developer environment (VsDevCmd.bat).
cmake --preset msvc-release-user
cmake --build --preset msvc-release-user
ctest --preset msvc-release-user --output-on-failure

cmake --preset native-release-user
cmake --build --preset native-release-user
ctest --preset native-release-user --output-on-failure
```

Installed public targets include:

- `TurboRaft::Core`
- `TurboRaft::Service`
- `TurboRaft::Multicore`
- `TurboRaft::NodeConfig`
- `TurboRaft::SnapshotReceiver`
- `TurboRaft::SnapshotSender`
- `TurboRaft::SnapshotManager`
- `TurboRaft::CNet`
- `TurboRaft::ControlPlane`
- the text syntax and replay targets

The wire schema is generated by the SaltsUtils `salts-idlc`; DataBind remains
the schema-backed serialization layer.
