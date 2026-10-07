# TurboRaft

TurboRaft is a C11 Raft library built on the Salts package family. It separates
the deterministic consensus core from transport, durable WAL storage, snapshot
transfer, application state machines, and a standalone CHttp JSON-RPC/HTTP
control plane.

The repository includes pre-vote and check-quorum elections, batched
replication, ReadIndex, leadership transfer, learners, Joint Consensus,
segmented WAL recovery, resumable snapshots, durable snapshot configuration,
live log compaction, a caller-driven CNet adapter, and a caller-driven FlowMQ
ROUTER/DEALER peer service.

An optional [multicore runtime](include/turboraft/raft_multicore.h) assigns
multiple groups to fixed owner threads, with bounded request/completion queues.
[NodeConfig](include/turboraft/raft_node_config.h) loads strict, versioned JSON;
see the [design and configuration contract](docs/DESIGN.md#optional-multicore-orchestration-130)
and [durable counter example](examples/multicore_node.c).

## Ownership model

- `TurboRaft::Core` and `TurboRaft::Service` are single-owner state machines.
- `TurboRaft::Multicore` optionally creates owner threads. Each group stays on
  one owner; storage and state-machine callbacks run on that thread.
- `TurboRaft::CNet` exposes transport framing plus a bounded adapter around a
  caller-owned CNet client. It never starts an I/O thread.
- `TurboRaft::FlowMQ` owns one FlowMQ context, one ROUTER, and one DEALER per
  peer. `tr_raft_flowmq_peer_service_step()` drives all progress on the caller's
  owner thread.
- `TurboRaft::SnapshotManager` emits transport-neutral payloads through a
  bounded enqueue callback.
- `TurboRaft::ControlPlane` when a standalone CHttp SDK is configured owns a JSON-RPC server from standalone
  `CHttp::Server`. Its status provider is an explicit cross-owner boundary;
  use an executor or mailbox when Raft belongs to another thread.

Successful send admission keeps bytes in bounded local storage. CNet retains
immutable Salts buffer slices until terminal completion; FlowMQ copies into its
owned storage. Admission does not mean the remote peer received or persisted
the bytes.

## Dependencies

Configure requires active-profile installations provided through:

- `SALTS_ROOT`
- `SALTS_UTILS_ROOT`
- `FLOWMQ_ROOT`
- `CHTTP_ROOT` when building or consuming `TurboRaft::ControlPlane`

The supplied user presets resolve Debug and Release profiles independently and
use `NO_DEFAULT_PATH` for first-party package discovery. Salts 2.1+ and
SaltsUtils 4.2+ are required. Source integration gates follow current first-party
branches; native package builds resolve current published SDKs and record the
resolved versions in each SDK manifest. FlowMQ must be built against the same
Salts/SaltsUtils generation; an older SDK that imports `Salts::TbeSchema` is
incompatible with SaltsUtils 4.2.

Third-party dependencies use the manifest and the shared
`qigao/vcpkg-cache` toolchain. The registry reference and baseline are pinned
together in `vcpkg-configuration.json`. User presets preserve the read-only
NuGet feed and writable local cache; provide `GITHUB_TOKEN` with package-read
access in the parent environment when restoring from the shared feed.
Snapshot and WAL SHA-256 use Salts `cmeta_crypto.h`, backed by GmSSL, with no
OpenSSL/BoringSSL link dependency.

### Source build profiles

`TURBORAFT_BUILD_PROFILE` makes source-only dependency boundaries explicit:

- `full` (default) builds the complete SDK, transport, FlowMQ integration,
  text/replay syntax, optional ControlPlane, tests and release surfaces. It
  requires `FLOWMQ_ROOT`, re2c, and the repository Lemon host tool.
- `core-dev` builds `TurboRaft::Core`, `TurboRaft::Service` and the optional
  orchestration library `TurboRaft::Multicore`. It
  requires `SALTS_ROOT` and `SALTS_UTILS_ROOT`, but not `FLOWMQ_ROOT`,
  `CHTTP_ROOT`, re2c, or Lemon.
- `storage-dev` extends `core-dev` with snapshot receiver/sender,
  `DataStream`, and `WalStorage`. It additionally resolves xxHash, but still
  does not require FlowMQ, CHttp, or text parser tools. The
  `win-storage-user` configure/build preset exposes this profile in a separate
  build tree and installs, when requested, to `turboraft-storage/release`.

Focused profiles are library profiles; tests, benchmarks, examples, fuzzers,
and database fixtures remain full-profile surfaces and fail fast if requested
with a focused profile.

SaltsUtils is intentionally still required by every profile: Core links the
installed DataBind target and source builds use the SDK's `salts-idlc` as a
build-time generator for the Raft wire schema. No checked-in-code or legacy
fallback is used in place of that compiler.

The production-qualified OS/compiler/dependency boundary is documented in
[`docs/SUPPORTED_PLATFORMS.md`](docs/SUPPORTED_PLATFORMS.md). A platform that
is not in that matrix may still compile, but it is not a TurboRaft 0.3.x
release gate until hosted CI proves the same runtime and package contracts.

The pre-1.0 source/API policy and the objective criteria for a stable 1.0 C
ABI are documented in
[`docs/PRE_1_0_COMPATIBILITY.md`](docs/PRE_1_0_COMPATIBILITY.md).

Operator backup, certificate-rotation, metrics, alerting, and incident-bundle
procedures are documented in
[`docs/OPERATIONS.md`](docs/OPERATIONS.md).

```powershell
# Run inside a Visual Studio developer environment (VsDevCmd.bat).
cmake --preset win-dev-user
cmake --build --preset win-dev-user
ctest --preset win-dev-user --output-on-failure

cmake --preset win-release-user
cmake --build --preset win-release-user
ctest --preset win-release-user --output-on-failure
```

Installed public targets include:

- `TurboRaft::Core`
- `TurboRaft::Service`
- `TurboRaft::Multicore`
- `TurboRaft::NodeConfig` in the full profile
- `TurboRaft::SnapshotReceiver`
- `TurboRaft::SnapshotSender`
- `TurboRaft::SnapshotManager`
- `TurboRaft::CNet`
- `TurboRaft::FlowMQ` when FlowMQ is configured
- `TurboRaft::ControlPlane`
- the text syntax and replay targets

The wire schema is generated by the SaltsUtils `tbe_compiler`; DataBind remains
the schema-backed serialization layer.
