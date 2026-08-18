# TurboRaft

TurboRaft is a C11 Raft library for the TurboNet ecosystem. It separates a deterministic consensus core from CoroNet peer transport, segmented WAL durability, application state machines, and an optional TurboHTTP JSON-RPC/HTMX management plane.

The repository contains a native Ready-style Raft core, pre-vote and check-quorum elections, batched replication, ReadIndex, leadership transfer, learners and Joint Consensus, segmented WAL recovery, resumable snapshot transport, durable Snapshot ConfState, live log compaction with lagging-peer snapshot recovery, CoroNet mTLS peer services, a single-owner Service loop, and an authenticated HTMX control plane. Production readiness still requires the chaos, fuzz, long-duration, rolling-upgrade, and platform validation defined in the implementation plan.

## Design goals

- Preserve Raft safety independently of network timing, process crashes, duplicate messages, and reordered delivery.
- Provide a stable C ABI with opaque handles and explicit ownership.
- Run each Raft group on one owner loop; perform no blocking disk I/O on that loop.
- Use CoroNet for authenticated peer streams and lifecycle management.
- Use TurboUtils file APIs for WAL and snapshot storage, with a small durability adapter for operations TurboUtils does not expose.
- Use TurboHTTP/Iris JSON-RPC only for administration and diagnostics.
- Support linearizable metadata for mesh namespace, rules, leases, and task ownership without putting file chunks in the Raft log.

## Documents

- [Architecture and contracts](docs/DESIGN.md)
- [Peer protocol draft](docs/PROTOCOL.md)
- [Implementation and validation plan](docs/PLAN.md)
- [Wire codec fuzzing](docs/FUZZING.md)
- [Multi-process chaos testing](docs/CHAOS_TESTING.md)
- [Rolling upgrades and rollback](docs/UPGRADES.md)
- [willemt/raft adoption assessment](docs/WILLEMT_RAFT_ASSESSMENT.md)

## Build and test

```powershell
cmake --preset win-release-user --fresh
cmake --build --preset win-release-user
ctest --preset win-release-user --output-on-failure
```

`TurboRaft::WalStorage` is the sole durable Raft storage backend. The CoroNet transport, snapshot manager, service owner, control plane, and console are built automatically when the TurboNet/TurboHttp packages are found; a configure without those packages still succeeds and builds the deterministic core, WAL storage, and text-syntax tooling.

## Installed package

```powershell
cmake --install build/msvc-release --prefix C:/projects/cpp/external/pkgs/turboraft
```

Consumers use the exported targets:

```cmake
find_package(TurboRaft CONFIG REQUIRED)
target_link_libraries(app PRIVATE TurboRaft::Service)
```

Installed storage is `TurboRaft::WalStorage`. Optional targets are `TurboRaft::CoroNet`, `TurboRaft::SnapshotManager`, `TurboRaft::ServiceOwner`, and `TurboRaft::ControlPlane` when the corresponding dependencies were found at configure time.

## Examples

The repository ships a small embeddable DSL demo (Query inspection plus a
deterministic 3-node Replay simulation):

```powershell
cmake --preset win-release-user -DBUILD_EXAMPLES=ON
cmake --build --preset win-release-user --target turboraft_dsl_embed_demo
build/msvc-release/bin/turboraft_dsl_embed_demo
```

The full usage guide for the text DSLs (tools, CMake linkage, C API
snippets, and limits) is in [docs/DSL_USAGE.md](docs/DSL_USAGE.md); syntax
details are in [docs/TEXT_SYNTAX.md](docs/TEXT_SYNTAX.md).

## References

- [Raft extended paper](https://raft.github.io/raft.pdf)
- [Raft dissertation](https://github.com/ongardie/dissertation)
- [Raft TLA+ specification](https://github.com/ongardie/raft.tla)
- [etcd Raft library design](https://github.com/etcd-io/raft)
- [willemt/raft](https://github.com/willemt/raft)
