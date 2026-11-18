# TurboRaft documentation

These documents describe the current source and SDK contract. Older SDKs,
mixed-version peers and downgrade compatibility are not supported. Start with
the [project README](../README.md) for dependency restoration and preset setup;
[the compatibility policy](PRE_1_0_COMPATIBILITY.md) defines the safety boundary.

## Architecture and application integration

| Document | Scope |
| --- | --- |
| [Design](DESIGN.md) | Module ownership, transport, shutdown and multicore orchestration |
| [Native core](NATIVE_CORE.md) | Deterministic consensus core and host responsibilities |
| [CFlow state machine](architecture/cflow-replicated-state-machine.md) | Ordered asynchronous apply, ownership, settlement and recovery |
| [TurboDB ORM boundary](architecture/turbodb-orm-state-machine.md) | Application-owned SQL transactions and durable applied markers |
| [High-performance replication](HIGH_PERFORMANCE_REPLICATION.md) | Bounded batching and replication contracts |
| [Joint consensus](JOINT_CONSENSUS.md) | Membership transitions, quorum and recovery |

## Protocols and storage

| Document | Scope |
| --- | --- |
| [Peer protocol](PROTOCOL.md) | Authentication, handshake and transport admission |
| [Wire protocol](WIRE_PROTOCOL.md) | Current envelope, limits and generated codec |
| [Data streams](DATA_STREAM.md) | Large-payload staging, durable ACKs and Raft descriptors |
| [Snapshot design](SNAPSHOT_DESIGN.md) | Snapshot storage, installation and compaction |
| [WAL storage](WAL_STORAGE.md) | Durable transactions, segment retention and recovery |
| [Recovery](RECOVERY.md) | Backup boundaries and startup/restore ordering |

## Administration and text APIs

| Document | Scope |
| --- | --- |
| [Control plane](CONTROL_PLANE.md) | Read-only HTTP/JSON-RPC administration |
| [Control audit](CONTROL_AUDIT.md) | Audit callbacks, ownership and error semantics |
| [Text syntax](TEXT_SYNTAX.md) | Query, protocol-debug and replay grammars |
| [DSL usage](DSL_USAGE.md) | CMake linking and C API examples |
| [Operations](OPERATIONS.md) | Backup, metrics and incident evidence |

## Build, verification and SDK contracts

| Document | Scope |
| --- | --- |
| [Platforms and dependencies](SUPPORTED_PLATFORMS.md) | Current CI matrix, SDK roots and package qualification |
| [Diagnostics](DIAGNOSTICS.md) | Sanitizer presets and instrumentation limits |
| [Chaos testing](CHAOS_TESTING.md) | Multiprocess scenarios and reproducible seed ranges |
| [Wire fuzz corpus](FUZZING.md) | Deterministic decoder regression coverage |
| [Performance](PERFORMANCE.md) | In-tree benchmarks and CTest execution |
| [Performance policy](PERFORMANCE_SLOS.md) | Implemented checks and requirements for future thresholds |
| [Pre-1.0 compatibility](PRE_1_0_COMPATIBILITY.md) | Current-only support and future stable ABI criteria |
| [Callback contracts](PUBLIC_CALLBACK_CONTRACTS.json) | Machine-readable public callback ownership contracts |

Workflow definitions and test registrations specify what runs; they do not
establish that a particular checkout has passed. Record the exact revision,
resolved dependencies, commands and results when reporting validation.
