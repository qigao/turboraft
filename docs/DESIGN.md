# TurboRaft design

TurboRaft keeps consensus, persistence, transport, and administration as
separate ownership domains.

```text
application
    |
TurboRaft::Service ---- TurboRaft::WalStorage
    |
transport-neutral payloads
    +---- TurboRaft::CNet ---- caller-owned CNet poll loop

status snapshot provider ---- TurboRaft::ControlPlane ---- CHTTP/CRPC worker
```

## Consensus and storage

`TurboRaft::Core` is deterministic and contains no network, filesystem, or
threading API. `TurboRaft::Service` applies the persistence-before-send and
persistence-before-apply ordering around the core. WAL and snapshot storage use
Salts filesystem/buffer primitives and report durability failures.

The split SDK integration uses SaltsBase's `<file/fs.h>` and
SaltsCNet's `<crypto/sha256.h>`. Crypto provider state remains private to Salts.
TurboRaft links `Salts::Crypto` instead of carrying a second direct crypto
backend dependency or exposing provider context layouts. SHA-256 remains the
32-byte wire and snapshot digest. Each receiver embeds one caller-owned,
fixed-size incremental context and initializes it before a new transfer.
Temporary WAL digest contexts live on the call stack. These contexts have no
heap ownership, are never copied after initialization, and require no separate
destruction. Crypto failures are translated at the Salts error-code boundary.

Core headers follow their owning directories: `<threads/threads.h>` supplies
the `sync_*` synchronization API, `<queue/ring_buffer.h>` supplies the existing
mutex-protected bipartite queues, and `<memory/buffer.h>` supplies retained
transport buffers. Filesystem operations use `fs_*`; durable publication state,
file-lock ownership, queue limits and shutdown ordering remain unchanged.

Generated wire bindings come from the installed `salts-idlc`. Each codec owns
one binding for the [current schema](../schema/turboraft_wire.schema). CMake
generates the C codec and CMeta native-binding metadata under the build tree's
`generated/wire` directory; checked-in generated copies and parallel historical
schemas are not used. See the [generation contract](../schema/README.md).

Service treats `SALTS_ENOSPC` from transport enqueue as bounded peer-local
backpressure. It pauses Core output for that peer and retains its exact unsent
Ready suffix plus a snapshot-request slot. Accepted messages are never replayed;
healthy peers continue under the normal quorum rules while the owner drains
each paused peer independently. Permanent transport errors fault Service.
Backup preparation waits until all staged work is empty. The
[Service API](../include/turboraft/raft_service.h) defines ownership, bounds and
peer delivery status queries.

## Wire boundary

`raft_transport.h` owns length-prefix framing, the validated current wire
contract, monotonic message IDs, cluster/source/destination validation, and
borrowed decode callbacks. It requires a completed peer handshake and does not
own a socket or event loop.

`TurboRaft::CNet` adds `tr_raft_cnet_peer_t`, a bounded queue around a borrowed
`cnet_client`. The caller installs its observer while connecting or accepting,
polls CNet, and calls `tr_raft_cnet_peer_step()` on the same owner thread.
Outbound frames are staged in Salts `mem_buffer_t` storage and admitted through
CNet's retained scatter/gather surface (`cnet_send_slicev`). TurboRaft does not
fall back to the copied `cnet_send` data path. Ordinary/control frames remain
one retained slice. `DATA_CHUNK` and non-empty `SNAPSHOT_CHUNK` frames use
two slices: the length/envelope/metadata prefix and the queue-owned payload
buffer. Snapshot metadata keeps the canonical TBE configuration, digest, and
chunk-data length prefixes in the first slice. The concatenated bytes are
identical to the contiguous wire encoders, but large chunk bytes are not copied
into the staging frame or CNet write storage.

Temporary send slices use CMeta static lifecycle declarations and
`cmeta_scope`. Reverse cleanup runs after success or any admission error;
CNet keeps its independently retained references on success. Rejection keeps
the queued payload and staged packet available for retry. Packet storage is
not mutated while a write is pending. The lifecycle change adds no thread,
allocation, queue capacity, or public API.

## Snapshot boundary

SnapshotManager depends only on `tr_raft_transport_payload_t` and an enqueue
callback. Snapshot bytes are copied into managed Salts buffers when retained by
CNet queues. Decoded bytes remain borrowed during the callback.

## Control boundary

`TurboRaft::ControlPlane` owns one CRPC server and registers `raft.status` at
`/raft/rpc`, plus `GET /raft/status`. Because CRPC owns a background CHTTP
worker, the application supplies a thread-safe status provider. When Raft is
owned by another thread, that provider must cross the boundary through a
bounded executor or mailbox; handlers never mutate Raft directly.

## Shutdown

1. Stop application producers.
2. Stop peer admission and close transport connections.
3. Continue driving CNet until accepted work is terminal.
4. Stop the CRPC/CHTTP server.
5. Destroy transport adapters, snapshot state, service, and storage.

Every capacity is explicit. Queue saturation is either retained in the bounded
Service Ready suffix or returned to the caller before consuming new input; it
is never converted into an unbounded allocation or silent drop.

## Optional multicore orchestration (#130)

`TurboRaft::Multicore` adds fixed owner threads above Service. Core, Runtime,
Service, wire and WAL formats retain their existing contracts. A group belongs
to exactly one owner for its entire lifetime. The copied assignment list is the
only routing authority. The runtime uses Salts threads, mutexes, condition
variables and fixed-storage rings; it does not introduce an executor framework.

| Boundary | Contract |
| --- | --- |
| Cross-thread unit | Inline request/inline completion, copied under the owner mutex; no borrowed payloads |
| Producers/consumers | Concurrent submitters, one executing owner; competing completion consumers |
| Ordering | Per-group admission FIFO, including failed/cancelled operations; no ordering across groups |
| Capacity | Per-group `capacity` credits include queued, executing and unread completed requests |
| Admission | `OK` reserves request and completion storage; full returns `ENOSPC` without side effects |
| Completion | Exactly once per admitted request; proposal receipt is Service's admission/durability result, not a promise of commit |
| Fairness | Each group receives at most `work_budget` requests per round; tick and network run between rounds |
| Ownership | Factory creates resources on their owner; runtime destroys Service before group cleanup, then owner cleanup |
| Failure | Startup waits for every owner and rolls all successful opens back on failure; failed factories clean their own partial objects |
| Shutdown | Stop admission, finish the current synchronous operation, cancel queued requests, close groups and transport on their owner, join |
| Observation | Group outstanding/queued/completed counts, saturating rejection count, background failure and terminal state |

Request/completion rings reserve `(capacity + 1) * sizeof(record)` bytes each
per group (one spare ring element), with checked multiplication and a 1 GiB
aggregate queue-storage ceiling before any thread is started. This excludes
Core logs, application state and network buffers. Consumption releases credits only when the caller takes the
completion. There is no per-submission allocation, unbounded completion list or
callback under a runtime lock. Destroy requires all external callers to have
quiesced; stop itself supports concurrent callers and retains completions.
Individual queued requests have no cancellation API in version 1; stop cancels
all unstarted requests with `ECANCELED`. Runtime callbacks cannot synchronously
stop/join their own threads (`EBUSY`). No forced thread termination is provided:
the host must bound storage, apply, network polling and cleanup callback time.
`wait_stopped(timeout_ms)` observes owner cleanup without joining or freeing
anything. A timeout leaves the handle alive and stopping; the caller can wait
again. A successful wait is followed by stop/destroy to join the OS threads.
A blocked storage callback delays other groups on the same owner. Separate
owners provide isolation; this is not a preemptive storage scheduler.

Monotonic time supplies elapsed Raft ticks; election timeout selection uses the
Salts OS random source within the configured inclusive range. Group tick failure
is recorded and stops further automatic ticks for that group; status remains
queryable, other requests report that failure. Network polling failure stops the
runtime because continued transport progress can no longer be guaranteed.

### Transport topology and compatibility

The runtime is transport-neutral. A group factory supplies storage, state
machine, recovery state and a transport enqueue callback. Owner open/poll/close
callbacks manage the host's transport resources on that owner thread. Incoming
messages may look up and mutate only Services on that same owner. Completed
handshakes and authentication remain the host's responsibility; JSON configuration
never establishes peer trust. A CNet adapter can use the existing caller-driven
poll/step contract. Parallel mutation of one group remains unsupported.

The new API and library are opt-in. Existing applications can migrate group
creation and transport progress into the factory callbacks, then replace direct
cross-thread Service calls with submit/take. Rolling back means stopping and
joining the runtime and opening the same durable storage through the original
single-owner API. No online migration, automatic rebalancing, hot reload, CPU
pinning, wire change or storage format change is introduced.

### Configuration version 1

`TurboRaft::NodeConfig` embeds [the canonical schema](../schema/turboraft_node.schema)
and loads it through DataBind's record API and canonical CMeta metadata.
There is no second field-descriptor table or custom JSON parser. The file and
memory loaders produce the same `tr_raft_node_settings_t` view; the programmatic
path calls `tr_raft_node_settings_validate` and runtime creation also validates
its copied topology. The document owns all arrays and strings exposed by the
settings view. Retain it until factories have finished using them, normally
until the runtime has stopped. Core bootstrap fields must be replaced with WAL
recovery state by the application, as shown in the example.

JSON is the supported file format for version 1. DataBind's JSON `_ex` binder
provides strict token and unknown-field preflight; the current YAML record API
does not expose equivalent strictness. YAML is therefore not advertised or
silently converted. The dynamic record route supports nested group/member lists
without introducing generated owning container types into the public API.

| Field | Default / validation |
| --- | --- |
| `version` | Required, exactly `1` |
| `node_id`, `cluster_id` | Required nonzero uint64 and UUID |
| `owner_count` | `1`; range 1..64, every owner must have at least one group |
| `capacity`, `work_budget` | 64, 8; per-group capacity 1..65536, budget 1..capacity |
| `tick_ms`, `idle_ms` | 100, 1; tick 1..60000, idle 1..tick |
| `groups` | Required nonempty list, at most 1024; unique nonzero `group_id`, zero-based `owner_index` |
| Group `voters`, `learners` | Required lists; nonzero distinct members, total at most 31, self must be present |
| Group `storage_path` | Required absolute WAL prefix, unique within this node; parent directory must exist when opening |
| Group `heartbeat_ticks`, `election_min_ticks`, `election_max_ticks` | 1, 10, 20; heartbeat < min <= max; timeout choice is inclusive |
| Group `max_log_entries` | 1024; positive, host must budget Core/WAL memory separately |
Transport endpoints, credentials, reconnect policies and peer authentication
are configured through the host transport API. Removed transport fields,
including `network_enabled`, `owners` and `config_epoch`, are rejected as unknown
fields; remove them from existing JSON and configure transport in the host's
factory callbacks. The node settings view no longer exposes transport owners.
This is a source/configuration break; migrate hosts and their configurations
together. Reverting requires the matching previous library, headers and config.
Raft wire and durable storage formats are unchanged by this removal.

Unknown fields (including nested fields), wrong token kinds, unsupported
versions, invalid assignments, null required values, embedded NUL in strings,
oversized documents (>1 MiB), relative resource paths and invalid capacities
fail. There is no environment interpolation or implicit environment override.
Paths are used as supplied; the loader performs no filesystem writes or secret
logging. Distinct textual WAL prefixes are checked here; WAL locking remains
the authority for aliases/symlinks naming the same storage. Semantic validation
returns a Salts status; DataBind errors additionally describe schema/JSON
failures. CPU affinity and hot reload are unsupported fields and are rejected.

### Running and validating

The [example configuration](../examples/multicore_node.json) runs four durable
single-node groups on two owners. Replace its WAL prefixes with absolute paths
whose parent directory exists (for example `C:/data/raft/group1` on Windows).
`turboraft_multicore_node node.json` starts the owners, replays each committed
WAL, appends one increment per group, prints each total and stops. Running it
again increments the recovered totals. The counter is reconstructed from the
WAL. This example supports only single-member groups and rejects snapshot
recovery; networked hosts supply their own owner transport callbacks and
state machine/SnapshotManager integration.

Windows commands (inside the VS developer environment with matching SDKs):

```powershell
cmake --fresh --preset msvc-release-user -DBUILD_EXAMPLES=ON -DBUILD_BENCHMARKS=ON
cmake --build --preset msvc-release-user
ctest --preset msvc-release-user -R 'turboraft\.(multicore|node_config)' --output-on-failure
ctest --preset msvc-release-user -R '^turboraft\.benchmark\.multicore$' -V
```

Linux uses `gcc-release-user` with the same feature values. Examples and
benchmarks are part of the complete configured graph. Tests
cover owner affinity, 1/2/4 owners, concurrent producers, per-producer FIFO,
payload copying, ring wrap, reserved completion credits, stall isolation,
concurrent stop, partial factory failure, strict config,
file loading, WAL replay and startup rollback releasing WAL locks. Existing
transport admission/handshake and snapshot-isolation suites remain applicable.
Stop cancels unexecuted local requests and closes transport queues; it is not a
cluster-wide commit/drain barrier. Previously admitted durable proposals can
still be committed after restart, so clients must resolve their Service receipts
instead of interpreting a local shutdown as a replicated cancellation.

The multicore benchmark uses in-memory storage/state machines and a bounded
proposal workload. Its output does not establish disk/network throughput or
linear scaling with owner count. See [Performance](PERFORMANCE.md) for CTest
entry points and measurement provenance, and [Diagnostics](DIAGNOSTICS.md) for
instrumented validation. A Release test run does not establish sanitizer
coverage.
