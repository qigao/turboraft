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
    +---- TurboRaft::FlowMQ -- caller-owned FlowMQ step loop

status snapshot provider ---- TurboRaft::ControlPlane ---- CHTTP/CRPC worker
```

## Consensus and storage

`TurboRaft::Core` is deterministic and contains no network, filesystem, or
threading API. `TurboRaft::Service` applies the persistence-before-send and
persistence-before-apply ordering around the core. WAL and snapshot storage use
Salts filesystem/buffer primitives and report durability failures.

The Salts 2.1 / SaltsUtils 4.2 integration uses `cmeta_fs.h` and
`cmeta_crypto.h`. Crypto provider state remains private to Salts, whose backend
is GmSSL. This reuses the installed SDK instead of carrying a second direct
OpenSSL dependency or exposing GmSSL context layouts in TurboRaft.
SHA-256 remains the same 32-byte wire and snapshot digest: no data migration,
algorithm change, or plaintext/TLS fallback is introduced. Each receiver owns
one incremental context, resets it before a new transfer, and destroys it with
the receiver. WAL and preflight operations release their temporary context on
success and every error exit. Context creation can fail with `SALTS_ENOMEM`;
the receiver is not published on failure. This adds one bounded allocation per
receiver and per WAL digest operation; no performance improvement is claimed.
Known digest vectors, streaming/reset cases, WAL corruption and recovery tests
remain the compatibility evidence. A provider rollback must preserve those
digests and SDK contracts; existing stored data needs no conversion.

Generated wire bindings come from the installed `salts-idlc`. Each wire codec
owns separate v2 and v3 schema bindings; v3 encoding passes its binding to the
generated serializer. Failed codec construction releases the earlier binding.
The schema files remain the source of wire layout, and generated CMeta
reflection/native-binding metadata is regenerated with the codec sources.

Service treats `SALTS_ENOSPC` from a transport enqueue as bounded local
backpressure rather than a permanent fault. It retains only the unsent suffix
of the current Ready (at most `TR_RAFT_MAX_VOTERS` messages and
`TR_RAFT_MAX_MEMBERS` snapshot requests) and admits no new Core input until that
suffix drains. Already accepted messages are never replayed. A later owner call
returns `SALTS_ENOSPC` without consuming its input while the transport remains
full; every other transport error keeps the fail-fast Service fault behavior.

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

`TurboRaft::FlowMQ` uses one ROUTER plus one DEALER per peer. No connector
thread, mutex handoff, hidden poller, or second ingress queue exists. The
service processes bounded receive and send batches from `step()` and retains a
payload until FlowMQ copies it successfully.

## Snapshot boundary

SnapshotManager depends only on `tr_raft_transport_payload_t` and an enqueue
callback. Snapshot bytes are copied into managed Salts buffers when retained by
CNet or FlowMQ queues. Decoded bytes remain borrowed during the callback.

## Control boundary

`TurboRaft::ControlPlane` owns one CRPC server and registers `raft.status` at
`/raft/rpc`, plus `GET /raft/status`. Because CRPC owns a background CHTTP
worker, the application supplies a thread-safe status provider. When Raft is
owned by another thread, that provider must cross the boundary through a
bounded executor or mailbox; handlers never mutate Raft directly.

## Shutdown

1. Stop application producers.
2. Stop peer admission and close transport connections.
3. Continue driving CNet/FlowMQ until accepted work is terminal.
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

Version 1 selects **one independent FlowMQ peer-service per owner**, with a
distinct bind endpoint and its own context/ROUTER/DEALER set. Peers must use the
same group-to-lane routing topology. A group factory supplies its own storage,
state machine and recovery configuration; the runtime does not fabricate a
completed handshake or share a WAL handle between threads. Owner callbacks may
look up only Services on that same owner; incoming messages never cross into
another owner's Service directly. Existing peer-service authentication, limits,
TLS hostname/CA checks and certificate-to-identity bindings remain authoritative.

Alternatives considered:

- A dedicated ingress owner would save listener endpoints but adds copies,
  inbound/outbound queue credits, authenticated-envelope ownership and another
  failure boundary. It is not necessary for the initial fixed topology.
- FlowMQ 1.2.1 owner lanes do not support listener bind. An ordinary ROUTER
  cannot be passed to `flowmq_owner_poll`. Splitting DEALERs onto explicit lanes
  while polling the ROUTER separately adds two progress paths without combining
  waits, so this version retains ordinary `flowmq_poll` on each independent
  owner. A unified wait requires an upstream listener contract first.
- Parallel mutation of a single group would break Core/Service single-owner
  invariants and is rejected. Scale multiple groups across owners instead.

Connection and queue costs grow with owner count: for `O` owners and `P` remote
nodes, each local node has `O` listeners and `O * P` outbound connections, plus
FlowMQ's bounded per-peer/group queues. Throughput is a measurement, not an
assumed linear function of owners. Slow peers continue to use Service's existing
per-peer bounded suffix retention.

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
silently converted. TLS fields are flat `tls_*` fields on owners and peers:
the installed DataBind schema contract restricts nested composite records to
fixed-size fields. The dynamic record route supports the required nested lists
without introducing generated owning container types into the public API.

| Field | Default / validation |
| --- | --- |
| `version` | Required, exactly `1` |
| `node_id`, `cluster_id` | Required nonzero uint64 and UUID |
| `config_epoch` | `1`; passed to the peer handshake configuration |
| `owner_count` | `1`; range 1..64, every owner must have at least one group |
| `capacity`, `work_budget` | 64, 8; per-group capacity 1..65536, budget 1..capacity |
| `tick_ms`, `idle_ms` | 100, 1; tick 1..60000, idle 1..tick |
| `groups` | Required nonempty list, at most 1024; unique nonzero `group_id`, zero-based `owner_index` |
| Group `voters`, `learners` | Required lists; nonzero distinct members, total at most 31, self must be present |
| Group `storage_path` | Required absolute WAL prefix, unique within this node; parent directory must exist when opening |
| Group `heartbeat_ticks`, `election_min_ticks`, `election_max_ticks` | 1, 10, 20; heartbeat < min <= max; timeout choice is inclusive |
| Group `max_log_entries` | 1024; positive, host must budget Core/WAL memory separately |
| `network_enabled`, `owners` | false; disabled requires an empty owners list and only local membership; enabled requires exactly owner_count entries |
| Owner `bind_endpoint`, `identity`, `peers` | Required when networking is enabled; distinct bind endpoints, nonempty identity <=255 bytes, 1..30 peers |
| Peer `node_id`, `identity`, `endpoint`, `client_certificate_sha256` | Required; unique nonlocal ID/identity within the owner; configured membership must resolve to its owner's peers |
| Owner/peer `tls_ca_file`, `tls_cert_file`, `tls_key_file`, `tls_server_name` | Optional strings; TLS requires absolute CA/cert/key paths, clients require a hostname |
| `tls_require_client_certificate` | true; TLS listeners require it; 1..4 canonical lowercase `sha256:` fingerprints per peer |
| `network_batch` | 16, range 1..65536 |
| `network_queue_items`, `network_queue_bytes` | 256, 4194304; explicit per-link HWM, byte HWM must fit the maximum frame |
| `reconnect_initial_ms`, `reconnect_max_ms` | 10, 1000; positive initial <= max |
| `heartbeat_interval_ms`, `heartbeat_timeout_ms` | 1000, 5000; positive interval < timeout |

Network settings apply to each owner independently. Runtime capacity supplies
the per-group network item limit; it cannot exceed network_queue_items.
Per-link active-group capacity is derived from the immutable assignment list.
Version 1 network plans require uniform TCP or TLS endpoints per owner; TCP
rejects TLS credentials/fingerprints. File configuration cannot supply a
handshake result or process incarnation. A deployment must provide authentic
completed negotiation and a fresh process incarnation before invoking
`tr_raft_flowmq_owner_create`; JSON never establishes peer trust. Existing TLS
validation checks actual certificate files at transport startup, with rollback
if any owner fails. Passwords are not file-schema fields; a host may pass them
through the existing programmatic TLS adapter.

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
WAL, and this example explicitly rejects networking and snapshot recovery;
networked hosts use the FlowMQ owner adapter and their existing state machine
and SnapshotManager callbacks. `multicore_fixture.h` exercises that adapter
with two nodes, four owners each, eight groups, TCP and authenticated TLS.

Windows commands (inside the VS developer environment):

```powershell
cmake --preset win-release-user
cmake --build --preset win-release-user --parallel 4
ctest --preset win-release-user -R 'turboraft\.(multicore|node_config)' --output-on-failure
cmake --preset win-benchmarks-user
cmake --build --preset win-benchmarks-user --parallel 4
ctest --preset win-benchmarks-user -R turboraft.benchmark.multicore -V
```

The benchmark preset also builds the standalone example. Linux uses the
corresponding `linux-release-user` and `linux-benchmarks-user` presets. Tests
cover owner affinity, 1/2/4 owners, concurrent producers, per-producer FIFO,
payload copying, ring wrap, reserved completion credits, stall isolation,
concurrent stop, partial factory failure, TCP/TLS replication, strict config,
file loading, WAL replay and startup rollback releasing WAL locks. Existing
transport identity/reconnect and snapshot-isolation suites remain applicable.
Stop cancels unexecuted local requests and closes transport queues; it is not a
cluster-wide commit/drain barrier. Previously admitted durable proposals can
still be committed after restart, so clients must resolve their Service receipts
instead of interpreting a local shutdown as a replicated cancellation.

Initial benchmark on 2026-10-07: Windows x64, MSVC Release, Ryzen 9 7940HX
(16 cores/32 logical processors), Salts 2.1.1, SaltsUtils 4.2.0, FlowMQ 1.2.1.
Each case uses eight single-voter groups, eight pending proposals per group,
262144 total 8-byte proposals and an in-memory test storage/state machine.
Startup/teardown is outside timing; checks and completion collection are inside.
P99 measures submission-to-observation including local queue residence. CPU is
process CPU time divided by elapsed time (cores occupied), including the single
producer/collector; peak RSS is the process high-water mark, not incremental
runtime memory. Queue bytes are calculated from public request/completion sizes.

| Owners | Proposals/s | P99 (µs) | CPU cores | Peak RSS (MiB) | Queue bytes |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 895561 | 158.0 | 1.98 | 146.0 | 348480 |
| 2 | 1231891 | 101.0 | 2.86 | 146.1 | 348480 |
| 4 | 1060052 | 84.3 | 3.03 | 146.2 | 348480 |

These are one local run, not disk/network throughput or a scaling guarantee.
The benchmark has one submitting/collecting thread and short synchronous
operations; four owners reduced P99 but had lower throughput than two. This
suggests measuring producer/queue contention before further optimization; it is
not profiling evidence identifying a unique bottleneck. ASan/TSan must still be
run on a supported instrumented toolchain; the local Windows Release run does
not establish sanitizer coverage.
