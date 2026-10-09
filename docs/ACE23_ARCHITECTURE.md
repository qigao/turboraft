# TurboRaft Next: CMeta / CNet / ACE integration (Salts 2.3)

Status: **design and isolated migration**, not a released SDK or implemented hot reload.
Tracked by [#142](https://github.com/qigao/turboraft/issues/142)
and [#143–#147](https://github.com/qigao/turboraft/issues/143).
Upstream is the Salts 2.3.0 Component/ACE release gate
[qigao/salts#1018](https://github.com/qigao/salts/issues/1018);
the Salts [Draft PR #1013](https://github.com/qigao/salts/pull/1013)
is **DO NOT MERGE / DO NOT PUBLISH**. A functionally frozen snapshot is
not an installed, qualified final package. Later POSA2 Interceptor/ACT/Monitor
changes are not covered by the earlier functional-freeze result.

## 1. Architecture and responsibility

```text
typed node/cluster settings (DataBind + immutable CMeta DataDesc)
                   |
             exact provider selection
                   |
    Salts::Component bounded per-domain graph
          |                 |
          |             ComponentPlugin / Plugin
          |              (ONLY if DSO is needed)
          |                 |
      node/owner domain: one listener, WAL writer, thread owner
          |                 |
    CNet / NativeIO           CHttp / CRPC
    accept/connector         control-plane HTTP owner
    TLS identity + SG        status via bounded read snapshot
          |
    final owning Raft lane
    Multicore bounded inbox -> Service -> Core -> Ready
                           |       |
                   transactional WAL   StateMachine apply/settlement
                           |
                  snapshot/stream transfer
```

Only the Raft Core owns consensus decisions. The current
`tr_raft_runtime_process`, `tr_raft_service_step`, and
`tr_raft_apply_runtime_poll` implement ordered durability and apply.
Component construction, CNet callbacks and ACE dispatch **may not**
advance commit/applied indices or replay a partial Ready outside these
owners.

### Canonical pattern mapping

| ACE / POSA | Canonical mechanism | TurboRaft boundary |
| --- | --- | --- |
| Component/Service Configurator | CMeta descriptors + Salts::Component exact DAG | Node/owner boot and stop, static adapters |
| Strategy, Factory, Adapter, Extension Interface | CMeta typed Interface, FunctionAbi, ObjectRef | Storage, Transport, Apply, Status provider declarations |
| Acceptor–Connector, Reactor/Proactor | existing CNet / NativeIO | Listener, authenticated peers, I/O completion |
| Half-Sync/Half-Async, Active Object | existing Multicore fixed-owner bounded ring; CFlow typed facade only if justified | Caller/HTTP/transport ingress to Raft owner |
| Scoped Locking, Thread-Safe Interface | existing CMeta/Platform mutex policy | Cross-owner queue and lifecycle-only synchronization |
| Plugin module lease, generation | existing Salts::Plugin / ComponentPlugin | Optional replaceable providers, never raw WAL state |

ACE *Leader/Followers* is a scheduling pattern, not Raft leader
election. Do not retrofit it into Core voting.

## 2. Typed Component graph

Salts 2.3's existing `cmeta_component`, `cmeta_provides`,
`cmeta_requires`, `cmeta_component_meta`, `SALTS_COMPONENT_CONTEXT_INIT`,
`salts_component_context_init/resolve/start/stop` are the canonical
declaration/graph machinery; do **not** introduce a TurboRaft-global mutable
descriptor registry.

Candidate real domain capabilities, with exact CMeta Interface metadata
to be authored in [#144](https://github.com/qigao/turboraft/issues/144):

- RaftStorage/WAL: wraps `tr_raft_storage_t`; one domain owns the
  writer/file lock and the exact durability transaction.
- RaftTransport: wraps `tr_raft_transport_t` / `tr_raft_cnet_peer_t`;
  `enqueue` means bounded local acceptance, **not** remote delivery.
- RaftStateMachine: wraps `tr_raft_state_machine_t` or the explicit
  `tr_raft_entry_state_machine_v1` settlement SPI. Application owns
  business rows and applied marker.
- RaftOwner: maps assigned group to one `tr_raft_service_t`; exposes
  bounded submit/completion, never a mutable borrowed service to other
  threads.
- RaftReadStatus: an immutable/copy snapshot accessible by CHttp owner.
- RaftNode: composition of owners, transport service, control-plane and
  optional ShardGroup policy. ShardGroup SQL/placement belongs above Core.

These are design-level identities, **not existing API symbols**. The
implementation should preserve ordinary typed C calls on the hot path,
compile-time method-signature checks and exact Interface ABI admission.
First implement static providers. Dynamic ones are conditional on a
real restartable requirement and correct Plugin lifetime.

Bounded arrays/dependency edges are caller-owned; choose capacities from
the configured owner/group ceiling. Reject cyclic, absent, ambiguous or
wrong-ABI bindings before allocating sockets or modifying WAL.

## 3. Ownership, startup, replacement and teardown

```text
ZERO -> DESCRIBED -> RESOLVED -> CREATED -> ACTIVE -> DRAINING -> STOPPED
                            \-> FAILED (exactly-once rollback)
```

Owner-domain lifetime is longer than any dynamic Component generation.
Network listener and WAL writer are singleton domain resources. There is
no second open/bind during provider-generation candidate construction.

If a provider is actually hot-swappable:

1. Validate exact 2.3 component ABI and provider manifest; reserve bounded
   storage and Plugin lease for N+1. Keep N published.
2. Resolve DAG and prepare N+1 without opening another writer/listener
   or replacing a live Core/WAL state machine.
3. Publish N+1 in ComponentPlugin, but fence domain's exclusive I/O by
   a separately serialized epoch before it is admitted.
4. New scopes use the new provider. N stops accepting exclusive work,
   while outstanding N scopes retain provider code/storage until completion.
5. Drain N, stop components in reverse order, destroy ObjectRefs, *then*
   release Plugin leases. Final domain owner closes listener/writer once.

`salts_component_plugin_scope` is address-stable, noncopyable and
thread-exclusive. Its release can fail and must be handled explicitly.
At most two generations are attached. Raft term, membership generation,
HashSlot ownership epoch and plugin generation are independent authorities.
A change to Raft members/ShardGroup slot ownership is **not** a component
hot reload and still requires the authoritative Raft protocol.

No automatic settlement retry, backup-to-live raw runtime replay,
cross-epoch borrowed pointer, source-level v0.2 compatibility path or
fallback provider ranking.

## 4. CNet-only end state

Current `raft_cnet_peer.c` already demonstrates bounded group queue,
retained `mem_slice_t` scatter/gather, exactly-once release on terminal send,
and caller-driven CNet observer. It still lacks a full equivalent of the
active `raft_flowmq_peer_service.c` path.

[#145](https://github.com/qigao/turboraft/issues/145) delivers one CNet
Acceptor–Connector/peer-service on final owning threads:

- Listener accepts and uses `cnet_handoff` credit/ticket ownership to
  hand accepted TCP connections to the designated CNet owner.
- CNet/NativeIO alone drives epoll/kqueue/IOCP and TLS.
  Authenticate peer node and certificate/exporter binding before Raft
  HELLO or any group payload enters the Service owner.
- One connection has one final CNet owner; group-to-Raft-owner routing uses
  bounded mailbox/transfer and an explicit admission-credit budget.
- Snapshot and data-stream frames preserve their own bounded buffers and
  cancellation; peer-local full queues stop only that peer, not unrelated quorum.
- Reconnect/stop/half-close retain queued bytes until a documented terminal,
  without resending already accepted local work in an invisible retry loop.

Only after real loopback TLS, identity negative tests, SG terminal tests,
multiple groups, snapshot catch-up, per-peer fairness and loss/reconnect
qualification should `TurboRaft::FlowMQ`, its source/targets, and
`FlowMQ.Native` be **deleted in one breaking cutover**. Do not ship two
runtime transports as a silent fallback. The initial research branch still
contains the existing FlowMQ implementation because this parity is unproven.

### Executed ACE 2.3 RC channel slice (owner-local, not yet full peer service)

As of [#148](https://github.com/qigao/turboraft/pull/148),
`tr_raft_cnet_channel` has one borrowed caller-owned CNet progress owner,
one verified certificate-to-Node ID admission policy, a canonical Raft
HELLO/ACK exchange, and one `tr_raft_transport_session` admitted only after
reciprocal handshake completion. No additional Reactor, transport queue,
worker or automatic retry is created. `tr_raft_cnet_channel_observer()`
belongs to exactly one TLS connection; `attach()` records the resulting
generation-checked handle. The owner polls CNet, not the channel.

It uses CNet's ordinary finite receive credits: one outstanding receive
demand is replenished only after its callback returns successfully. A
payload received before exact mTLS + Node ID + Cluster ID + HELLO/ACK
authorization is never delivered. Ordinary Raft, group-aware DATA_CHUNK and
SNAPSHOT_CHUNK traverse the existing wire codec and the caller's explicit
owner-local payload callback; borrowed chunk bytes expire on callback exit.
Outbound `send()` uses bounded direct CNet write admission. It reports
acceptance, **not** remote durability or delivery, and never automatically
retries an uncertain transaction.

Qualified in the isolated Linux installed
[ACE 2.3 RC CTest lane](https://github.com/qigao/turboraft/actions/runs/37969773334)
against exact Salts.Native 2.3.0-rc.1 and SaltsUtils.Native 4.3.0-rc.1:
7/7 registered CTests, including real mutual TLS, reciprocal handshake,
forged ID and foreign-cluster rejection, unauthorized certificate rejection,
actual two-way Raft frames, and multiplexed group-aware snapshot/data frames.

**Still required to replace FlowMQ:** one full multi-peer Node/Owner service,
CNetManager/dial/reconnect lease semantics, deterministic per-peer fair
scheduling, large snapshot SG terminal tests, bounded queue-full recovery,
actual snapshot persistence/catch-up, stop/cancel/seal races, ASan/TSan and
Windows/macOS final package validation. The dedicated RC qualification is
not the full native-SDK release gate. No version fallback or FlowMQ deletion
is authorized by this test result.

### ManagedDial with stable Raft Service transport (owner-local)

`tr_raft_cnet_managed_peer` composes exactly one upstream
`cnet_managed_dial` per configured peer and borrows a single, shared
`cnet_manager` owned by the CNet final Owner. Manager allocates bounded
records, validates incarnation/generation identities and recycles only after
terminal callbacks; ManagedDial owns finite reconnect attempts, absolute
deadline and jittered backoff. The caller advances these functions alongside
the existing CNet progress loop: **no TurboRaft retry thread or queue**.

```c
/* Per Owner, initialized exactly once (configuration simplified). */
cnet_manager manager = {0};
/* cnet_manager_init(&manager, &strict_config); */

/* Per peer: exact Node ID, TLS policy, Raft HELLO, bounded retry policy.
 * All borrowed config/credential storage survives until manager recycle. */
tr_raft_cnet_managed_peer_t *peer = NULL;
/* tr_raft_cnet_managed_peer_create(&peer_config, &peer); */

/* Long-lived Service transport self: NOT a pointer to a recycled CNet
 * connection or a per-generation tr_raft_cnet_channel. */
tr_raft_cnet_managed_group_binding_t group = {peer, 42};
tr_raft_transport_t transport = {0};
tr_raft_cnet_managed_group_transport_bind(&group, &transport);

/* Host owns normal progress, e.g.:
 * cnet_client_poll(owner_client, 1, &events);
 * cnet_manager_advance(&manager, capacity, &work);
 * tr_raft_cnet_managed_peer_advance(peer, cmeta_monotonic_ms(), &wait);
 */
```

`CONNECTED` is not protocol admission. Only verified client/server TLS
peer certificate → *configured exact* Node ID → same Cluster/HELLO plus
reciprocal ACK transitions the Raft Channel to ACTIVE. Only then does the
owner explicitly mark `cnet_managed_dial_protocol_ready`. Security rejection
seals the recovery state rather than downgrading to plaintext or choosing
a different peer. Remote transport loss may schedule a *connection*
attempt subject to the finite policy, but never replays accepted Raft
messages or retries uncertain WAL/apply settlements.

Critically, the `tr_raft_cnet_managed_group_binding` retains a stable
Raft `Transport.enqueue` self across connection N → N+1. The Component,
Service and WAL owner do not retain stale `cnet_connection` handles.
Unready/delayed/CNet-capacity-full send returns `SALTS_ENOSPC` to the
existing Service peer-suffix owner. An explicit stop seals the managed
dial; Manager recycle and terminal CNet callbacks must finish before
peer destruction. Manager/client destruction follows last; all belong to
one Owner.

The initial live TLS test deliberately exercises **two simultaneous physical
connections to one authorized certificate/Node ID**, to verify Manager
credits and generation recycling independently of cluster membership.
It is **not** a claim that the full Node directory / distinct multiple
peer identity admission, multi-owner placement or membership changes
are implemented. Actual multi-node manager directory + peer-route
uniqueness and CNet client-pool protocol lease qualification remain open.
The full native SDK remains separate and currently RED.

## 5. Multicore and CFlow boundary

`tr_raft_multicore` already owns fixed cmeta threads, completion storage,
and a single owner per Raft group. This is the canonical Half-Sync/Active
Object execution owner. It must not be wrapped in a second queue + thread
pool merely to use an ACE name.

Investigate a CFlow typed Channel/Actor **facade** only where it replaces
cross-owner ad hoc handoff with fewer copies or stronger static typing.
Measure admitted request bytes, successful/failed admission, queue depth,
wake cost and p50/p99 under 1, 10 and 100+ ShardGroups before accepting a
second message mechanism. Keep one admission / one completion, explicit
cancel and bounded wait on stop. No peer callback mutates Service from
the wrong owner thread.

## 6. New-only packaging and gates

- Single exact `Salts.Native 2.3.0-ace.sha<FULL_SHA>` candidate only after
  SHA-attested upstream publication. Installed `find_package(Salts 2.3.0
  EXACT CONFIG)` rejects previous minor. All downstream C11/C++17 hosts,
  Plugin DSOs, SaltsUtils/CHttp/FlowMQ (until removed) must use a coherent
  installed ABI/SONAME; **NuGet version equality alone is not ABI evidence**.
- Do not fall back to 2.2, accept package wildcards or checkout unqualified
  Salts HEAD instead of the pinned candidate.
- Retire dedicated v0.2.0 producer/consumer, mixed-version and downgrade
  qualification. Keep current-source wire/Snapshot/WAL corruption, power-loss,
  quorum, ReadIndex, crash, chaos and installed package consumer tests.
- Linux GCC + Clang, Windows MSVC and available macOS; ASan+UBSan and
  separate TSan. Investigate active CNet/CHttp race [#141](https://github.com/qigao/turboraft/issues/141),
  never suppress it to obtain green status. The Windows binary cache blocker
  is [#140](https://github.com/qigao/turboraft/issues/140).
- Existing ccache/vcpkg binary caches affect build objects/packages only,
  not persisted Raft state, test outputs or benchmark evidence.
- Current multi-node SQLite/HashSlot/ShardGroup design
  [#131–#137](https://github.com/qigao/turboraft/issues/131) remains a
  higher-level consumer of this strict owner/service model.

## 7. Staged implementation order

1. [#143](https://github.com/qigao/turboraft/issues/143): new-only
   static build/package admission and remove v0.2.0-only files.
2. [#144](https://github.com/qigao/turboraft/issues/144): static CMeta
   Component graph over existing Runtime/Service callback boundaries.
3. [#145](https://github.com/qigao/turboraft/issues/145): CNet-native
   authenticated peer service, and remove FlowMQ after equivalence.
4. [#146](https://github.com/qigao/turboraft/issues/146):
   single-owner ACE mailbox/CFlow facade and queue benchmarks.
5. [#147](https://github.com/qigao/turboraft/issues/147):
   optional provider-generation fences, scoped teardown, TSan qualification.
6. Final exact Salts 2.3 SDK + installed consumer + OS/sanitizer/release
   gates. The Salts #1013 development PR remains **DO NOT MERGE**.

The architecture is an executable acceptance plan, not proof that the
current branch has completed the future CNet/Component work.
