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
