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

### Distinct Node ID directory and fixed CNet Owner route

`tr_raft_cnet_peer_directory` is a **pure, bounded, owner-local routing
composition**, not a mutable process registry or second service locator.
The caller declares one unique `NodeID → ManagedPeer` entry for each
locally handled remote peer and a fixed list of Group IDs that may enter
this network Owner. Those borrowed arrays remain immutable and outlive
every Raft Service borrowing a directory-backed Transport.

Initialization checks the complete `tr_raft_cnet_identity_policy`,
rejects missing/duplicate Node IDs and duplicate live ManagedPeer
pointers, verifies that each underlying ManagedDial was created for
that exact peer, and uses Salts
`cnet_owner_placement_choose(CNET_OWNER_PLACE_STRICT_KEY)` with
`key_hash = NodeID` to enforce one stable network Owner. A peer that
belongs to another Owner is **not** rebalanced when local CNet Manager
credits are full. Actual socket admission still happens exclusively
through the correct Owner's `cnet_manager_reserve`, `cnet_handoff`
and CNet I/O.

The canonical Raft `tr_raft_transport_t.enqueue` may borrow a
`tr_raft_cnet_directory_group_binding_t` instead of binding to a
single physical peer. For each message it checks local NodeID, the
message's exact destination NodeID, authorized Group, current CNet
Owner and the destination's existing ManagedPeer, then delegates
one admission without an additional message queue or automatic retry.
Snapshot callbacks remain separately owned. Outgoing SNAPSHOT/DATA
frames use the same explicit Group/Node preflight through
`tr_raft_cnet_peer_directory_send`.

Unknown Node/Group returns `SALTS_ENOENT`; misrepresented source,
self-target or malformed route returns `SALTS_EPROTO`/`SALTS_EINVAL`;
a known Node pinned to a foreign Owner returns `SALTS_EPERM`; an
unready peer propagates existing peer-local `SALTS_ENOSPC`. Bind
itself rejects an unapproved Group **before** a Runtime begins. The
directory creates no socket, credits, scheduler thread, storage writer
or dynamic Plugin generation.

The route-preflight fixture uses distinct Node 1 / Node 3 authorizations
and two real but undialed ManagedPeer records to prove duplicate rejection,
strict Owner selection, Group gating, cross-thread denial and zero Manager
credit mutation. Separately, the executed [distinct-node mTLS acceptance
lane](https://github.com/qigao/turboraft/actions/runs/37978488299) uses
**two different real client certificates**, one for Node 1 and one rotated
client certificate explicitly assigned to Node 3, connecting to Node 2.
Two fully negotiated TLS/HELLO/ACK channels exchange independent Raft
Group 101/103 messages; a valid client certificate claiming the wrong
Node ID is rejected while the healthy Node remains ready.

Ingress now has one optional typed callback gate:
`tr_raft_cnet_peer_directory_receive`. The caller installs it as the
authenticated CNet Channel's `on_payload`, with one address-stable
`tr_raft_cnet_directory_ingress_t` that borrows **that exact live
Channel** plus the immutable Directory. It asks CNet Channel status
for the verified Node ID and ACTIVE phase, then checks payload source,
local destination, authorized Group and strict CNet Owner against the
Directory **before** invoking the supplied bounded Owner callback.
No caller-provided fingerprint or raw HELLO may grant authorization.

```c
tr_raft_cnet_directory_ingress_t ingress = {
    .directory = &directory,
    .on_payload = bounded_owner_route,
    .context = owner_route_context
};
tr_raft_cnet_channel_config_t peer = authenticated_channel_config;
peer.on_payload = tr_raft_cnet_peer_directory_receive;
peer.payload_context = &ingress;
tr_raft_cnet_channel_t *channel = NULL;
/* tr_raft_cnet_channel_create(&peer, &channel); */
ingress.channel = channel; /* before CNet connect/accept callbacks */
```

The callback validates a real TLS Channel before forwarding, and never
starts a second acceptor, thread, scheduler, queue or Plugin generation.
The bounded sink must remain on this CNet Owner, or explicitly transfer
owned bytes through the *existing* Raft Owner mailbox before mutating a
Service. The two logical clients currently share one network test Owner:
this qualifies simultaneous distinct certificate identity, **not**
cross-Owner transfer, client-pool lease arbitration or full multi-peer
Cluster membership. Those remain open.

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

### Transport-neutral Raft Owner handoff (executed in existing Multicore)

The CNet callback path is **one** admission, not a second Actor scheduler:

```text
mTLS CNet/NativeIO progress Owner
   -> Raft reciprocal HELLO/ACK -> tr_raft_cnet_channel ACTIVE
   -> tr_raft_cnet_peer_directory_receive (verified Node/Group/Owner)
   -> tr_raft_multicore_ingress_receive (INLINE Raft message copy)
   -> EXISTING tr_raft_multicore_submit(Group, TR_RAFT_MULTICORE_STEP)
   -> one fixed Raft Group Owner -> Service::step -> Core/WAL/Apply
   -> EXISTING tr_raft_multicore_take(Group) completion
```

The `tr_raft_multicore_ingress` callback belongs to **TurboRaft::Multicore**,
not TurboRaft::CNet, so no network provider dependency leaks into the Raft
scheduler. It allocates one small address-stable correlation generator but
**no separate queue, timer, transport connection or thread**. A single
atomic request-ID namespace may be shared by multiple CNet ingress
producers, and the host must reserve disjoint ranges for unrelated submit
callers. Raft STEP messages use an **inline full-value copy** into
Multicore's existing bounded request ring before the borrowed CNet callback
returns. Snapshot and Data chunks instead use the explicitly separate
`tr_raft_multicore_ingress_submit_with_origin()` owned-payload path:
the original borrowed chunk is retained into one bounded lease; its bytes
are released after the assigned Group Owner callback returns and before
publishing the exact Group completion. Authenticated Node/Group, channel
incarnation, connection token and Component generation travel as copied
`reply_origin` values, not raw CNet runtime handles. Both item and
byte credits are preflighted before retaining the borrowed chunk.
Request and completion credits are reserved together; completion space
is held until the application calls `tr_raft_multicore_take()`.
Per-Group capacity exhaustion returns `SALTS_ENOSPC` and never steals
another Group's credits or silently retries an uncommitted message.
After stop, each already-accepted request produces exactly one
`SALTS_ECANCELED` completion rather than disappearing.

### Real certified multi-Peer Group overload: item vs byte credits

Two additional **exact installed-SDK** fixtures reuse the existing
separately certified Node1 and Node3 mTLS connections to Node2 and
the **same two fixed** Multicore Group101/Group103 Owners:

- `turboraft.flowmq13.cnet_group_pressure` reserves precisely
  **two of two** per-Group completion/item credits (one verified Raft STEP
  and one leased DATA/SNAPSHOT chunk). Another genuinely decoded Node3
  Snapshot, with valid Node/Cluster/Group, fails **only** Group103's
  `SALTS_ENOSPC` preflight. The corresponding Node3 TLS Channel faults
  and settles terminally, without a third Owner callback or implicit retry.
  Group101's authenticated Node1 Channel remains ACTIVE; after consuming
  its original completions it admits and completes a further Raft heartbeat.
- `turboraft.flowmq13.cnet_byte_pressure` instead configures
  **capacity 3** but **24 bytes** per Group. An explicitly test-controlled
  callback on the *real Group103 Owner thread* temporarily holds the first
  24-byte SNAPSHOT SG lease. There is still a spare item slot, but a
  second otherwise valid 24-byte Node3 TLS Snapshot fails `SALTS_ENOSPC`
  from the **byte budget**, not from Item or certificate validation.
  The separate Group101 Owner successfully finishes its own STEP and
  DATA callback while Group103 is deliberately paused; the test releases
  the owner before teardown and asserts all bytes and completions return
  to their original Group with no duplicate ACK.

Both use the existing CNet progression and Multicore rings: **no extra
scheduler, fallback route, hidden buffering, or automatic application
retransmission**. Failure remains explicitly **fail-closed**: overload
closes only the affected Peer Channel. This is per-Group credit isolation
under **two simultaneous real TLS clients**, not a claim of lossless
pause/resume, three independently operating Raft servers, distributed
quorum convergence or wire-level throughput equality with FlowMQ.

The installed-SDK `turboraft.flowmq13.cnet_item_rejoin` and
`turboraft.flowmq13.cnet_byte_rejoin` fixtures now extend **each**
overload type through an actual physical Node3 **N → N+1** TLS reconnect.
Before reusing its connection slot, the test waits for **both** terminal
CNet endpoints and asserts `payloads_admitted == payloads_completed +
payloads_canceled` with **zero** writes pending. It preserves the exact
old Group103 completion's authenticated `reply_origin` as copied data,
then constructs a new physical connection, validates certificate + reciprocal
HELLO and requires a distinct Channel instance and NativeIO slot token.

The old request had **no valid storage ACK**; a *test-only synthetic ACK*
is presented only as a **negative generation-fencing probe** and must
return `SALTS_ECANCELED` before consuming any new CNet send credit.
It is never delivered or used as a durable confirmation. There is no
implicit resend after the TLS reconnection: an explicit host-initiated
Snapshot from Node3 re-enters Group103 as a **new** owned request,
produces exactly one non-durable completion and returns its SG bytes;
a concurrent new Node1 Raft heartbeat still finishes on Group101.
Owner credit rejections remain isolated to Group103 and Node1's certified
Channel remains ACTIVE throughout.

The new reconnect tests each run as a fresh process **25 times on Linux
epoll, Windows IOCP and macOS Kqueue**. They prove fail-closed recovery
and no automatic settlement retry on the current TLS/Owner implementation.
They do **not** prove lossless backpressure, transport-side frame parking,
streaming network fairness under sustained large Snapshot load, or a
three-server Raft quorum.

The **CNet Channel** currently treats a negative payload callback result
(including full capacity) as a failed connection and closes that Channel.
This is fail-closed rather than lossless flow control: upstream peers must
recover replication from Raft protocol progress, not by an automatic local
replay of an uncertain message. A later flow-control slice may establish
bounded pause/resume with retained frame ownership and receive demand;
until proven, do not claim zero-copy/lossless backpressure under overload.

Snapshot/data chunks indeed carry **borrowed byte pointers** at the CNet
decode edge; they are **not** shallow-copied into Raft STEP requests. The
existing owned-chunk admission keeps exactly one bounded byte lease per
accepted request until the receiving Group Owner finishes. In the real
certified Node3 mTLS Snapshot fixture, Group103 `SnapshotReceiver` only
reports a positive **installed** final receipt after production
`WalStorage` has published the validated SHA-256 Snapshot and manifest.
A partial accepted offset is progress only. CNet refuses to send a
full-size `SNAPSHOT_ACK` without an installed owner receipt, and also
rejects wrong-generation/foreign-channel completions **before using
send credits**. A failed WAL Snapshot staging write must not be
translated into a successful ACK. The single CNet progress owner and
the existing Group Owner ring remain the only transport/compute owners.

The bridge is also exercised **through a real mTLS wire receive** in
`test_distinct_peer_tls.c`: two differently certified clients (Node 1
and Node 3) connect to Node 2. Each message passes the live CNet
certificate/Node ID + reciprocal HELLO/ACK, the immutable Group/Owner
directory and the transport-neutral Multicore copy, and is then
processed by **two distinct Raft Group Owner threads**. The test takes
the independent per-Group `TR_RAFT_MULTICORE_STEP` completions and
verifies each succeeded, including after the original network decode
buffer's callback lifetime ended. A valid client certificate with a
forged HELLO cannot create a completion for the other Group; the
healthy Peer remains unaffected. This was executed against exact
Salts.Native 2.3.0-rc.1 and SaltsUtils.Native 4.3.0-rc.1 installed
packages in [CI #37980929678](https://github.com/qigao/turboraft/actions/runs/37980929678),
which passed **11/11 registered CTests** (2 standalone Component
consumers, 9 CNet/Multicore consumers).

The ingress is borrowed by CNet and only destroyed after its final CNet
callback is quiescent, before the Multicore runtime is freed. Stopping
the adapter itself never stops or drains another owner or CNet instance.
The host remains responsible for collecting every accepted completion
and deciding whether the corresponding message generated a legal durable
or applied transition. No raw runtime state is serialized.

### Distinct qualification scopes: 64-MiB streaming vs quorum simulation

The installed full-profile SDK consumer
`turboraft.flowmq13.large_snapshot_stream` runs a **64-MiB** generated
Snapshot through `SnapshotSender.begin_source(read_at)`, the real Raft
wire chunk and cumulative-ACK codecs, and `SnapshotReceiver`'s streaming
sink. It sends exactly **1,024 64-KiB chunks**, hard-gates the
**four-inflight-chunk / 256-KiB sender slot bound**, rejects a fifth
unacknowledged claim, and checks per-write/source-read limits, incremental
SHA-256 and one terminal stream commit. The test allocates no logical
64-MiB payload buffer. On **Linux**, it also gates the entire process's
`getrusage(RUSAGE_SELF).ru_maxrss` at **48 MiB** (high-water RSS in KiB,
not a precise heap allocator trace). CI prints the actual counters,
not only test PASS. This validates the installed wire/stream contract,
**not** actual CNet bandwidth, full transport backpressure, bounded
streaming-WAL disk staging or cross-node catch-up.

The installed Core consumer `turboraft.flowmq13.three_node_core` runs
three independent Raft Core instances with **deterministically simulated
links** and persisted Ready values. A former Leader is isolated; the
other 2-of-3 quorum elects a new Leader and commits an ordered suffix.
The old Leader's Core is destroyed/recreated while disconnected, then
all three heal and must converge exactly on eight log entries and
committed/applied indices. It uses a **bounded in-process message queue**,
not real CNet connections or multi-host quorum. The real certified TLS,
WAL and process-SIGKILL tests remain distinct and must not be combined
into a false claim of automatic multi-node convergence.

### Certified CNet wire quorum: positive and fail-closed negative gates

Four installed-full-profile executables now exercise **three independent
`RaftCore` instances** against **two actual mutual-TLS CNet connections**
between Node2 and certified Node1/Node3. The test uses the installed Core and
CNet SDKs, real Raft binary envelopes, reciprocal HELLO/ACK and certificate
identities, instead of routing voted/replicated Ready messages directly
through a simulated delivery function:

- `turboraft.flowmq13.cnet_core_quorum`: the Node1 Core is deliberately
  prevented from *processing* its genuine TLS messages in its test callback,
  while Node2 receives Node3's real wire vote and authentic Append ACK.
  Node2+Node3 must elect a 2-of-3 Leader/majority and commit three commands.
  The Node1 Core is destroyed and recreated from its persisted **TEST
  in-memory Ready values**, resumes handling certified TLS frames and
  must converge exactly on three ordered log entries plus applied/commit
  indices. CNet sends and ACKs are real; there is no direct
  `RaftCore.step` forwarding for remote messages.
- `turboraft.flowmq13.cnet_core_quorum_loss`: after that successful
  2-of-3 replication, the test also temporarily prevents Node3's
  application callback from stepping otherwise-valid certified TLS
  AppendEntries while Node1 remains silent. Node2 may **append index4
  locally**, but **must not advance `commit_index` or `applied_index`
  from 3 without a second voter**. Restoring Node3 allows Core-protocol
  tick/reprobe and a new **actual TLS Append ACK** to commit index4.
  Node1 subsequently rejoins and must converge byte-for-byte on four
  entries, commit4 and applied4. There is **no host-level CNet message or
  storage-settlement retry**. Muting a test *Core callback* is NOT the
  same as a physical packet loss / OS link failure.
- `turboraft.flowmq13.cnet_core_quorum_ticked_loss` extends that negative
  gate by continuing **eight monotonic-time Raft ticks** with both remote
  callbacks silent. `CheckQuorum` must eventually DEMOTE minority Leader
  Node2 rather than let it retain authority. Persisted Ready, `commit_index`
  and `applied_index` are checked on every progress iteration and remain
  at 3 despite the uncommitted index4 log. Restoring Node3 must yield a
  **fresh real TLS vote** and a higher-term election. The old-term index4
  remains uncommitted after that election: only explicit proposal of a
  **new-term index5 barrier**, authenticated Append ACK and majority match
  may commit both entries. Node1 then catches up through index5. This is
  a real Raft minority-role transition but only simulates loss at callbacks:
  it does **not** exercise independent node election clocks or TCP faults.
- `turboraft.flowmq13.cnet_core_quorum_physical_loss` strengthens the same
  minority/CheckQuorum + current-term commit-barrier test with a **real CNet
  TLS socket shutdown**, not receive-callback muting, on Node3's certified
  N connection. Both CNet owners must report terminal with exact send
  settlement before destroying N callbacks. While Node3 is disconnected,
  emitted Raft Ready output to that offline peer is explicitly discarded
  by the TEST transport (never admitted to CNet). Node2 must demote and
  leave index4 uncommitted during eight elapsed-time ticks. The host then
  explicitly establishes a NEW authenticated mTLS/HELLO N+1 with a
  different Channel instance and CNet slot-generation token on the same
  listener and owners. Before N+1 authentication, any test-only Ready
  output left for the terminal old Node3 channel is explicitly rejected;
  Node1's independent output remains FIFO-ordered. No accepted or unsent
  old-generation Raft output is automatically replayed. Only after fresh
  higher-term votes and a current-term index5 majority ACK can index4
  and index5 commit, after which Node1 rejoins and converges. This is
  an actual socket teardown/re-establishment, not an involuntary network
  partition, and all three Cores still run inside one test process.

The CNet test harness has finite 15-second TLS read and 5-second write
timeouts, an 8-second bounded Raft progress phase, and drives heartbeats
using **monotonic elapsed time** (100-ms ticks), not arbitrary IOCP vs
Kqueue/epoll poll counts. A failure reports only bounded non-secret
counters (terms, commit indices, wire message and ACK counts, Channel
phase/errors). These four CTests each run in **25 fresh OS processes per
host** under the exact released FlowMQ 1.3.0 full SDK conformance profile,
in addition to the prior fixed Group Ownership and WAL crash suites.

**Evidence boundary:** all three Core objects still share **one process**
and a bounded test-only in-memory Ready store; there are only two
bidirectional TLS links centered on Node2. It is stronger than
`three_node_core`'s simulated transport, but it does **not** prove three
isolated server processes / separate on-disk WAL writers, a full mesh,
real physical network partition healing, automatic membership migration,
or replacement parity for FlowMQ. All those remain hard gates before
deleting FlowMQ.Native 1.3.0 or merging PR #148.

### #149 Linux three-process Core/WAL bootstrap (separate from quorum)

The installed full FlowMQ 1.3 profile now has a separate **Linux-only**
`turboraft.flowmq13.three_process_wal_bootstrap` acceptance gate. Its
controller launches **three concurrently live, independently exec'ed
processes**, each with a distinct PID, new `RaftCore`, private controller
socket, separate directory and an exclusive production `WalStorage` writer.
All child startup status messages contain only copied scalar snapshots.
An explicit test fixture seeds hard term/vote on each WAL, and **an
uncommitted index1 only on Node2** (this is **not** a Raft proposal,
replication, or majority commit). Opening Node2's WAL with a second writer
while the process remains live must be rejected by the production file lock.
Node2 then suffers a real **SIGKILL**, while Node1/Node3 remain alive;
a new `fork+exec` worker reopens exactly the previous Node2 WAL with
`create_if_missing=false`. It must recover the same exact uncommitted
entry, term/vote, and **commit=0/applied=0**, with no handle or runtime
state carried from the dead process. Independent Node1/Node3 stay unchanged.
The Linux installed-SDK CI executes this isolated CTest with 25
fresh-process repetitions and keeps its JUnit result separately labelled.

**Scope boundary:** this tests process/WAL writer ownership and a forced
process death, **not** a three-process Raft consensus, mTLS vote/Append
traffic, real network partition, crash during an admitted but incomplete
network send, or distributed durability/catch-up. Those are explicit
remaining parts of [#149](https://github.com/qigao/turboraft/issues/149)
under [#145](https://github.com/qigao/turboraft/issues/145); do not
mislabel this bootstrap as full release/parity evidence. No production
queue, fallbacks, public ABI or runtime-state persistence is added.

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
