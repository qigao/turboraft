# TurboRaft native core boundary

Status: election and heartbeat core plus bounded native log foundation.

## Decision

TurboRaft is a new C11 implementation built on TurboUtils. The pinned
`willemt/raft` source is not a production dependency. It is retained as:

- a behavioral reference for Raft edge cases;
- an independent oracle for later differential traces;
- a source of regression scenarios that are rewritten against TurboRaft APIs.

The production target is `TurboRaft::Core`; it links `TurboUtils::Core`, uses
`TurboUtils::STL` privately for its internal containers, and does not link
`TurboRaft::WillemtCore`.

## First-slice scope

The native core currently owns these state transitions:

- follower, pre-candidate, candidate, and leader roles;
- pre-vote and RequestVote processing;
- log freshness checks for voting;
- heartbeat-driven leader recognition and candidate demotion;
- logical election and heartbeat timers;
- duplicate vote suppression;
- term and vote hard-state effects.

Single-entry log replication, current-term majority commit advancement, leader
check-quorum, bounded leadership transfer, and quorum-confirmed ReadIndex are
now part of the native Core. Replication sends bounded batches of at most eight
entries and permits one append in flight per peer, with deterministic heartbeat
retry and conflict-term-first-index backoff. Static learners share replication
progress but are excluded from elections, voting quorums, commits, ReadIndex,
check-quorum, and leadership transfer. Dynamic membership changes and
application dispatch remain outside this slice. Leadership
transfer catches up one voting peer before sending `TimeoutNow`, blocks
proposals while active, and cancels a stalled transfer after one election
timeout. ReadIndex requires a current-term committed entry, captures the commit
index at request time, and returns only after a matching context reaches a
voting majority. Service delivery additionally enforces `applied >= read`.

Startup accepts a compacted `(base_index, base_term)` plus a contiguous durable
log suffix, durable commit index, and last applied index. Creation copies and
validates the suffix and enforces `base <= applied <= commit <= last`.

`tr_raft_core_poll` exposes committed but unapplied entries after startup.
Committed entries also accompany the Ready that advances commit during normal
operation. `tr_raft_core_advance` moves `applied_index` only after the owner has
persisted the Ready, transmitted its messages, and atomically applied the
committed entry batch. Failure in any phase leaves Ready outstanding and the
node must be faulted rather than processing another input.

## Native log foundation

`src/core/raft_log.c` is an internal, bounded log fact source implemented with
TurboSTL `vec_t` and its natural `vec_*` API. Creation reserves the configured
maximum entry count, so reconciliation never grows memory beyond that bound. It
supports contiguous local append, previous-index/term matching, conflict-term
hints, idempotent replay, and atomic conflicting-suffix replacement.

Entries currently carry a bounded 512-byte command payload. Larger mesh data is
represented by an application-level content reference rather than copied into
the consensus log. The log treats different payloads at the same index and term
as a protocol safety violation.

## Ownership and effects

One event-loop owner calls a core instance. The core performs no I/O, obtains no
wall-clock time, generates no randomness, starts no coroutine, and invokes no
user callback. Inputs are `tick` or peer `message` values. Outputs are returned
through a caller-owned bounded `Ready` message array.

If the output array is too small, the call returns `TURBO_ENOSPC` before state
mutation. A non-empty `Ready` blocks the next `tick` or `step` until `advance`.
When hard state changed, the service layer must durably persist `(term,
voted_for)` before transmitting any messages from that `Ready`. Persistence
failure must fault the owning node; it must not call `advance` or send messages.

The core is single-owner, not thread-safe. CoroNet serialization belongs to the
future service layer rather than this state machine.

## Determinism and bounds

The caller supplies election timeout values in the configured range. This keeps
random generation and replay control outside consensus logic. Voter IDs are
non-zero, unique, sorted, static for this slice, and capped at
`TR_RAFT_MAX_VOTERS`. The fixed bound makes quorum tracking allocation-free
after creation.

## Next implementation boundary

Replication preserves this ordering:

1. Validate all input and output bounds.
2. Compute deterministic state transition.
3. Return hard-state and log persistence effects.
4. Persist effects in the service layer.
5. Send messages.
6. Advance the core.

Differential tests may compare normalized traces with `willemt/raft`, but
upstream structs, callbacks, errors, and ownership rules must not cross into the
native API.

## Runtime adapter boundary

`tr_raft_runtime_t` is a non-owning orchestrator around one Core. Storage,
transport, and state-machine capabilities use narrow C callback tables with an
explicit context pointer. The deterministic Core never invokes these callbacks.

Storage callbacks form one atomic transaction. The transport callback accepts
messages into a reliable local queue; connection and remote-delivery failures
remain transport concerns. The state machine atomically applies a committed
batch and persists its applied index with application state.

Runtime processing follows this order:

1. Persist hard state, log mutations, and commit index in one transaction.
2. Commit that transaction.
3. Enqueue outbound messages.
4. Atomically apply committed entries.
5. Advance the Core.

Any synchronous adapter failure faults the Runtime, leaves Ready unadvanced,
and requires constructing a new Core and Runtime from durable state.

## Wire codec boundary

The wire codec wraps a generated TBE payload in a fixed 40-byte, big-endian
envelope carrying magic, protocol version, header size, payload size, reserved
flags, 128-bit cluster ID, and message ID. Payload schema is little-endian and
versioned independently in `schema/turboraft_wire.schema`.

Only generated codec code links DataBind. Public TurboRaft APIs expose no
DataBind objects. Decoder limits are fixed before allocation, unknown versions
and non-zero reserved fields fail fast, and a frame must contain exactly one
payload with no trailing bytes.

## CoroNet stream transport boundary

`TurboRaft::CoroNet` is the repository's fixed transport adapter. It remains
outside `TurboRaft::Core`, so the deterministic state machine has no socket,
coroutine, retry, TLS, or connection lifecycle dependency.

One single-owner session represents one peer connection. It wraps a
caller-owned connected `coro_socket_t` and enforces these boundaries:

- a four-byte big-endian length precedes each wire frame;
- receive chunks may fragment one frame or coalesce multiple frames;
- cluster id, source peer, destination node, and a strictly increasing nonzero
  message id are checked before dispatch;
- framing, identity, replay, callback, and socket failures fault the session;
- a faulted session never resynchronizes or retries implicitly;
- destroying a session never closes its caller-owned socket.

`tr_raft_coronet_enqueue()` matches the Runtime transport enqueue callback and
must run in a managed CoroNet coroutine because `coro_socket_send()` may
suspend. `tr_raft_coronet_receive_once()` receives one transport chunk and
synchronously dispatches all complete messages in that chunk.

Message ids are scoped to a connection session. The service layer owns close,
reconnect, backoff, and fresh-session construction. Durable replay protection
across reconnects belongs to the authenticated service protocol.

Cluster and node identifiers are routing invariants, not credentials. A
production service must supply authenticated TLS or an equivalently trusted
channel; this adapter does not authenticate a raw TCP peer.

### Outbound TLS connector

`tr_raft_coronet_peer_manager_connect_outbound` performs one fail-fast TLS
connection attempt and transfers the connected socket to the owned admission
pipeline. `connect_host` is the routable endpoint selected by the mesh address
mapping layer. `request_host` is the stable virtual domain used for TLS SNI and
certificate verification, so callers do not expose or depend on a node's real
address.

The configuration must enable peer verification and name the expected peer node
ID. Admission checks the certificate-derived identity and HELLO node ID against
that expected ID before creating the managed session. Retry, backoff, endpoint
refresh, and cancellation remain scheduler responsibilities outside this socket
ownership boundary.

### Outbound dial scheduler

`tr_raft_coronet_dial_scheduler_t` is a bounded, single-owner state machine. A
caller drives it with monotonic `now_ms` values and schedules the next call from
`next_attempt_ms`; the library does not spawn a hidden coroutine or thread. Each
attempt resolves a fresh endpoint, then invokes the configured connector.

The scheduler owns only retry state: attempt count, current exponential delay,
next attempt time, and last error. The peer manager remains the sole owner of a
connected session. Endpoint results are fixed-capacity values valid for one
step. Retry policy is explicit and injected; malformed endpoints, invalid
configuration, protocol failures, and unexpected peer identities always stop
the retry cycle. The owner must detach a failed session before calling reset.

### Inbound TLS admission service

`tr_raft_coronet_inbound_service_handle` plugs directly into CoroNet
`coro_socket_listen_on`. CoroNet continues to own the TLS listener and accept
loop; the service consumes each accepted socket through the existing owned
admission pipeline and reports the result at one boundary callback.

The listener must require a verified client certificate. The handshake maps its
fingerprint to a node ID and verifies the HELLO claim before manager admission.
Inbound configuration deliberately has no expected peer ID because identity is
derived from the verified certificate; deterministic direction validation still
runs in the manager. The application must close and drain the CoroNet listener
before destroying the service, and must destroy the service before the manager.

### Certificate identity registry

`tr_raft_coronet_identity_registry_t` is an immutable, bounded mapping from the
canonical CoroNet certificate SHA-256 identity to a Raft node ID. Creation
copies, validates, and sorts at most
`TR_RAFT_CORONET_MAX_CERT_IDENTITIES` entries. Handshake lookup performs a
bounded binary search without allocation. Unknown certificates return
`TURBO_EPERM`; malformed identities return `TURBO_EPROTO`.

Multiple fingerprints may map to one node so operators can overlap old and new
certificates during rotation. Duplicate fingerprints are rejected. Rotation
creates a replacement registry snapshot; the owner swaps it only after existing
handshake callbacks have drained, then destroys the old snapshot.

### Peer service facade

`tr_raft_coronet_peer_service_t` is the optional single-owner facade for the
manager, identity registry, inbound admission service, and bounded outbound
schedulers. It injects the current identity snapshot into every handshake and
provides Runtime enqueue, scheduler stepping, identity replacement, and one
quiescent destroy boundary. It does not own the CoroNet listener and does not
duplicate session state. See [PEER_SERVICE.md](PEER_SERVICE.md).

### Bounded peer manager

One Runtime transport callback must route messages to every configured voter.
`tr_raft_coronet_peer_manager_t` provides that boundary without adding an
internal queue. It copies at most `TR_RAFT_MAX_VOTERS - 1` strictly ascending
peer IDs and performs bounded binary lookup by `message.to`.

The manager and all attached sessions have one event-loop owner. A successful
attach transfers session ownership to the manager; detach transfers it back;
manager destruction releases sessions still attached. Session sockets remain
caller-owned throughout. Attach validates cluster, local node, configured peer,
duplicate slots, and session state before ownership changes.

`tr_raft_coronet_peer_manager_enqueue()` is the Runtime adapter. Unknown,
unattached, detached, or faulted destinations fail synchronously. It never
drops, retries, allocates, or selects another route. Because CoroNet send may
suspend, attach, detach, status mutation, and enqueue must not run concurrently
on different coroutines.

Connection admission uses one deterministic rule for every node pair: the
smaller stable node ID opens the outbound connection and the larger node ID
accepts it inbound. `tr_raft_coronet_peer_manager_admit()` rejects the opposite
direction, detached sessions, identity mismatches, and duplicate occupied
slots. It never replaces an existing session. The service must first detach a
known faulted session before admitting its replacement.

This direction rule resolves simultaneous dialing but does not authenticate a
peer. The service must create the session only after TLS and HELLO validation
bind the certificate, cluster ID, and claimed node ID to the same identity.

### Peer handshake

The pre-session handshake is a bounded fixed-record codec with no allocation.
HELLO advertises process incarnation, configuration epoch, wire version range,
feature bits, and accepted frame and snapshot limits. Negotiation checks the
TLS-authenticated node ID and cluster ID, selects the highest common version,
intersects features, and selects the lower resource limits.

Both HELLO_ACK records must exactly match the negotiated result before it is
marked complete. A connected CoroNet session requires that completed result;
detached encode/feed operation remains available for deterministic codec tests.

The stream exchange accepts arbitrary recv fragmentation and coalescing. It
sends local HELLO first, emits one ACK after validating remote HELLO, and stops
consumption immediately after the validated remote ACK. Bytes following that
ACK remain in their original CoroNet receive buffer and must be fed into the new
session or explicitly released exactly once.

The CoroNet adapter obtains only the verified peer certificate SHA-256 identity
and resolves it through a caller-supplied read-only mapping to stable node ID.
It does not parse certificate subjects or accept unverified TLS/plaintext. The
configured handshake timeout is applied to the socket; the service must replace
it with the normal peer read-idle timeout after admission.

### Owned socket admission

`tr_raft_coronet_peer_manager_admit_owned_socket()` is the fail-closed bridge
from a connected TLS socket to an active peer slot. Passing a non-null socket
transfers ownership immediately. The function resolves verified certificate
identity, completes HELLO/ACK, creates an owning session, admits the
deterministic direction, applies any coalesced Raft bytes, and installs the
normal peer idle timeout.

The manager becomes the sole session owner on success. Every failure closes
the socket. Coalesced Raft bytes are dispatched only after the session occupies
its peer slot, so callback-generated responses can route through the same
manager. If that first dispatch fails, the session is detached and destroyed.

Message callbacks may synchronously enqueue responses and query status, but
manager lifecycle mutation is rejected with `TURBO_EBUSY` while any managed
callback is active. Destroying the currently executing session is the one
exception: destruction is marked pending, then the session is unlinked and
released immediately after callback return; the interrupted feed reports
`TURBO_ECANCELED`. This guard prevents callback-driven detach/destroy from
invalidating stack-held session pointers.
