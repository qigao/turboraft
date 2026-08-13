# TurboRaft RPC Control Plane

TurboRaft exposes a JSON-RPC 2.0 control plane and an HTMX management page on
the same HTTP endpoint. The RPC service is the authoritative management API;
the HTMX page only submits RPC requests and does not own cluster state.

## Endpoint and authentication

- `GET /raft` returns the HTMX management page.
- JSON-RPC requests use `POST /raft/rpc`.
- `GET /raft/status` returns the HTMX status fragment.
- `/raft/ws` is a read-only WebSocket status endpoint.
- `raft.status`, `raft.members`, `raft.progress`, and
  `raft.storage.status` are read-only diagnostics and do not require the
  management token.
- All other methods require the configured bearer token.
- Missing or invalid authorization fails before a command reaches the Raft
  service owner loop.

Example request:

```http
POST /raft/rpc HTTP/1.1
Content-Type: application/json
Authorization: Bearer <management-token>

{"jsonrpc":"2.0","id":1,"method":"raft.read_index","params":{}}
```

Every failure is returned as a JSON-RPC error. Callers must treat any `error`
member as failure and must not infer success from the HTTP status alone.

Production deployments must serve the control plane through
`iris_server_start_tls()` or `iris_server_start_tls_on()`. The TLS ALPN list
must contain `h2` and `http/1.1`; this lets the same Iris app and routes accept
HTTP/2 and HTTP/1.1 without protocol-specific handlers. Plaintext listeners are
development-only because mutation requests carry a bearer credential. Cleartext
HTTP/2 uses prior knowledge and does not weaken this production TLS requirement.

Leader-only mutation failures use the stable control-plane category
`NOT_LEADER` and include `leader_id`. The identifier is a virtual Raft node ID;
it is not an IP address or transport endpoint. Clients resolve it through the
mesh virtual-address mapping and must not bypass that mapping.

The endpoint accepts at most 16 KiB per JSON-RPC request and at most eight
items per batch. Exact-boundary requests are accepted; one-over requests fail
before method execution. Method response builders and owner commands use their
own bounded capacities and fail instead of truncating data.

## Read-only diagnostics

### `raft.status`

Parameters: none.

Returns the local node identity, Raft role and term, leader, commit/applied/log
indexes, membership transition state, owner-loop state, and the latest runtime
durability status.

### `raft.members`

Parameters: optional `role`, either `voter` or `learner`.

Returns a read-only snapshot of the committed membership configuration,
including its phase, transition identifier, voters, and learners. The response
is derived from the service/core state and is not maintained as a separate
control-plane copy. When `role` is supplied, only matching members are
returned.

### `raft.progress`

Parameters: optional positive `node_id`.

Returns bounded per-peer replication progress: peer identifier, `match_index`,
`next_index`, recent activity, append-in-flight state, elapsed in-flight ticks,
and whether snapshot installation is required. A supplied `node_id` returns
only that peer; an unknown peer returns an error.

### `raft.storage.status`

Parameters: none.

Returns the latest runtime durability barrier, including the durable term,
vote, log index, commit index, and applied index. This reports the state known
to the Raft runtime; it does not fabricate database-specific health fields.
The nested `audit` object reports whether a sink is configured, whether a
required sink failure has latched mutation rejection, and the optional sink's
dropped-event count.

### `raft.operation.status`

Parameters:

- `term`: positive term from an accepted mutation receipt.
- `index`: positive log index from the same receipt.

This authenticated query executes through the Service owner loop and derives
its result from the retained log, commit index, applied index, and compacted log
base. It returns `PENDING`, `COMMITTED`, `APPLIED`, `LOST`, or `EXPIRED`.
`EXPIRED` means compaction removed the older term identity, so the server
refuses to fabricate success. A future term or malformed receipt fails as
invalid parameters.

## Commands

### `raft.tick`

Advances the local Raft clock through the service owner loop. This method is
primarily intended for controlled deployments and tests; production schedulers
normally drive ticks directly.

### `raft.propose`

Submits an application command to the current leader. Success means the command
was accepted into the Raft log, not that an arbitrary external side effect has
completed.

### `raft.propose_async`

Accepts the same positive `command_id` and bounded `data` parameters as
`raft.propose`, but returns the current receipt state plus the appended entry's
term and index. New control-plane clients should use this method and continue
with `raft.operation.status` while the state is `PENDING` or `COMMITTED`.
Once accepted, closing the HTTP connection does not cancel the Raft command.
A client reconnects and queries the term/index receipt; the control plane owns
no independent pending-operation registry.

### `raft.read_index`

Starts a linearizable ReadIndex operation. The compatibility name
`raft.readIndex` remains available.

### `raft.takeReadState`

Consumes an available ReadIndex completion. Absence of a completed read is
reported as an error rather than as a fabricated index.

### `raft.leader.transfer`

Parameters:

- `target_id`: positive node identifier of the target voter.

Starts leadership transfer. The compatibility name
`raft.transferLeadership` remains available.

### `raft.member.add_learner`

Parameters:

- `node_id`: positive node identifier to add.
- `transition_id`: positive, caller-unique membership transition identifier.

Derives a new Joint Consensus configuration from the current committed
configuration and adds the node as a learner. Success returns the membership
entry receipt with its current operation state, term, and index.

### `raft.member.promote`

Parameters:

- `node_id`: existing learner to promote.
- `transition_id`: positive, caller-unique membership transition identifier.

Moves an existing learner into the voter set through Joint Consensus and
returns its receipt.

### `raft.member.remove`

Parameters:

- `node_id`: existing voter or learner to remove.
- `transition_id`: positive, caller-unique membership transition identifier.

Removes the node through Joint Consensus and returns its receipt. Invalid
targets and attempts made while another membership transition is active fail
fast.

### `raft.changeMembership`

Low-level compatibility method that accepts complete voter and learner sets.
New management clients should use the three intent-level `raft.member.*`
methods so the authoritative current configuration remains the only source used
to derive a transition.

### `raft.snapshot.trigger`

Parameters: none.

Creates and durably stores a snapshot at the current applied index, then
compacts the core log. It fails when snapshots are disabled or when no entries
have been applied since the current log base; it does not return a false
success. Automatic threshold-based snapshots continue to use the same snapshot
pipeline.

## Membership and concurrency rules

- Membership commands execute in the service owner loop.
- A command starts only from a stable final configuration.
- Voter and learner arrays are bounded by `TR_RAFT_MAX_MEMBERS` and sorted
  before submission for deterministic encoding.
- The core/service membership state is the sole source of truth. The RPC and
  HTMX layers do not cache writable membership state.
- A successful RPC submission does not bypass Raft quorum, persistence, or
  Joint Consensus rules.

## HTMX management page

The page provides status, membership, replication progress, storage durability,
ReadIndex, leadership transfer, learner add, learner promotion, member removal,
advanced full membership change, and explicit snapshot controls. Responses are
rendered from the RPC result, so page refreshes and multiple operators cannot
advance an independent UI-side state machine.

## WebSocket status snapshots

`/raft/ws` is registered once with `iris_app_ws()`. The same route accepts an
HTTP/1.1 RFC 6455 Upgrade or an HTTP/2 RFC 8441 extended CONNECT. WebSocket over
HTTP/3 is not supported and is never downgraded implicitly.

After the handshake, the server sends one text frame containing the same bounded
status JSON used by `raft.status`. A client may send the exact text or binary
payload `refresh` to request another snapshot. Any other application payload
closes the connection with policy code 1008. Rendering failure closes it with
code 1011.

The endpoint is deliberately read-only. Membership, leadership, proposal, and
snapshot mutations remain JSON-RPC operations so they retain authentication,
audit, receipt, and owner-loop semantics. The handler does not retain the Iris
WebSocket handle after its callback and does not maintain a second copy of Raft
state. Server-initiated broadcast requires a separate, bounded Iris-context
lifecycle protocol and is not part of this endpoint.
