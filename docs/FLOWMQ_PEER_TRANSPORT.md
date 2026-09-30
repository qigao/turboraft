# FlowMQ peer transport

`TurboRaft::FlowMQ` is a caller-driven ROUTER/DEALER adapter over the current
FlowMQ socket API.

Each service owns one context, one bound ROUTER, and one DEALER per configured
peer. Every public call belongs to one owner thread. `step()` performs a
zero-timeout poll, receives at most `max_receive_batch_items`, then admits at
most `max_send_batch_items` per peer.

Creation requires explicit queue, batch, byte-HWM, message-HWM, reconnect, and
per-peer negotiated-contract values. A zero or unsupported value fails before
the service is returned; no transport profile is inferred.

There are no connector threads, callback-to-owner rings, producer mutexes, or
hidden progress workers. This matches FlowMQ's native ownership model and
avoids an extra copy/handoff on the hot path.

Outbound payloads are held in a fixed-capacity CSTL deque. Snapshot/data bytes
that outlive enqueue are copied into managed Salts buffers. A queue element is
released only after `flowmq_send(..., FLOWMQ_DONTWAIT)` succeeds; would-block
and transient disconnects preserve FIFO ownership.

## Raft Service backpressure boundary

`tr_raft_service_t` treats transport capacity as a **peer-local** condition.
The first `SALTS_ENOSPC` for one target pauses new Core output for that peer
and retains only the exact unsent suffix from the current Ready. A saturated or
disconnected minority peer therefore does not become a service-wide admission
barrier: healthy peers continue election, replication, proposal commit,
ReadIndex, membership, and snapshot progress under the ordinary Raft quorum
rules.

The ownership contract is:

- accepted transport work is never staged or sent again;
- each paused peer owns a bounded FIFO of at most one Ready's message suffix;
- the message FIFO is allocated lazily only after that peer first rejects
  capacity;
- one snapshot-request slot is retained independently after the message suffix;
- Core suppresses new outbound work for a paused peer and freezes that peer's
  Append window timeout/probe state until the staged suffix drains;
- draining one peer never waits for another peer to recover;
- when a peer's staged messages and snapshot request are empty, Service resumes
  Core output for that peer and normal catch-up traffic is generated;
- permanent transport/protocol errors remain fail-fast Service faults.

No paused peer is removed from membership or quorum calculation. Joint
Consensus, learner status, check-quorum, and leadership rules remain
authoritative. Leadership transfer to a paused target is rejected until the
peer resumes.

`tr_raft_service_get_peer_delivery_status()` exposes the bounded operational
state for one peer: pause state, staged message count/bytes, snapshot staging,
capacity rejection count, logical paused ticks, and last target error. Backup
handoff remains a stronger quiescence boundary and refuses to start while any
peer still owns staged transport work.


ROUTER identities are matched exactly against configured peer identities.
Decoded frames additionally validate cluster ID, source node, destination node,
wire version, and strictly increasing message ID. A TLS listener requires mutual
TLS and one to four canonical certificate SHA-256 fingerprints for every peer.
Before a HELLO identity enters the ROUTER peer table, FlowMQ binds it to the
verified client certificate. See [TLS identity binding](FLOWMQ_TLS_IDENTITY_BINDING.md)
for configuration, ownership, migration, and rotation rules.

The `turboraft.flowmq_peer_service` integration test drives two real services
over loopback mTLS. Node 1 uses `node1-cert.pem` as its client credential, node
2 uses `node2-cert.pem` as its server credential, and both trust the test-only
`ca.pem`. The positive path verifies normal Raft, multi-group, and snapshot
traffic over the authenticated peer link. Negative paths cover a missing client
certificate, a CA-valid certificate bound to the wrong peer, and a valid
certificate claiming a forged HELLO identity. Unauthorized peers must not reach
the Raft callback, while `tls_identity_rejections` remains observable without
poisoning the listener. The fixture keys are test data and must not be used
outside loopback tests.

The caller stops producers first. Raft Service must first drain or explicitly
discard/reload any peer-local staged work according to the caller's shutdown
policy; backup preparation refuses to cross that boundary while staged work is
owned. The caller then calls `tr_raft_flowmq_peer_service_stop()` and destroys
the service. Sockets close DEALER-first, followed by ROUTER and context
termination.
