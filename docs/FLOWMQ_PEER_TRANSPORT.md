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

## Raft Service backpressure ownership

Transport capacity failure is isolated by target peer. When enqueue for peer
`P` returns `SALTS_ENOSPC`, `TurboRaft::Service` copies only the exact
unsent Ready suffix for `P` into bounded per-peer storage and pauses future
Core output for that peer. The pause does **not** change membership, voter
weight, Joint Consensus rules, or quorum calculations.

Other peers continue to receive election, replication, ReadIndex, membership,
and heartbeat traffic. Service opportunistically retries every paused peer
before later owner-thread operations; one peer remaining full does not prevent
another peer from draining. Once the exact staged suffix for `P` is accepted,
Service resumes Core output for `P`; any tracked AppendEntries retry timer was
frozen while the peer was paused and resumes from the same logical in-flight
state.

FIFO is preserved per peer. An item successfully admitted by the underlying
transport is never copied into the staged suffix and is never resent by this
layer. No cross-peer delivery order is promised. While a peer still owns a
staged suffix, a new inbound Service step from that same peer returns
`SALTS_ENOSPC` without consuming the message; this bounds response growth
without blocking unrelated local proposals or healthy-peer input.

`tr_raft_service_transport_status()` exposes the bounded read-only state for
each current remote peer: paused state, staged message/snapshot counts,
capacity-rejection count, and last target error. Permanent non-capacity
transport failures remain fail-fast Service faults.

The caller stops producers first, calls `tr_raft_flowmq_peer_service_stop()`,
then destroys the service. Sockets close DEALER-first, followed by ROUTER and
context termination.
