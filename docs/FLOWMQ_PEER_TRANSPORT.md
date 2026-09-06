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
wire version, and strictly increasing message ID. TLS endpoints configure CA,
certificate, key, server name, and optional client-certificate enforcement
through FlowMQ socket options before bind/connect.

The `turboraft.flowmq_peer_service` integration test drives two real services
over loopback mTLS. Node 1 uses `node1-cert.pem` as its client credential, node
2 uses `node2-cert.pem` as its server credential, and both trust the test-only
`ca.pem`. The positive case verifies one exact Raft heartbeat crosses the wire.
The negative case connects a CNet TLS probe without a client certificate to the
same FlowMQ ROUTER and requires the explicit TLS read failure while verifying
that no Raft callback runs. The fixture keys are test data and must not be used
outside loopback tests.

The caller stops producers first, calls `tr_raft_flowmq_peer_service_stop()`,
then destroys the service. Sockets close DEALER-first, followed by ROUTER and
context termination.
