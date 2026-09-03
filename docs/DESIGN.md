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

## Wire boundary

`raft_transport.h` owns length-prefix framing, the validated current wire
contract, monotonic message IDs, cluster/source/destination validation, and
borrowed decode callbacks. It requires a completed peer handshake and does not
own a socket or event loop.

`TurboRaft::CNet` adds `tr_raft_cnet_peer_t`, a bounded queue around a borrowed
`cnet_client`. The caller installs its observer while connecting or accepting,
polls CNet, and calls `tr_raft_cnet_peer_step()` on the same owner thread.

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

Every capacity is explicit. Queue saturation is returned to the caller and is
never converted into an unbounded allocation or silent drop.
