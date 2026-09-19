# TurboRaft Multi-Group Foundation Design

**Status:** Proposed architecture for review  
**Date:** 2026-09-19  
**Scope:** TurboRaft only

## 1. Purpose

TurboRaft remains a reusable replicated-state-machine and consensus library. This design adds only the transport and snapshot substrate required for one physical process to host many independent Raft groups efficiently.

The design does **not** add sharding, placement, SQL, KV semantics, distributed query, storage-engine selection, or database policy to TurboRaft.

The boundary is:

> TurboRaft owns Raft protocol state, Raft durability, Raft replication, and transport of opaque application bytes. It does not own application-data semantics.

## 2. Current constraint

Today a transport session is identified by cluster ID, local node ID, peer node ID, and message ID. The wire metadata has no Raft group identity. Consequently, one physical peer connection cannot demultiplex traffic to multiple independent tr_raft_service_t instances without an external protocol layer.

For a Multi-Raft host this would otherwise force one connection per group or duplicate TurboRaft framing and reliability above the library.

## 3. Architectural rule

One tr_raft_service_t continues to represent exactly one Raft group.

TurboRaft does **not** introduce a tr_multi_raft_t owner. A higher-level host owns the group registry and the set of services.

~~~text
Process
  |
  +-- Group 100 -> tr_raft_service_t
  +-- Group 101 -> tr_raft_service_t
  +-- Group 102 -> tr_raft_service_t
  |
  +-- shared physical peer transport
~~~

The consensus core remains unaware of its hosting group ID.

## 4. Group identity

Add a transport-level group identity:

~~~c
typedef uint64_t tr_raft_group_id_t;
~~~

Rules:

- 0 is invalid for group-aware sessions.
- Group IDs are assigned and interpreted by the embedding system.
- Group ID is not stored in tr_raft_message_t.
- Group ID is not part of election, quorum, term, log, or membership semantics.
- Group ID exists only at routing, transport, snapshot, and diagnostics boundaries.

This preserves a clean separation between Raft algorithm state and Multi-Raft hosting.

## 5. Group-aware wire envelope

The group-aware envelope becomes conceptually:

~~~text
cluster_id
group_id
message_id
payload_kind
payload
~~~

Every payload kind uses the same routing identity:

- RAFT
- SNAPSHOT_CHUNK
- SNAPSHOT_ACK
- DATA_CHUNK
- DATA_ACK

A frame is first validated for cluster/session identity and then routed by group ID.

The group ID MUST NOT be encoded independently inside each payload type. It belongs to the common envelope.

## 6. Negotiation and compatibility

Add an explicit peer capability such as:

~~~c
TR_RAFT_HANDSHAKE_FEATURE_GROUP_MULTIPLEX_V1
~~~

Compatibility rules:

1. A peer pair without this capability keeps the current single-group contract.
2. A group-aware session requires both peers to negotiate the feature.
3. Legacy frames are never silently interpreted as group-aware frames.
4. A group-aware session rejects group ID 0.
5. Frame limits and snapshot limits remain negotiated exactly as they are today.
6. Rolling upgrade compatibility is explicit; no inferred downgrade path is allowed.

For legacy single-group operation, the caller may configure one default local group association outside the wire protocol. This is an adapter concern and must not create a second wire interpretation.

## 7. Physical peer connection model

For one physical node pair:

~~~text
Node A ======================== Node B
             mTLS / FlowMQ
                  |
          +-------+-------+
          |       |       |
        G100    G101    G102
~~~

The FlowMQ peer service continues to own one physical ROUTER/DEALER topology per node pair, but outbound work becomes group-tagged.

The transport remains caller-driven and single-owner. No hidden transport worker or per-group thread is introduced.

## 8. Outbound API

The group-aware enqueue boundary must make the group explicit.

Conceptually:

~~~c
int tr_raft_flowmq_peer_service_enqueue_group(
    tr_raft_flowmq_peer_service_t *service,
    tr_raft_group_id_t group_id,
    const tr_raft_transport_payload_t *payload);
~~~

The existing single-group APIs remain source-compatible where practical and are implemented as compatibility adapters rather than duplicated transport stacks.

## 9. Inbound dispatch

Decoded frames are delivered with group identity:

~~~text
validated frame
    |
    +-- group_id
    +-- typed payload
    |
    v
embedding group router
    |
    +-- group 100 service
    +-- group 101 service
    +-- group 102 service
~~~

The embedding runtime owns the mapping from group_id to a local service.

TurboRaft transport does not own a distributed group registry.

## 10. Unknown and stopped groups

Unknown or stopped groups are **group-routing conditions**, not peer-session corruption.

Therefore:

- malformed frame -> session/protocol failure,
- wrong cluster identity -> session/protocol failure,
- failed TLS identity -> session failure,
- unsupported negotiated contract -> session failure,
- unknown group ID -> local group rejection, connection remains usable,
- stopped group -> local group rejection, connection remains usable,
- local per-group saturation -> group-level backpressure, connection remains usable.

The higher-level host records diagnostics and decides whether to refresh group metadata or stop sending to the group.

An unknown group must never automatically tear down unrelated groups sharing the same connection.

## 11. Group isolation and fairness

A shared connection introduces a new safety requirement: one busy group must not starve another group's heartbeats, votes, or replication.

The transport therefore uses two capacity levels:

~~~text
global peer capacity
    +
per-group retained capacity
~~~

Required properties:

- explicit global message and byte HWM,
- explicit per-group message and byte limits,
- bounded number of active groups per peer service,
- bounded send work per step(),
- fair iteration across active groups,
- no unbounded spill queue,
- no silent drop.

The first implementation should use deterministic bounded round-robin scheduling. Weighted or priority scheduling is future work and must not be inferred implicitly.

Protocol traffic that has already been admitted retains FIFO order within its group.

## 12. Backpressure semantics

Current SALTS_ENOSPC semantics remain meaningful.

For group-aware transport, saturation is attributable to per-group capacity or global peer capacity. Both must be observable.

No group is allowed to allocate unbounded memory because another group is stalled.

A caller must be able to retry a rejected enqueue without ambiguity about whether the payload was accepted.

## 13. Raft Core and Service boundary

The following remain unchanged in ownership:

~~~text
tr_raft_core_t
  -> election
  -> replication state
  -> quorum
  -> ReadIndex
  -> membership

tr_raft_service_t
  -> persistence-before-send
  -> persistence-before-apply
  -> state-machine apply ordering
  -> local snapshot policy
~~~

Neither receives sharding, database-provider, placement, or query APIs.

One service remains one independently recoverable Raft durable state.

## 14. WAL boundary

TurboRaft continues to own Raft storage semantics.

Each group uses an independent durable WAL instance and prefix.

Example higher-level layout:

~~~text
node/
  raft/
    100/
      raft.00000001.wal
    101/
      raft.00000001.wal
    102/
      raft.00000001.wal
~~~

TurboRaft does not create a cross-group WAL format.

Benefits:

- independent recovery,
- independent corruption/fault domain,
- independent compaction,
- no cross-group transaction coupling,
- simpler backup and replacement.

## 15. Raft data versus application data

TurboRaft owns:

- current term,
- voted-for,
- Raft log,
- commit index,
- committed membership,
- snapshot Raft metadata,
- replication progress,
- opaque command bytes while stored or replicated.

TurboRaft does not interpret:

- SQL,
- keys and values,
- tables,
- rows,
- indexes,
- storage-engine pages,
- database schemas.

Opaque application bytes may be stored, copied, checksummed, and transferred by TurboRaft without becoming application semantics.

## 16. Snapshot boundary

The current snapshot protocol correctly separates Raft metadata from opaque FSM bytes, but database-backed groups require snapshots substantially larger than the current in-memory-oriented policy.

Add a streaming snapshot contract.

Conceptually:

~~~c
typedef struct tr_raft_snapshot_source {
    void *context;
    int (*describe)(void *context, tr_raft_snapshot_manifest_t *out);
    int (*read_at)(void *context,
                   uint64_t offset,
                   uint8_t *buffer,
                   size_t capacity,
                   size_t *out_size);
} tr_raft_snapshot_source_t;

typedef struct tr_raft_snapshot_sink {
    void *context;
    int (*begin)(void *context,
                 const tr_raft_snapshot_manifest_t *manifest);
    int (*write_at)(void *context,
                    uint64_t offset,
                    const uint8_t *data,
                    size_t size);
    int (*commit)(void *context);
    int (*abort)(void *context);
} tr_raft_snapshot_sink_t;
~~~

Exact API names may change during implementation planning, but the protocol is fixed:

- snapshot identity is immutable,
- total size is known,
- digest or manifest is known before installation becomes authoritative,
- reads and writes are offset-based,
- chunks are bounded,
- retry uses the same snapshot identity,
- receiver publishes only after complete verification,
- abort never publishes partial state,
- no API requires the entire database snapshot in memory.

## 17. Group-aware snapshot transfer

Every snapshot transfer is identified by:

~~~text
cluster_id
group_id
leader term
snapshot index
snapshot term
snapshot digest
snapshot size
~~~

A snapshot ACK can only advance the matching group's sender.

A snapshot for group 100 must never advance replication progress for group 101 even when both share the same physical peer connection.

## 18. Opaque large-data stream

The optional Data Stream facility remains an opaque consensus-coupled data plane.

It may know:

- group ID,
- stream ID,
- offset,
- size,
- digest,
- durable acknowledgement.

It must not know object schema, database row semantics, table identity, or query semantics.

Data Stream is optional and does not become the distributed database layer.

## 19. Diagnostics

Expose group-aware transport diagnostics sufficient for a host to observe:

- active group count,
- queued messages and bytes globally,
- queued messages and bytes per group,
- blocked groups,
- rejected unknown groups,
- sent and received frames per group,
- snapshot bytes in flight per group.

Diagnostics must be bounded snapshots, not unbounded history.

## 20. Security

The physical peer identity remains node-level mTLS identity.

Group identity is protocol routing metadata, not an authentication substitute.

A peer authenticated as node 2 may send only frames whose source node is node 2, regardless of group ID.

Higher-level authorization of whether node 2 is expected to host a particular group remains a host/catalog concern, while Raft membership still enforces voting and replication correctness inside each group.

## 21. Non-goals

This design deliberately excludes:

- shard routing,
- shard split or merge,
- replica placement,
- database-provider selection,
- SQLite or TidesDB integration,
- SQL,
- distributed KV API,
- Map/Reduce,
- distributed transactions,
- cross-group atomic snapshots,
- node discovery,
- group catalog ownership.

Those belong above TurboRaft.

## 22. Failure model

TurboRaft distinguishes three failure domains.

### Group failure

Examples:

- one group's state machine fails,
- one group's WAL fails,
- one group is stopped.

Other groups on the node may continue.

### Peer transport failure

Examples:

- TLS disconnect,
- FlowMQ socket failure,
- authenticated peer unreachable.

All groups using that peer link observe loss of transport to that peer.

### Protocol/session failure

Examples:

- malformed wire frame,
- cluster mismatch,
- invalid negotiated contract,
- non-monotonic message ID where prohibited.

The session fails closed.

This separation must be preserved in tests.

## 23. Verification gates

R0 is complete only when all of the following pass:

1. Three physical processes share one physical peer connection per node pair while hosting at least three Raft groups.
2. Different groups can elect different leaders concurrently.
3. Saturating one group's outbound capacity does not starve heartbeat or election traffic of the other groups.
4. One group's snapshot transfer does not block unrelated groups beyond configured bounded scheduler work.
5. Unknown or stopped group traffic does not tear down the shared peer session.
6. Malformed or identity-invalid frames still fail the session.
7. Every group recovers from its own WAL without opening another group's WAL.
8. Group removal does not disturb unrelated groups on the same physical transport.
9. Streaming snapshot transfer works without retaining the full snapshot in memory.
10. Existing single-group Core, Service, WAL, snapshot, and transport behavior remains unchanged.
11. Legacy peer interoperability follows the explicit negotiated compatibility contract.
12. Full release tests and multiprocess chaos tests remain green.

## 24. Resulting TurboRaft boundary

After R0:

~~~text
TurboRaft
  |
  +-- deterministic Raft Core
  +-- Service/runtime ordering
  +-- Raft WAL and recovery
  +-- membership / ReadIndex / transfer
  +-- snapshot protocol
  +-- group-aware wire transport
  +-- multiplexed physical peer links
  +-- bounded per-group isolation
  +-- streaming opaque snapshot transfer
  |
  '-- no database semantics
~~~

TurboRaft becomes a Multi-Raft-capable consensus substrate without becoming a distributed database.
