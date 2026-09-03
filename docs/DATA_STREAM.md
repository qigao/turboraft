# FlowMQ-backed Raft data streams

TurboRaft keeps large bytes out of `AppendEntries`. FlowMQ carries ordered
64 KiB `DATA_CHUNK` frames; Raft commits a 56-byte descriptor only after the
same stream is durable on a voting quorum.

## Protocol

- Identity: `(leader term, stream_id, stream_size, SHA-256 digest)`.
- Ownership: sender chunks borrow immutable caller storage until the complete
  durable ACK; the FlowMQ queue copies accepted chunks into `mem_buffer_t`.
- Ordering: receivers accept the exact cumulative `next_offset`. Duplicate
  earlier chunks return the current offset; gaps and identity changes fail.
- Capacity: one stream is at most 1 GiB. A chunk is at most 64 KiB. The default
  FlowMQ batch is four frames (256 KiB), the receive buffer is 256 KiB, and the
  retained per-peer data budget is 4 MiB.
- Backpressure: a full outbound item queue or byte budget returns
  `SALTS_ENOSPC`; no data is dropped or moved to unbounded storage.
- Durability: the receiver calls `sink.commit` only after length and SHA-256
  validation. Only an accepted final ACK sets `durable=true`.
- Consensus: old and new voting majorities are checked, but proposal remains
  gated until every current replication target has a durable staged copy. This
  prevents a follower from receiving and applying a descriptor before its
  bytes. A future per-peer AppendEntries gate can safely relax the all-target
  requirement without changing descriptor semantics.
- Shutdown: reset or destroy calls `sink.abort` for an uncommitted receiver.

## Sender and receiver

Create one sender per peer and one receiver per inbound stream owner. Sink
callbacks stage directly to a bounded file, database blob, or object store;
`write` receives ordered offsets and must not publish partial content.

```c
tr_raft_data_stream_sender_config_t config = {
    .self_id = 1,
    .peer_id = 2,
    .max_stream_bytes = 1024U * 1024U * 1024U,
};
tr_raft_data_stream_sender_t *sender = NULL;
int rc = tr_raft_data_stream_sender_create(&config, &sender);
if (rc == SALTS_OK)
    rc = tr_raft_data_stream_sender_begin(sender, term, stream_id,
                                           bytes, byte_count);
while (rc == SALTS_OK) {
    tr_raft_data_chunk_t chunk;
    rc = tr_raft_data_stream_sender_next(sender, &chunk);
    if (rc == SALTS_OK) {
        tr_raft_transport_payload_t payload = {
            .kind = TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK,
            .data.data_chunk = chunk,
        };
        rc = tr_raft_flowmq_peer_service_enqueue_payload(flowmq, &payload);
        if (rc != SALTS_OK)
            tr_raft_data_stream_sender_cancel(sender, chunk.stream_offset);
    }
}
```

`sender_begin` returns `SALTS_EINVAL` for invalid identity, pointer, or size and
`SALTS_EBUSY` while another stream is active. `sender_next` returns
`SALTS_EBUSY` when its bounded window is full. Receiver callbacks propagate
their error; digest, order, or identity violations return `SALTS_EPROTO`.

After every current member returns a final durable ACK, encode the proposal:

```c
uint8_t descriptor[TR_RAFT_DATA_DESCRIPTOR_ENCODED_SIZE];
tr_raft_proposal_t proposal;
rc = tr_raft_data_quorum_make_proposal(quorum, command_id,
                                       descriptor, &proposal);
if (rc == SALTS_OK)
    rc = tr_raft_core_propose(core, &proposal, &ready);
```

The state machine decodes committed descriptor bytes with
`tr_raft_data_descriptor_decode` and atomically publishes the staged object.
