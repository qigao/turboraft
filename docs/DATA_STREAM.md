# Raft data streams

TurboRaft keeps large bytes out of `AppendEntries`. The transport carries ordered
64 KiB `DATA_CHUNK` frames; Raft commits a 56-byte descriptor only after the
same stream is durable on a voting quorum.

## Protocol

- Identity: `(leader term, stream_id, stream_size, SHA-256 digest)`.
- Ownership: `sender_begin` copies the complete input for small objects.
  `sender_begin_source` takes ownership of a source on success and reads into
  bounded sender-owned chunk slots. CNet enqueue copies accepted chunks into managed
  `mem_buffer_t` storage before returning from enqueue.
- Ordering: receivers accept the exact cumulative `next_offset`. Duplicate
  earlier chunks return the current offset; gaps and identity changes fail.
- Capacity: the caller supplies a nonzero 64-bit `max_stream_bytes`, a chunk
  size from 1 through 64 KiB, and 1 through 16 in-flight chunks (4 recommended).
  Sender chunk storage is `chunk_size * max_inflight_chunks`. Transport item
  and byte budgets are separately configured; none is inferred from stream size.
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

The [public API](../include/turboraft/raft_data_stream.h) and
[formal tests](../tests/core/test_raft_data_stream.c) define the source, sink and
ACK contracts. A sender configuration must supply all capacities explicitly:

```c
tr_raft_data_stream_sender_config_t config = {
    .self_id = 1,
    .peer_id = 2,
    .max_stream_bytes = 1024ULL * 1024ULL * 1024ULL,
    .chunk_size = TR_RAFT_WIRE_MAX_DATA_CHUNK_BYTES,
    .max_inflight_chunks = TR_RAFT_DATA_STREAM_RECOMMENDED_INFLIGHT_CHUNKS,
};
```

The 1 GiB limit above is an application choice, not a protocol ceiling. For
large objects, provide a source with the declared size, SHA-256 digest and a
`read_at` callback that fills each requested bounded chunk. Source ownership
transfers only after `sender_begin_source` succeeds; its release callback runs
when the sender clears or destroys that source. The small-object `sender_begin`
helper instead allocates and copies the entire input.

Drive the sender and transport on their owner. `sender_next` reserves an
in-flight slot and returns a borrowed chunk view. Copy/enqueue it before ACK,
cancel, resume, reset or destruction can release/reuse its slot. If enqueue
rejects the newest chunk, cancel that exact offset and check the cancellation
result before retrying. A successful enqueue is local admission only; deliver
received ACKs to `sender_acknowledge` to release the bounded window. Window
exhaustion returns `SALTS_EBUSY`; it is not transfer completion. Query sender
status to distinguish progress from completion.

Invalid sender identity, capacities, pointers or sizes return `SALTS_EINVAL`.
Beginning another transfer while one is active returns `SALTS_EBUSY`.
Source and receiver callback errors propagate; digest, order or identity
violations return `SALTS_EPROTO`. Reset or destroy aborts an uncommitted sink.

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

Keep `descriptor` alive through the proposal call: the proposal borrows that
storage. The application owns staging-object retention and publication; an
accepted transport frame alone never authorizes publishing bytes.
