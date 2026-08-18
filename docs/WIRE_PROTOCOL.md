# TurboRaft wire protocol

## Version 2 envelope

Every frame has the fixed 40-byte authenticated transport payload header:
magic, protocol version, header size, bounded payload size, payload kind,
cluster ID, and message ID. Version 2 assigns payload kind 1 to ordinary Raft
messages, 2 to InstallSnapshot chunks, and 3 to InstallSnapshot acknowledgements.
The current V5 profile adds kind 4 for data-stream chunks and kind 5 for
data-stream acknowledgements.
Version 1 frames and unknown payload kinds fail with `TURBO_EPROTO`.

## Snapshot transfer V4/V5

A chunk identifies the sender, receiver, leader term, snapshot index and term,
total size, byte offset, SHA-256 digest, and final-chunk state. Snapshot size is
bounded to 64 MiB. V4 retains a 512-byte chunk ceiling and stop-and-wait
delivery. V5 raises the hard chunk ceiling to 64 KiB and supports a bounded
four-chunk sender window (256 KiB maximum in flight). Empty snapshots use one
final zero-byte chunk.

An acknowledgement repeats the snapshot identity and reports the next required
offset. V5 treats this as a cumulative ACK and releases every covered sender
slot. The receiver may reject a transfer without changing the durable snapshot.
The optional stream sink writes into caller-owned temporary storage, updates
SHA-256 incrementally, and calls its atomic commit boundary only after the final
digest matches. The legacy install callback remains available for in-memory
staging.

The handshake negotiates V5 only when both peers advertise `SNAPSHOT_V5` and
the negotiated frame/chunk limits can carry V5. Otherwise `SNAPSHOT_V4` selects
V4. There is no implicit downgrade after negotiation.

Snapshot sender configuration must match the selected transport profile:
zero `chunk_size`/`max_inflight_chunks` selects V4 compatibility; V5 uses
64 KiB and four slots. A mismatch fails at the transport boundary instead of
silently changing protocol behavior.

## FlowMQ data streams

Large application bytes use ordered 64 KiB `DATA_CHUNK` frames rather than
larger Raft entries or frames. The default FlowMQ batch contains four chunks
(256 KiB), and `DATA_ACK` reports cumulative offset plus final durable state.
Raft commits only the stream descriptor after voting-quorum durability. The
ownership, backpressure, validation, and apply protocol is specified in
[DATA_STREAM.md](DATA_STREAM.md).

CoroNet stores ordinary Raft messages, snapshot chunks, and acknowledgements in
the same per-peer tagged FIFO. One writer coroutine assigns monotonically
increasing message IDs and writes every payload kind, so snapshot traffic cannot
bypass socket ordering. The reader validates envelope, cluster, message ID, and
peer identity before invoking the payload-specific borrowed callback.

`tr_raft_snapshot_chunk_t::data` is an immutable borrowed view. A decoded view
expires when the receive callback returns. A sender view remains valid until
that sender is reset or destroyed. Asynchronous CoroNet/FlowMQ peer FIFOs copy
snapshot bytes into a retained TurboUtils `mem_buffer_t`; queue removal and
service destruction release the retained handle.

## Generated codec

`schema/turboraft_wire.schema` is the only wire-layout fact source. CMake locates
the TurboUtils `tbe_compiler` and regenerates
`src/wire/generated/turboraft_wire_tbe.h` and `.c` whenever the schema changes.
Configuration fails if the build-time compiler is unavailable; runtime does not
depend on `tbe_compiler`.
