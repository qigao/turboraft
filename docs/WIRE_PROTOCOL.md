# TurboRaft wire protocol

## Version 2 envelope

Every frame has the fixed 40-byte authenticated transport payload header:
magic, protocol version, header size, bounded payload size, payload kind,
cluster ID, and message ID. Version 2 assigns payload kind 1 to ordinary Raft
messages, 2 to InstallSnapshot chunks, and 3 to InstallSnapshot acknowledgements.
Version 1 frames and unknown payload kinds fail with `TURBO_EPROTO`.

## Snapshot transfer

A chunk identifies the sender, receiver, leader term, snapshot index and term,
total size, byte offset, SHA-256 digest, and final-chunk state. Snapshot size is
bounded to 64 MiB and each chunk is at most 512 bytes. Non-final chunks must be
exactly 512 bytes; the final chunk must consume the remaining bytes. Empty
snapshots use one final zero-byte chunk.

An acknowledgement repeats the snapshot identity and reports the next required
offset. The receiver may reject a transfer without changing the durable
snapshot. The peer-service staging layer must verify the digest before calling
the atomic SQLite snapshot installation boundary.

CoroNet stores ordinary Raft messages, snapshot chunks, and acknowledgements in
the same per-peer tagged FIFO. One writer coroutine assigns monotonically
increasing message IDs and writes every payload kind, so snapshot traffic cannot
bypass socket ordering. The reader validates envelope, cluster, message ID, and
peer identity before invoking the payload-specific borrowed callback.

## Generated codec

`schema/turboraft_wire.schema` is the only wire-layout fact source. CMake locates
the TurboUtils `tbe_compiler` and regenerates
`src/wire/generated/turboraft_wire_tbe.h` and `.c` whenever the schema changes.
Configuration fails if the build-time compiler is unavailable; runtime does not
depend on `tbe_compiler`.
