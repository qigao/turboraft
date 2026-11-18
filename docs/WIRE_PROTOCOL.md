# TurboRaft wire protocol

The peer contract is current-only. A transport session requires a completed
handshake with the current major/minor pair and full frame/chunk limits.
Handshake feature bits are reserved and must be zero. A missing or reduced
contract fails session creation; there is no inferred version or size fallback.
The declarations in [raft_peer_handshake.h](../include/turboraft/raft_peer_handshake.h)
and [raft_wire_codec.h](../include/turboraft/raft_wire_codec.h) define the constants.

Each packet has a four-byte big-endian length prefix followed by the current
version-6 TBE envelope. Its 48-byte header carries magic, protocol version,
header size, payload size and kind, cluster ID, a monotonically increasing
message ID and the group ID. Payloads are Raft messages, snapshot
chunks/acknowledgements or application data-stream chunks/acknowledgements.

Frames with an unexpected version, cluster, source, destination, payload kind,
size or non-increasing message ID fail before dispatch. The decoder accepts
fragmented input and multiple complete packets in one input view. Decoded
pointers remain borrowed only until the callback returns.

Snapshot and data chunks are bounded to 64 KiB. Their acknowledgements report
cumulative offsets; final durability remains an application/storage decision.
Asynchronous CNet queues retain chunk bytes in managed Salts
`mem_buffer_t` storage until successful transport admission or shutdown.

The [current schema](../schema/turboraft_wire.schema) is the wire-layout fact
source. CMake requires the installed SaltsUtils `salts-idlc` and generates one
C binding with `--lang c --binary-codec` under the build tree's `generated/wire`
directory. Generated codecs are not checked in. See the
[generation contract](../schema/README.md) for outputs and dependencies.
Missing required tools fail configuration.

[Peer protocol](PROTOCOL.md) describes authentication and admission;
[Data streams](DATA_STREAM.md) describes durable large-payload staging.
