# TurboRaft wire protocol

The active peer contract is current-only. A transport session requires a
completed handshake result containing all `TR_RAFT_HANDSHAKE_FEATURE_CURRENT`
features, the current major/minor pair, the full frame limit, and the full
snapshot chunk limit. A missing or reduced contract fails session creation;
there is no implicit version, capability, or size fallback.

Each packet has a four-byte big-endian length prefix followed by a bounded TBE
envelope. The envelope carries magic, protocol version, header size, payload
size and kind, cluster ID, and a monotonically increasing message ID. Payloads
are Raft messages, snapshot chunks/acknowledgements, or application data-stream
chunks/acknowledgements.

Frames with an unexpected version, cluster, source, destination, payload kind,
size, or non-increasing message ID fail before dispatch. The decoder accepts
fragmented input and multiple complete packets in one input view, but never
retains a borrowed decode pointer after its callback returns.

Snapshot and data chunks are bounded to 64 KiB. Their acknowledgements report
cumulative offsets; final durability remains an application/storage decision.
Asynchronous CNet and FlowMQ queues retain chunk bytes in managed Salts
`mem_buffer_t` storage until successful transport admission or shutdown.

`schema/turboraft_wire.schema` is the wire-layout fact source. CMake requires
the SaltsUtils `tbe_compiler` and regenerates the checked-in codec sources when
the schema changes. Missing build tools fail configuration.
