# Peer protocol

Peer traffic uses the TurboRaft binary wire format over authenticated CNet
connections. Administration uses CHTTP/CRPC and never enters the
consensus protocol.

Each wire frame has a four-byte big-endian length prefix followed by a bounded
TBE envelope. The decoder accepts fragmented prefixes, fragmented payloads,
and multiple complete frames in one receive view. It rejects oversized frames,
wrong cluster IDs, wrong source/destination nodes, unsupported versions, and
non-increasing message IDs.

The payload kinds are ordinary Raft messages, snapshot chunks and
acknowledgements, and application data-stream chunks and acknowledgements.
Chunk data is borrowed during decode callbacks. Any queue retaining it must
copy it into managed storage.

`tr_raft_handshake_exchange_t` is the transport-neutral capability negotiation
state machine. Deployments run it after authenticating the peer identity and
must pass its completed result into `tr_raft_transport_session_create()`.
Missing results, reduced capabilities, and reduced frame limits fail session
creation; there is no inferred or compatibility contract.

Successful transport admission transfers a copy to bounded local storage. It
does not acknowledge remote receipt, durability, commit, or apply.

See [Wire protocol](WIRE_PROTOCOL.md) for the current envelope and generation
contract. The host authenticates the connection and binds it to the peer
identity before transport admission. All peers must use the current matching contract;
older releases and mixed-version sessions are not qualified.
