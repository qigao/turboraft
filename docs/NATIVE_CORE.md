# Native C11 core

TurboRaft's core is a deterministic C11 state machine. It owns terms, votes,
logs, progress, membership, quorum calculations, ReadIndex, and snapshot
requirements. It has no socket, HTTP, filesystem, or background-thread
dependency.

The core returns work through bounded Ready-style outputs. The service layer
enforces persistence and apply ordering. Ready acknowledgement releases the
admitted work; application acknowledgement advances the proven applied index.
These are separate facts for asynchronous applications; see the
[CFlow contract](architecture/cflow-replicated-state-machine.md). All mutable
core and service calls belong to one owner.

The public `TurboRaft::Core` target links `Salts::Core` and privately uses
`CMeta::CSTL` plus `Salts::DataBind` for generated wire schemas. Transport is a
separate component:

- [raft_transport.h](../include/turboraft/raft_transport.h) provides framing and validated borrowed decode callbacks.
- [raft_cnet_peer.h](../include/turboraft/raft_cnet_peer.h) adapts one authenticated CNet connection without owning
  the CNet poll loop.

Transport enqueue functions copy data before returning success and surface
capacity pressure. They never retry, block indefinitely, silently drop, or
interpret local admission as remote delivery.
