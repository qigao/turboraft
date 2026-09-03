# High-performance replication

TurboRaft uses bounded work at every boundary. The Raft core can pipeline a
fixed number of AppendEntries requests per peer, snapshot and data senders use
fixed windows, and each FlowMQ peer has one fixed-capacity CSTL deque.

FlowMQ is the model for the peer hot path: one owner thread drives one ROUTER
and one DEALER per peer. There are no connector threads, mutex handoffs,
callback rings, hidden pollers, or unbounded queues. `step()` limits receive
and send work so one busy peer cannot monopolize the owner loop.

Payload ownership is explicit. Raft messages copy by value. Snapshot and data
chunks are borrowed at the producer and decoder boundaries; a queue that keeps
them copies the bytes into a Salts `mem_buffer_t`. A queue entry is released
only after FlowMQ or CNet has accepted its encoded frame.

Backpressure never changes the protocol. `SALTS_ENOSPC` means the local queue
is full, and transient send errors keep the same encoded packet and message ID
for retry. A successful local send does not imply remote receipt, durability,
commit, or application.

All capacities, batch limits, HWM values, reconnect intervals, and negotiated
peer contracts are mandatory configuration. Missing or unsupported values fail
during creation; TurboRaft does not infer a legacy profile or silently reduce
limits.

Performance claims require Release benchmarks and must name the measured
boundary. Codec throughput, transport admission, loopback delivery, storage
durability, and end-to-end replication are distinct measurements.
