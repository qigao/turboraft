# Peer disconnect and reconnect

`tr_raft_coronet_peer_service_disconnect_peer()` intentionally closes one
outbound peer session while preserving that peer's bounded outbound FIFO.

```c
uint64_t now_ms = turbo_monotonic_ms();
int result = tr_raft_coronet_peer_service_disconnect_peer(
    service, peer_node_id, now_ms);
if (result != TURBO_OK) {
    return result;
}

/* Drive the CoroNet context until the active reader observes the close. */
coro_context_run(context, TURBO_RUN_ONCE);

/* The scheduler reconnects at or after now_ms. */
return tr_raft_coronet_peer_service_step(service, turbo_monotonic_ms(),
                                         &step_result);
```

## State and ownership

- The peer service owns the outbound queue; disconnect never removes entries.
- The reader coroutine remains the sole owner responsible for session release.
- The control call closes only the socket and records the reconnect timestamp.
- Reader cleanup resets the outbound scheduler after releasing the old session.
- A peer without an active reader has its scheduler reset immediately.

## Errors

- `TURBO_EINVAL`: null service or zero peer ID.
- `TURBO_EBUSY`: a service step or disconnect for the same peer is active.
- `TURBO_EPIPE`: the service has entered permanent stop.
- `TURBO_EPROTO`: the peer has no configured outbound scheduler.
- Transport close errors are returned without changing the scheduler.
