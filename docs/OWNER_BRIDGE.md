# Control-plane owner bridge

The control plane may run on a CoroNet context different from the Raft service
owner. Mutable service state remains owned by the owner context; HTTP and RPC
handlers submit service reads and writes through the same bounded owner queue.

## Protocol

- Producers: one or more RPC coroutines; consumer: the single service owner.
- Capacity and command-size admission use `tr_raft_service_owner_submit()`.
- `owner_bridge_timeout_ms` bounds time spent in `QUEUED`; zero selects 5000 ms.
- A queued timeout atomically changes `QUEUED` to `CANCELED`. The owner drains
  the entry without invoking its service command, so returning timeout cannot
  hide a later mutation.
- Once the owner changes a call to `RUNNING`, the request waits for `COMPLETE`.
  Service commands are non-blocking owner-loop operations; returning early here
  would permit a client retry after an operation had already taken effect.
- The bridge copies the top-level payload. Borrowed nested pointers remain valid
  because they are dereferenced only while the request is waiting in `RUNNING`;
  canceled calls never dereference them.
- The request and owner each retain one envelope reference. The final reference
  destroys the idle CoroNet wait and envelope.

## Shutdown

`tr_raft_service_owner_stop()` rejects new submissions and accepted bridge calls
remain in the owner post queue. The owner context must continue running until
`tr_raft_service_owner_close()` returns `TURBO_OK`; `TURBO_EBUSY` means accepted
work or the tick coroutine still needs to drain.

The control plane must be destroyed before the owner context and service. The
owner context and RPC context must be driven concurrently when they differ.
