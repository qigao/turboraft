# CHTTP/CRPC control plane

`TurboRaft::ControlPlane` owns a bounded CRPC server and its background CHTTP
worker. It exposes:

- JSON-RPC method `raft.status` at `/raft/rpc`
- `GET /raft/status` returning the same status snapshot as JSON

The application supplies the complete `crpc_server_config`, including all
network, request, route, and response bounds. It also supplies a status
provider callback. CRPC invokes that callback on the server owner thread; the
provider must use an executor or bounded mailbox when the Raft service belongs
to another owner.

Lifecycle is explicit:

1. `tr_raft_control_plane_create()` initializes routes but does not listen.
2. Optional CHTTP middleware/routes may be added through
   `tr_raft_control_plane_http()`.
3. `tr_raft_control_plane_start()` starts the listener.
4. `tr_raft_control_plane_stop()` stops and drains within the configured
   timeout.
5. `tr_raft_control_plane_destroy()` releases a stopped server.

Destroying a started server returns `SALTS_EBUSY`. This keeps listener shutdown
and callback quiescence visible to the owner.
