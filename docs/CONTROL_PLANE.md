# CHTTP Application control plane

`TurboRaft::ControlPlane` owns a bounded `CHttp::App` Application. It exposes
JSON-RPC method `raft.status` at `/raft/rpc` and `GET /raft/status`.

The caller supplies `chttp_application_config` through `config.server`, including
HTTP, RPC, worker and binding budgets. Optional Controllers, components and
middleware belong in that configuration before creation. The Application binds
the internal Controller to the owning plane; it borrows that plane until close
has settled. DTOs and status output are local to each invocation.

The status provider runs on Application workers and may be called concurrently.
Use an executor or bounded mailbox when the Raft service belongs to another
owner. `stop_timeout_ms` must be nonzero and bounds close/drain. See the
[configuration and lifecycle API](../include/turboraft/raft_control_plane.h).

Creation initializes bindings without listening. Start begins listening; stop
closes and drains the Application; start may initialize a fresh Application
using the retained configuration after a successful stop. Destroy releases a
stopped plane and returns `SALTS_EBUSY` while it is started. If Application
initialization leaves cleanup ownership after failure, creation returns that
plane through its output and the caller must destroy it. Configuration metadata
and callback contexts must remain alive until successful destruction.

The old standalone CRPC server and borrowed HTTP-engine accessor were removed
from CHttp. TurboRaft now declares its RPC operation through
[schema/turboraft_control.schema](../schema/turboraft_control.schema), generated
native DTO bindings and DataBind execution metadata. This uses the Application's
RPC parser, dispatcher and lifetime rules. There is no parallel legacy path.
Callers migrate `crpc_server_config` to `chttp_application_config` and put route
customization in the Application configuration. Rolling back requires the
matching old CHttp SDK and source together, selected explicitly by the caller.

The Service projects the `RaftStatus` record directly into the JSON-RPC
`result` object. Its fields are node_id, leader_id, term, commit_index,
applied_index, faulted and role. Provider failure retains code `-32001`; the
schema-based error uses message `StatusUnavailable` and
`error.data.reason = "Raft status unavailable"`. GET failure remains HTTP 503.

Since 0.1.5, SaltsUtils supplies native storage for empty C records and admits empty
request and response roots when compiling a Service binding. `StatusRequest`
therefore retains zero logical and wire fields. The formal
`turboraft.control_plane` test covers creation, both status transports and provider
failure; run it after rebuilding against the selected SDK. TurboRaft does not
inject a dummy wire parameter or bypass the binding compiler.
