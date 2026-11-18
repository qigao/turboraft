# Control-plane audit sink

The audit adapter provides a fixed-metadata sink for caller-emitted
authorization, acceptance and completion events. It never accepts command
bytes, application payloads, credentials, certificates, headers, or snapshot
content.

The current [control plane](CONTROL_PLANE.md) is status-only. Its
[configuration](../include/turboraft/raft_control_plane.h) has no audit-handle
field, and it does not automatically emit mutation audit events. Applications
that use the [audit API](../include/turboraft/raft_control_audit.h) own emission
and enforcement at their command boundary.

## Lifecycle

`tr_raft_control_audit_t` is an opaque, caller-owned handle:

```c
#include <turboraft/raft_control_audit.h>

static int write_audit(void *context,
                       const tr_raft_control_audit_event_t *event)
{
    audit_writer_t *writer = context;
    return audit_writer_append(writer, event, event->size);
}

tr_raft_control_audit_config_t config =
    TR_RAFT_CONTROL_AUDIT_CONFIG_INIT;
tr_raft_control_audit_t *audit = NULL;

config.sink = write_audit;
config.context = writer;
config.required = 1U;
if (tr_raft_control_audit_create(&config, &audit) != SALTS_OK) {
    return STARTUP_AUDIT_ERROR;
}

/* The configured sink and context must outlive all emit calls. */
tr_raft_control_audit_destroy(audit);
```

Creation allocates the opaque adapter once. Emission does not allocate. The
caller destroys the handle only after all producers are quiescent.

The adapter borrows its sink and context. Stop all emitters before destroying
the handle, then release the sink context.

## Configuration

- `version`: must equal `TR_RAFT_CONTROL_AUDIT_VERSION`.
- `size`: must be at least the current config structure size.
- `sink`: callback that consumes one borrowed event during the call.
- `context`: borrowed callback context.
- `required`: `0` for optional or `1` for fail-closed audit.
- `reserved`: must be zero-initialized for forward compatibility.

Use `TR_RAFT_CONTROL_AUDIT_CONFIG_INIT` rather than positional initialization.
Creation returns `SALTS_EINVAL` for an invalid version, size, required value, or
a required configuration without a sink, and `SALTS_ENOMEM` if the opaque
adapter cannot be allocated.

## Emission

`tr_raft_control_audit_emit()` assigns the event version, size, and monotonic
per-instance sequence before invoking the sink. Method and phase must use the
declared typed constants.

For required audit, a sink failure is returned unchanged to the caller. The
application must check that result before submitting a command whose audit is
required. For optional
audit, a missing or failing sink increments the atomic dropped-event counter
and returns success. Read the counter with
`tr_raft_control_audit_dropped()`.

The adapter does not submit commands, maintain a control-plane fault state or
roll back durable work. If a completion event fails after a command commits,
the application must retain the real command outcome separately from the audit
error; an audit failure is not proof that retrying the command is safe.

The sink may be called concurrently and must provide its own output
synchronization. Avoid re-entering the emitting command boundary from the sink.

## ABI and redaction

- The handle is opaque; atomics and implementation layout do not cross the ABI.
- Event method, phase, outcome, version, and size fields have fixed widths.
- Caller identity is a fixed 32-byte fingerprint, never a raw token or
  certificate subject.
- Node, term, and index fields are identifiers only. No variable-length field
  exists in the event structure.
