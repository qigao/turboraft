# TurboRaft Text Syntax

TurboRaft provides three small text languages:

- Query syntax for read-only inspection plans.
- Protocol-debug syntax for describing protocol frames.
- Replay syntax for deterministic simulation actions.

Configuration DSL is intentionally out of scope.

## Build

CMake uses `re2c` for lexers and the repository-provided Lemon source for
parsers. Lemon is built from `tools/lemon/CMakeLists.txt` as a host tool, so
the runtime library does not depend on either generator.

## Query syntax

```text
# comments begin with '#'
show status;
show members;
show members where role == voter;
show members where role == learner;
show progress for node 2;
```

The parser returns `tr_text_query_plan_t`. It does not inspect or mutate Raft
state. Execution belongs to the caller that owns the relevant read-only view.
All three text syntaxes accept `#` comments through the end of the line.

### Query execution adapter

`tr_text_query_execute()` dispatches the typed plan to a runtime-specific
read-only driver:

```c
tr_text_query_executor_ops_t ops = {
    .status = status_callback,
    .members = members_callback,
    .progress = progress_callback,
};
tr_text_query_execute(&plan, &ops, context);
```

The callbacks map directly to the existing service diagnostics:

- `status` may call `tr_raft_service_status()`.
- `members` may call `tr_raft_service_configuration()` and apply the command's
  `role` filter.
- `progress` may call `tr_raft_service_progress()` and select `node_id`.

The generic adapter does not format output, retain snapshots, or silently
ignore unsupported commands. It executes synchronously in plan order and
returns the first callback error unchanged.

For callers that own a `tr_raft_service_t`, the optional
`TurboRaft::TextQueryService` adapter provides the service mapping:

```c
tr_text_query_execute_service(service, &plan, &sink, context);
```

It calls `tr_raft_service_status()`, `tr_raft_service_configuration()`, or
`tr_raft_service_progress()` as required. Configuration members are filtered
by `where role`; progress selects the exact requested node. The adapter only
borrows each snapshot until its sink callback returns and never starts a read
barrier or advances time.

### Remote query console

`turboraft_console` compiles query DSL locally and invokes the existing typed
JSON-RPC methods. The endpoint is a console session argument, not part of the
DSL:

```text
turboraft_console --endpoint http://127.0.0.1:8080/raft/rpc \
  --query "show members where role == voter;"
```

Without `--query`, the console reads one complete query statement per input
line until `exit` or `quit`. It supports only the read-only query syntax;
protocol-debug and replay text are never sent to a remote Raft node.

## Protocol-debug syntax

```text
frame version 3 kind raft {
    from = 1;
    to = 2;
    term = 7;
    message = append_request;
    payload = 0x0102a0ff;
}
```

Each frame requires `version`, `kind`, `from`, `to`, `term`, and `message`.
Each field may appear at most once; duplicate fields are rejected as semantic
errors.
`payload` is optional and accepts an even number of hexadecimal digits after
`0x`. The parser keeps the literal as a borrowed view; decoding and wire
encoding remain the responsibility of the caller and existing TBE codecs.
The parser returns `tr_text_protocol_debug_plan_t`; it does not encode or send
the frame. Existing wire codecs remain the single binary protocol
implementation.

### Protocol execution adapter

`tr_text_protocol_debug_execute()` validates and decodes complete wire frames
from the `payload` field:

```c
tr_text_protocol_debug_executor_ops_t ops = {
    .frame = on_decoded_frame,
};
tr_text_protocol_debug_execute(&plan, &ops, context);
```

The executor supports `raft`, `snapshot_chunk`, and `snapshot_ack` payload
kinds. It reuses the existing wire codec for envelope validation and typed
decoding, then checks the requested version, kind, endpoints, term, and
message name before invoking the callback. The callback receives a decoded
view only until it returns; it owns any copy needed afterward. Missing payload,
oversized hexadecimal frames, malformed wire data, and plan/frame mismatches
fail fast. Callback errors are returned unchanged.

## Replay syntax

```text
node 1;
node 2;
tick 5;
send node 1 -> node 2;
drop next append_request;
expect node 1 role == leader;
expect node 2 commit_index >= 4;
```

Fault injection actions use the same typed replay plan:

```text
partition node 1 with node 2;
heal node 1 with node 2;
delay next append_request by 3 ticks;
duplicate next append_request;
```

Client submit and bounded polling actions use an explicit request id:

```text
submit request 3 to node 1 client 7 sequence 9 payload 0x0102a0ff;
poll request 3 until committed timeout 20 ticks;
```

`submit` only constructs a client operation; it does not send the request.
`poll` observes the request state up to the specified number of replay ticks;
it does not advance time by itself. Submit payloads are borrowed hexadecimal
views and must contain an even number of digits after `0x`.

The parser returns `tr_text_replay_plan_t`. Applying actions to a simulation
is a separate runtime concern.

### Replay execution adapter

`tr_text_replay_execute()` provides a small synchronous adapter for the
runtime primitives that have stable semantics across drivers:

```c
tr_text_replay_executor_ops_t ops = {
    .submit = submit_callback,
    .poll = poll_callback,
    .tick = tick_callback,
};
tr_text_replay_execute(&plan, &ops, context);
```

`submit` receives the decoded payload and returns a `(term, index)` receipt.
The executor maps that receipt by `request_id`, so `poll` can reject unknown
requests and cannot accidentally use another operation's receipt. Duplicate
request ids are rejected. `poll` receives the declared timeout but never
advances time; only `tick` can do that. The first callback error stops the
trace and is returned unchanged.

Payload decoding uses fixed temporary storage and enforces
`TR_RAFT_MAX_ENTRY_BYTES`. The action and payload views are borrowed only
until the callback returns. `client_id` and `sequence` remain action metadata;
the callback decides how the application layer maps them because the native
`tr_raft_proposal_t` contract currently contains only `command_id` and data.
Node, transport-fault, and expectation actions remain available in the typed
plan but return `TURBO_ENOTSUP` from this generic adapter until a
runtime-specific driver handles them.

## Lifetime and limits

The plan arrays have fixed capacity. Input size and statement count are
bounded by `TR_TEXT_MAX_INPUT_BYTES` and `TR_TEXT_MAX_STATEMENTS`, and callers
may select lower limits with `tr_text_parse_options_t`.

String fields use borrowed `tstr_v` views into the input buffer. Keep the input
buffer alive and unchanged until the plan is no longer used.

On failure, the output plan is cleared and the diagnostic, when provided,
contains the error code, category, byte offset, line, column, and static
message. The parser never silently falls back to another grammar.
