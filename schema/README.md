# TurboRaft wire schema

[`turboraft_wire.schema`](turboraft_wire.schema) defines the current Raft batch
and snapshot payloads. It contains one bounded message with eight entry slots,
snapshot chunks with canonical ConfState metadata, and snapshot acknowledgements.
The version 6 envelope is defined by `TR_RAFT_WIRE_VERSION` in
[`raft_wire_codec.h`](../raft/include/turboraft/raft_wire_codec.h).

CMake generates one C header/source pair under the selected preset's
`build/<preset>/schema/generated/wire/` directory. The `turboraft_wire_schema`
object target owns generation and compilation; `turboraft_core` consumes it.
The control-plane DTOs are owned by `turboraft_control_schema` under
`build/<preset>/schema/generated/control/`. Generated files are build artifacts
and are not retained in the source tree. Each build directory owns its output;
different presets do not overwrite one another's generated code.

Configure requires `salts-idlc` from the selected `SALTS_UTILS_ROOT`. The wire
build explicitly enables
`--binary-codec` and depends on both the schema and compiler. Generation failure
stops the build; checked-in generated sources are not used as a fallback.
DataBind remains a runtime dependency; the schema compiler is a build-time tool.

Regenerate through the normal project build after loading the SDK environment:

```powershell
cmake --preset native-release-user
cmake --build --preset native-release-user
```

Only the current wire contract is supported. There are no historical schema
copies, version-specific encoders, or downgrade paths. Changes to wire bytes
must update the wire version and the current codec tests under
[`raft/tests/core/`](../raft/tests/core/), including rejection of unsupported frames.
