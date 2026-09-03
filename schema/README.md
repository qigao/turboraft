# TurboRaft wire schema

`turboraft_wire.schema` retains the v2 single-entry Raft contract and
defines the v4 snapshot payload with separate canonical ConfState metadata.
`turboraft_wire_v3.schema` is the current bounded batch contract. Generated C
source is stored under `src/wire/generated`; runtime requires DataBind but not
Lemon or re2c. Configure requires the pinned `tbe_compiler` because CMake keeps
both generated contracts synchronized.

Regenerate with the pinned SaltsUtils build-time tool:

```powershell
tbe_compiler schema/turboraft_wire.schema --lang c `
  --output src/wire/generated/turboraft_wire_tbe.h `
  --source-output src/wire/generated/turboraft_wire_tbe.c
```

Any schema change must increment the affected wire version, regenerate both files, and
add old/new compatibility tests before release.

V3 sends at most `TR_RAFT_MAX_APPEND_ENTRIES` explicit entry slots. The public
`tr_raft_wire_encode_version` is the capability-negotiated v2 encoder; there is
no automatic downgrade. Raft decoders accept v2/v3 envelopes; snapshot chunks
require v4 because omitting ConfState is unsafe.
