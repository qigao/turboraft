# New-only runtime and storage policy (ACE 2.3 transition)

TurboRaft Next intentionally drops historical v0.2.0 binary, API, peer
protocol, WAL downgrade and mixed-version cluster compatibility.
There is no automatic fallback, automatic upgrade, legacy shim, ABI alias or
best-effort parser path. Upgrade/downgrade tooling tied to a released v0.2.0
binary is removed on the development branch.

## Still mandatory

- Exact current-generation native ABI and package admission. Until an
  immutable Salts 2.3 candidate passes [upstream #1018](https://github.com/qigao/salts/issues/1018),
  this branch is **not** a supported release and is allowed to fail closed.
- Never interpret unknown wire, metadata, WAL, snapshot or configuration
  versions as if they were current. Reject incompatible facts *before mutation*.
- Keep crash recovery, interrupted WAL writes, checksums, snapshot integrity,
  protocol malformed-input handling and deterministic Raft replay tests.
  Dropping v0.2.0 compatibility does **not** grant permission to corrupt or
  silently delete existing user data.
- Mixed binary versions must not form a supported cluster. Members use one
  exact negotiated current protocol/ABI and validated node identity. A
  mismatched peer fails the handshake rather than downgrading.
- Application business data and applied index commit atomically under the
  application storage provider. Raw Component/Actor/Runtime state is not
  persisted or reconstructed as a compatibility mechanism.

For an existing deployment running an older release, choose a **separate
planned cutover** (new cluster or independently verified offline export/import).
No in-place migration guarantee is made. Destructive conversion of user data
is never an implicit side effect of startup.

See [ACE 2.3 architecture](ACE23_ARCHITECTURE.md), [#142](https://github.com/qigao/turboraft/issues/142)
and [#143](https://github.com/qigao/turboraft/issues/143).
