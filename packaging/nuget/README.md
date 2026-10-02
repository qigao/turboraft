# TurboRaft.Native

Native TurboRaft SDK package.

Release-qualified SDK payloads:

- `sdk/linux-x64`
- `sdk/windows-x64`

First-party dependency policy:

- restore the latest available `Salts.Native`
- restore the latest available `SaltsUtils.Native`
- restore the latest available `FlowMQ.Native`
- restore the latest available `CHttp.Native` when consuming ControlPlane
- do not encode first-party version numbers in consumer configuration

Restore producer packages explicitly together with `TurboRaft.Native`.
Installed CMake consumers require `SALTS_ROOT`, `SALTS_UTILS_ROOT`, and
`TURBORAFT_ROOT`; Core links `Salts::DataBind`, and source builds use
SaltsUtils to locate `salts-idlc`.

`FLOWMQ_ROOT`, `CHTTP_ROOT`, and `TURBODB_ROOT` are required only when the
selected component uses those optional integrations. The packed TurboRaft
package intentionally does not freeze a first-party dependency epoch.

macOS and Android are intentionally not shipped in TurboRaft 0.3.x because
they are not release-qualified platforms in `docs/SUPPORTED_PLATFORMS.md`.
