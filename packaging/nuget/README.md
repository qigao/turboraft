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

Restore the first-party SDK graph together with `TurboRaft.Native`, then set
`SALTS_ROOT`, `SALTS_UTILS_ROOT`, and `TURBORAFT_ROOT` to the unique SDK
directories for the target RID. Core links `Salts::DataBind`, which is
supplied by SaltsUtils; source builds also use SaltsUtils to locate
`salts-idlc`. The package intentionally does not freeze a first-party
dependency epoch.

`FLOWMQ_ROOT`, `CHTTP_ROOT`, and `TURBODB_ROOT` are required only when the
selected component uses those optional integrations.

macOS and Android are intentionally not shipped in TurboRaft 0.3.x because
they are not release-qualified platforms in `docs/SUPPORTED_PLATFORMS.md`.
