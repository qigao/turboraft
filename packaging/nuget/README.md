# TurboRaft.Native

Native TurboRaft SDK package.

Release-qualified SDK payloads:

- `sdk/linux-x64`
- `sdk/windows-x64`

Exact first-party package dependencies:

- Salts.Native 1.8.3
- SaltsUtils.Native 4.1.3
- FlowMQ.Native 1.1.1

Installed CMake consumers require `SALTS_ROOT`, `SALTS_UTILS_ROOT`, and
`TURBORAFT_ROOT`. Core links `Salts::DataBind`, which is supplied by the
SaltsUtils package, while source builds also use SaltsUtils to locate
`salts-idlc`.

`FLOWMQ_ROOT`, `CHTTP_ROOT`, and `TURBODB_ROOT` are required only when the
selected component uses those optional integrations.

macOS and Android are intentionally not shipped in TurboRaft 0.3.x because
they are not release-qualified platforms in `docs/SUPPORTED_PLATFORMS.md`.
