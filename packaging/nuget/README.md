# TurboRaft.Native

Native TurboRaft SDK package.

Release-qualified SDK payloads:

- `sdk/linux-x64`
- `sdk/windows-x64`

Exact first-party package dependencies:

- Salts.Native 1.8.3
- SaltsUtils.Native 4.1.3
- FlowMQ.Native 1.1.1

Installed CMake consumers require `SALTS_ROOT` and `TURBORAFT_ROOT`.
`FLOWMQ_ROOT`, `CHTTP_ROOT`, and `TURBODB_ROOT` are required only when the
selected component uses those optional integrations.

`SALTS_UTILS_ROOT` is a **source-build/tooling** dependency used to locate
`salts-idlc`; it is not required by installed TurboRaft targets.

macOS and Android are intentionally not shipped in TurboRaft 0.2.x because
they are not release-qualified platforms in `docs/SUPPORTED_PLATFORMS.md`.
