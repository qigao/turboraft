# TurboRaft.Native

Native TurboRaft SDK package.

Release-qualified SDK payloads:

- `sdk/linux-x64`
- `sdk/windows-x64`

Exact first-party package dependencies:

- latest published Salts.Native
- latest published SaltsUtils.Native
- latest published FlowMQ.Native

Set `SALTS_ROOT`, `SALTS_UTILS_ROOT`, `FLOWMQ_ROOT`, and `TURBORAFT_ROOT`
to the matching restored SDK directories before configuring a CMake consumer.

macOS and Android are intentionally not shipped in TurboRaft 0.2.x because
they are not release-qualified platforms in `docs/SUPPORTED_PLATFORMS.md`.
