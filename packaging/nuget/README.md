# TurboRaft.Native

Native TurboRaft SDK package.

Release-qualified SDK payloads:

- `sdk/linux-x64`
- `sdk/windows-x64`

Exact first-party package dependencies:

- Salts.Native 1.8.3
- SaltsUtils.Native 4.1.3
- FlowMQ.Native 1.1.1

Set `SALTS_ROOT`, `SALTS_UTILS_ROOT`, `FLOWMQ_ROOT`, and `TURBORAFT_ROOT`
to the matching restored SDK directories before configuring a CMake consumer.

macOS and Android are intentionally not shipped in TurboRaft 0.2.x because
they are not release-qualified platforms in `docs/SUPPORTED_PLATFORMS.md`.
