# TurboRaft.Native

Native TurboRaft SDK package.

Release-qualified SDK payloads:

- `sdk/linux-x64`
- `sdk/windows-x64`

First-party package dependencies resolve the latest stable release:

- Salts.Native (CMake requires 2.1+)
- SaltsUtils.Native (CMake requires 4.2+)
- FlowMQ.Native (built against the matching SDK generation)

The build records the resolved versions in `turboraft-sdk-manifest.txt`.
CI configures, builds and installs through `native-sdk-linux-user` or
`native-sdk-windows-user` and its matching `install-` build preset, with vcpkg
manifest mode enabled. GmSSL-backed hashing is consumed through Salts::Core;
TurboRaft no longer exports an OpenSSL dependency.

Installed CMake consumers require `SALTS_ROOT`, `SALTS_UTILS_ROOT`, and
`TURBORAFT_ROOT`. Core links `Salts::DataBind`, which is supplied by the
SaltsUtils package, while source builds also use SaltsUtils to locate
`salts-idlc`.

`FLOWMQ_ROOT`, `CHTTP_ROOT`, and `TURBODB_ROOT` are required only when the
selected component uses those optional integrations.

macOS and Android are intentionally not shipped in TurboRaft 0.3.x because
they are not release-qualified platforms in `docs/SUPPORTED_PLATFORMS.md`.
