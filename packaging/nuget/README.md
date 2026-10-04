# TurboRaft.Native

Native TurboRaft SDK package.

Release-qualified SDK payloads:

- `sdk/linux-x64`
- `sdk/windows-x64`

First-party package policy:

- release builds restore the latest stable released Salts.Native,
  SaltsUtils.Native, FlowMQ.Native, and CHttp.Native packages;
- TurboRaft.Native does not publish fixed transitive versions for those
  producer packages;
- the release SDK manifest records the exact producer versions resolved for
  qualification evidence.

Installed CMake consumers require `SALTS_ROOT`, `SALTS_UTILS_ROOT`, and
`TURBORAFT_ROOT`. Core links `Salts::DataBind`, which is supplied by the
SaltsUtils package, while source builds also use SaltsUtils to locate
`salts-idlc`.

`FLOWMQ_ROOT`, `CHTTP_ROOT`, and `TURBODB_ROOT` are required only when the
selected component uses those optional integrations.

macOS and Android are intentionally not shipped in TurboRaft 0.3.x because
they are not release-qualified platforms in `docs/SUPPORTED_PLATFORMS.md`.
