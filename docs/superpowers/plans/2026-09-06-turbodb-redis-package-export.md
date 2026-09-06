# TurboDB Redis State-Machine Package Export Plan

## Goal

Publish the optional `TurboRaft::TurboDbRedisStateMachine` target from a
TurboRaft installation when the feature is enabled. A downstream consumer must
be able to resolve the target and its TurboDB dependency using only the
configured first-party package roots.

## Boundaries

- Keep the existing top-level `TURBORAFT_INSTALL_TARGETS` installation model;
  do not add another install function or duplicate install rule.
- Preserve optionality: packages built without the Redis adapter must not
  require `TURBODB_ROOT` unless a consumer requests that component.
- Resolve TurboDB only from `TURBODB_ROOT` and fail configuration if its root
  is absent, invalid, or incomplete.

## Steps

1. Add a configure-only package-consumer fixture requiring the Redis component
   and linking its exported target. Verify it fails against the current SDK.
2. Add the optional library to the existing installation export list and record
   whether the configured package contains the component.
3. Extend `TurboRaftConfig.cmake.in` to resolve TurboDB precisely when needed.
4. Reinstall through the Release preset and configure the consumer fixture
   against the installed package; then run the focused and full regressions.
