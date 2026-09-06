# TurboDB Redis Explicit Recovery API Plan

## Decision

Add an explicit TurboRaft adapter recovery API. The caller reconnects Redis and
rebuilds the Raft range from WAL before invoking it. The adapter performs no
reconnect, no implicit retry, and no Raft progress update.

`tr_turbodb_redis_state_machine_reconcile_batch` returns a typed result:

- `REPLAYED`: metadata and journal verify the exact range is committed.
- `PENDING`: the verified range may be retried exactly once through the bound
  state-machine callback using the identical entries.

All other TurboDB receipts and all I/O failures fail fast with an error.

## Steps

1. Add the public enum, API contract, and real-Redis red test for
   `PENDING -> apply -> REPLAYED`.
2. Factor request construction and bounded CFlow operation completion so apply
   and reconcile share one input-validation and ownership path.
3. Map only TurboDB `PENDING` and `REPLAYED` receipts to the public enum;
   map GAP, CONFLICT, COMMIT_UNKNOWN, malformed replies, timeouts, and stream
   failures to explicit non-success status.
4. Update the architecture record with the reconnect/WAL/explicit-reconcile
   sequence and its one-retry constraint.
5. Run focused real-Redis tests, all Release CTest, install, and diff checks.

## Verification

```powershell
cmake --build --preset win-release-user --target turboraft_turbodb_redis_state_machine_tests
$env:TURBODB_REDIS_TEST_PORT = '6391'
ctest --preset win-release-user --output-on-failure -R '^turboraft.turbodb_redis_state_machine$'
cmake --build --preset win-release-user
ctest --preset win-release-user --output-on-failure
cmake --build --preset install-win-release-user
git diff --check
```
