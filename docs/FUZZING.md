# Wire codec fuzz corpus

TurboRaft shares a harness for untrusted wire input across the Raft-message,
snapshot-chunk and snapshot-acknowledgement decoders. A successfully decoded
object must encode and decode again; failure of that invariant aborts the
process. See the [harness](../tests/fuzz/raft_wire_fuzz.c) and
[corpus driver](../tests/core/test_raft_wire_fuzz_corpus.c).

## Deterministic CTest corpus

`BUILD_TESTS=ON` includes `turboraft.wire_fuzz_corpus` in the normal test graph.
The corpus generates valid Raft, snapshot-chunk and snapshot-acknowledgement
frames, then exercises truncations, one-byte mutations, trailing data,
null/empty input and 128 deterministic bounded-noise inputs. This harness does
not provide round-trip coverage for the application data-stream decoders.

From a configured compiler environment with the required SDK inputs:

```powershell
cmake --fresh --preset msvc-release-user
cmake --build --preset msvc-release-user
ctest --preset msvc-release-user -R "^turboraft\.wire_fuzz_corpus$" --output-on-failure
```

Linux uses `gcc-release-user` or one of the
[diagnostic presets](DIAGNOSTICS.md). CI runs the corpus as part of its full
CTest suite.

## Coverage boundary

The harness exports `LLVMFuzzerTestOneInput`, but the current CMake graph does
not register a libFuzzer executable or a fuzzing feature switch. The maintained
entry point is the deterministic CTest corpus; a passing corpus is not evidence
of a coverage-guided fuzzing campaign.

Retain a minimized failing input with the regression that fixes it. Do not add
secrets or unreviewed production network data to the corpus.
