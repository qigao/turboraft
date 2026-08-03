# Wire Codec Fuzzing

TurboRaft uses one shared harness for the untrusted wire envelope and all three payload decoders: Raft messages, snapshot chunks, and snapshot acknowledgements. Every successfully decoded object must encode and decode again; failure of that invariant terminates the process so a fuzzing engine records the input.

## Deterministic CTest corpus

The default test suite builds `turboraft_wire_fuzz_corpus_tests`. It generates valid frames for every payload kind, then exercises every truncation, one-byte mutations, trailing data, null/empty input, and 128 deterministic bounded-noise inputs.

```powershell
cmake --build --preset win-release-user --target turboraft_wire_fuzz_corpus_tests
ctest --preset win-release-user -R turboraft.wire_fuzz_corpus --output-on-failure
```

This path is compiler-independent and is intended for every CI run. It is a regression corpus, not a replacement for coverage-guided fuzzing.

## Clang/libFuzzer target

Configure a separate Clang build with `TURBORAFT_BUILD_FUZZERS=ON`. Configuration fails fast for compilers that do not provide the libFuzzer instrumentation used by the target.

```bash
cmake --preset linux-dev-user --fresh \
  -DTURBORAFT_BUILD_FUZZERS=ON \
  -DCMAKE_C_COMPILER=clang
cmake --build --preset linux-dev-user --target turboraft_wire_fuzzer
cmake -E make_directory build/fuzz-corpus/wire
./build/linux-gcc-debug/bin/turboraft_wire_fuzzer \
  -max_len=8232 -timeout=10 -max_total_time=3600 build/fuzz-corpus/wire
```

Use an isolated corpus directory and retain minimized crash inputs in the issue or regression test that fixes the defect. Do not add unreviewed, secret, or production-derived network data to the repository.

The instrumentation and runtime flags follow the [LLVM libFuzzer documentation](https://llvm.org/docs/LibFuzzer.html).
