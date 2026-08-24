include_guard(GLOBAL)

# ENABLE_TESTS is kept for compatibility with the shared project presets.
# BUILD_TESTS is the option consumed by the TurboRaft build graph.
option(ENABLE_TESTS "Enable the tests" ON)
option(BUILD_TESTS "Build the TurboRaft test suite" ON)

option(ENABLE_MSVC_ANALYZE "Enable MSVC static code analysis" OFF)
option(TURBORAFT_BUILD_FUZZERS
       "Build opt-in Clang/libFuzzer protocol fuzz targets" OFF)

option(BUILD_BENCHMARKS "Build TurboRaft performance benchmarks" OFF)
option(BUILD_EXAMPLES "Build TurboRaft examples" OFF)

# TurboNet/TurboHttp-backed adapters (CoroNet transport, snapshot manager,
# service owner, control plane, console) are enabled automatically when the
# corresponding packages are found during configure. First-party package roots
# are supplied only by the active CMake user preset.
