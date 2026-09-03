include_guard(GLOBAL)

option(BUILD_TESTS "Build the TurboRaft test suite" ON)

option(ENABLE_MSVC_ANALYZE "Enable MSVC static code analysis" OFF)
option(TURBORAFT_BUILD_FUZZERS
       "Build opt-in Clang/libFuzzer protocol fuzz targets" OFF)

option(BUILD_BENCHMARKS "Build TurboRaft performance benchmarks" OFF)
option(BUILD_EXAMPLES "Build TurboRaft examples" OFF)

# Salts-backed CNet, CHTTP, and CRPC adapters are enabled when their imported
# targets are present. First-party package roots are supplied only by the active
# CMake user preset.
