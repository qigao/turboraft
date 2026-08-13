include_guard(GLOBAL)

# ENABLE_TESTS is kept for compatibility with the shared project presets.
# BUILD_TESTS is the option consumed by the TurboRaft build graph.
option(ENABLE_TESTS "Enable the tests" ON)
option(BUILD_TESTS "Build the TurboRaft test suite" ON)

option(ENABLE_MSVC_ANALYZE "Enable MSVC static code analysis" OFF)
option(TURBORAFT_BUILD_FUZZERS
       "Build opt-in Clang/libFuzzer protocol fuzz targets" OFF)

option(TURBORAFT_BUILD_SQLITE_STORAGE
       "Build the optional SQLite durable storage adapter" ON)
option(BUILD_BENCHMARKS "Build TurboRaft performance benchmarks" OFF)
option(BUILD_EXAMPLES "Build TurboRaft examples" OFF)

# TurboNet/TurboHttp-backed adapters (CoroNet transport, snapshot manager,
# service owner, control plane, console) are enabled automatically when the
# corresponding packages are found during configure. A missing package only
# disables the dependent targets; it does not fail the configure.

set(TURBO_UTILS_ROOT
    ""
    CACHE PATH "TurboUtils package prefix")
set(TURBO_NET_ROOT
    ""
    CACHE PATH "TurboNet package prefix")
set(TURBO_HTTP_ROOT
    ""
    CACHE PATH "TurboHttp package prefix")
set(TURBO_FLOW_ROOT
    ""
    CACHE PATH "TurboFlow package prefix")
