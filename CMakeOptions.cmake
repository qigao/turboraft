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
option(TURBORAFT_BUILD_CORONET "Build the optional CoroNet transport adapter"
       OFF)
option(TURBORAFT_BUILD_CONTROL_PLANE
       "Build the optional TurboHTTP JSON-RPC and HTMX control plane" OFF)

option(TURBORAFT_BUILD_WILLEMT_REFERENCE
       "Build the pinned willemt/raft reference implementation" OFF)
option(
  TURBORAFT_BUILD_UPSTREAM_TESTS
  "Build the pinned willemt/raft regression suite after its test dependency is audited"
  OFF)
option(BUILD_BENCHMARKS "Build TurboRaft performance benchmarks" OFF)

set(TURBO_UTILS_ROOT
    ""
    CACHE PATH "TurboUtils package prefix")
set(TURBO_NET_ROOT
    ""
    CACHE PATH "TurboNet package prefix")
set(TURBO_HTTP_ROOT
    ""
    CACHE PATH "TurboHttp package prefix")
