include_guard(GLOBAL)

set(TURBORAFT_BUILD_PROFILE "full" CACHE STRING
    "TurboRaft source build profile: full, core-dev, cnet-dev, or storage-dev")
set_property(CACHE TURBORAFT_BUILD_PROFILE PROPERTY STRINGS
             full core-dev cnet-dev storage-dev)
if(NOT TURBORAFT_BUILD_PROFILE STREQUAL "full" AND
   NOT TURBORAFT_BUILD_PROFILE STREQUAL "core-dev" AND
   NOT TURBORAFT_BUILD_PROFILE STREQUAL "storage-dev" AND
   NOT TURBORAFT_BUILD_PROFILE STREQUAL "cnet-dev")
  message(FATAL_ERROR
          "Unsupported TURBORAFT_BUILD_PROFILE='${TURBORAFT_BUILD_PROFILE}'; expected full, core-dev, cnet-dev, or storage-dev")
endif()

option(BUILD_TESTS "Build the TurboRaft test suite" ON)

option(ENABLE_MSVC_ANALYZE "Enable MSVC static code analysis" OFF)
option(TURBORAFT_BUILD_FUZZERS
       "Build opt-in Clang/libFuzzer protocol fuzz targets" OFF)
option(TURBORAFT_ENABLE_ORM_SQLITE_FIXTURES
       "Build opt-in CFlow/Orm::C SQLite recovery and crash fixtures" OFF)

option(BUILD_BENCHMARKS "Build TurboRaft performance benchmarks" OFF)
option(BUILD_EXAMPLES "Build TurboRaft examples" OFF)

# Source profiles are explicit. The full profile owns transport, text syntax,
# optional integrations, tests, benchmarks and examples. core-dev and
# storage-dev omit unrelated dependencies; cnet-dev builds CNet only, without FlowMQ/CHttp.
# First-party package roots are supplied only by the active CMake user preset.
