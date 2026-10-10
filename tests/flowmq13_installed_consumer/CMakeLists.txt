cmake_minimum_required(VERSION 3.23)
project(TurboRaftFlowMQ13Installed LANGUAGES C CXX)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
enable_testing()

foreach(root SALTS_ROOT SALTS_UTILS_ROOT FLOWMQ_ROOT TURBORAFT_ROOT)
  if(NOT DEFINED ENV{${root}} OR
     "$ENV{${root}}" STREQUAL "" OR
     NOT IS_DIRECTORY "$ENV{${root}}")
    message(FATAL_ERROR "Required exact installed SDK root is absent: ${root}")
  endif()
endforeach()

# Normalize host paths before passing them through find_package/vcpkg macros:
# raw Windows backslashes (for example D:\\a) are not valid CMake escapes.
file(TO_CMAKE_PATH "$ENV{SALTS_ROOT}" _flowmq13_salts_root)
file(TO_CMAKE_PATH "$ENV{FLOWMQ_ROOT}" _flowmq13_flowmq_root)
file(TO_CMAKE_PATH "$ENV{TURBORAFT_ROOT}" _flowmq13_turboraft_root)
find_package(Salts 2.3.0 EXACT CONFIG REQUIRED
             PATHS "${_flowmq13_salts_root}" NO_DEFAULT_PATH NO_CMAKE_FIND_ROOT_PATH)
find_package(FlowMQ 1.3.0 EXACT CONFIG REQUIRED
             PATHS "${_flowmq13_flowmq_root}" NO_DEFAULT_PATH NO_CMAKE_FIND_ROOT_PATH)
find_package(TurboRaft 0.3.1 EXACT CONFIG REQUIRED
             COMPONENTS FlowMQ SnapshotSender SnapshotReceiver CNet
             PATHS "${_flowmq13_turboraft_root}"
             NO_DEFAULT_PATH NO_CMAKE_FIND_ROOT_PATH)

# Reuse the existing formal TurboRaft test suite, rather than a header-only
# smoke: real FlowMQ 1.3.0 ROUTER/DEALER, identity-policy rejection,
# group-bound queue limits, TLS, Raft send/receive and snapshot streaming.
add_executable(turboraft_flowmq13_formal
               ../core/test_raft_flowmq_peer_service.c)
target_link_libraries(turboraft_flowmq13_formal PRIVATE
    TurboRaft::FlowMQ TurboRaft::SnapshotSender
    TurboRaft::SnapshotReceiver Salts::CNet Salts::TinyTest)
target_compile_definitions(turboraft_flowmq13_formal PRIVATE
    TURBORAFT_TEST_FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../fixtures")
add_test(NAME turboraft.flowmq13.formal COMMAND turboraft_flowmq13_formal)
set_tests_properties(turboraft.flowmq13.formal PROPERTIES TIMEOUT 45)

# Independently installed C11 + C++17 product consumers exercise the
# exported TurboRaft::FlowMQ target against the 1.3.0 SDK without linking
# the source tree or using a provider/compatibility fallback.
add_executable(turboraft_flowmq13_c11
               ../package_flowmq_consumer/main.c)
target_link_libraries(turboraft_flowmq13_c11 PRIVATE TurboRaft::FlowMQ)
add_test(NAME turboraft.flowmq13.c11 COMMAND turboraft_flowmq13_c11)

add_executable(turboraft_flowmq13_cpp flowmq13_cpp.cpp)
target_link_libraries(turboraft_flowmq13_cpp PRIVATE TurboRaft::FlowMQ)
add_test(NAME turboraft.flowmq13.cpp17 COMMAND turboraft_flowmq13_cpp)
