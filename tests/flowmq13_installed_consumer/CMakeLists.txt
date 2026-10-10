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
                        WalStorage ChunkReceivers Multicore
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

# This uses the exact same two real mTLS identity + CNet/Multicore fixture as
# the independent CNet qualification. In this separate full-SDK target only,
# Group103's SnapshotReceiver installs into production WalStorage before
# CNet may send a positive SNAPSHOT_ACK. Group101 still uses a mock DATA
# consumer and is required NOT to emit a durable ACK.
add_executable(turboraft_flowmq13_cnet_durable
               ../ace23_cnet_consumer/test_distinct_peer_tls.c)
target_compile_definitions(turboraft_flowmq13_cnet_durable PRIVATE
    TURBORAFT_TEST_DURABLE_NET_SNAPSHOT=1
    TURBORAFT_ACE23_FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../fixtures")
target_link_libraries(turboraft_flowmq13_cnet_durable PRIVATE
    TurboRaft::CNet TurboRaft::Multicore TurboRaft::ChunkReceivers
    TurboRaft::WalStorage Salts::Platform Salts::TinyTest)
add_test(NAME turboraft.flowmq13.cnet_durable_snapshot
         COMMAND turboraft_flowmq13_cnet_durable)
set_tests_properties(turboraft.flowmq13.cnet_durable_snapshot
                     PROPERTIES TIMEOUT 55)

# Test-only fault seam from src/storage: inject one real snapshot staging
# write EIO on the existing Multicore Group Owner after certified mTLS ingress.
# The failed completion cannot turn into a positive network ACK, and a fresh
# process-local WAL reopen must not recover a half-installed snapshot.
add_executable(turboraft_flowmq13_cnet_snapshot_fault
               ../ace23_cnet_consumer/test_distinct_peer_tls.c)
target_compile_definitions(turboraft_flowmq13_cnet_snapshot_fault PRIVATE
    TURBORAFT_TEST_DURABLE_NET_SNAPSHOT=1
    TURBORAFT_TEST_DURABLE_NET_SNAPSHOT_FAILURE=1
    TURBORAFT_ACE23_FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../fixtures")
target_include_directories(turboraft_flowmq13_cnet_snapshot_fault PRIVATE
    "${CMAKE_CURRENT_LIST_DIR}/../../src/storage")
target_link_libraries(turboraft_flowmq13_cnet_snapshot_fault PRIVATE
    TurboRaft::CNet TurboRaft::Multicore TurboRaft::ChunkReceivers
    TurboRaft::WalStorage Salts::Platform Salts::TinyTest)
add_test(NAME turboraft.flowmq13.cnet_snapshot_fault
         COMMAND turboraft_flowmq13_cnet_snapshot_fault)
set_tests_properties(turboraft.flowmq13.cnet_snapshot_fault
                     PROPERTIES TIMEOUT 55)

# Two genuine TLS SNAPSHOT_CHUNK frames (12+12 bytes) share one SHA-256:
# the first Group103 completion may acknowledge forward progress but must
# NEVER claim installed/durable; the second must finish the real WAL publish
# before the single positive network ACK. Separate from the EIO fault case.
add_executable(turboraft_flowmq13_cnet_segmented_snapshot
               ../ace23_cnet_consumer/test_distinct_peer_tls.c)
target_compile_definitions(turboraft_flowmq13_cnet_segmented_snapshot PRIVATE
    TURBORAFT_TEST_DURABLE_NET_SNAPSHOT=1
    TURBORAFT_TEST_DURABLE_NET_SNAPSHOT_SEGMENTED=1
    TURBORAFT_ACE23_FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../fixtures")
target_link_libraries(turboraft_flowmq13_cnet_segmented_snapshot PRIVATE
    TurboRaft::CNet TurboRaft::Multicore TurboRaft::ChunkReceivers
    TurboRaft::WalStorage Salts::Platform Salts::TinyTest)
add_test(NAME turboraft.flowmq13.cnet_segmented_snapshot
         COMMAND turboraft_flowmq13_cnet_segmented_snapshot)
set_tests_properties(turboraft.flowmq13.cnet_segmented_snapshot
                     PROPERTIES TIMEOUT 55)

# Combined bounded multi-part delivery and real owner-local WAL failure:
# the 12-byte interim chunk is accepted but NOT durable; terminal 12-byte
# chunk fails at the first Snapshot staging write. No network success ACK,
# no automatic replay, and no authoritative half-snapshot after reopen.
add_executable(turboraft_flowmq13_cnet_segmented_fault
               ../ace23_cnet_consumer/test_distinct_peer_tls.c)
target_compile_definitions(turboraft_flowmq13_cnet_segmented_fault PRIVATE
    TURBORAFT_TEST_DURABLE_NET_SNAPSHOT=1
    TURBORAFT_TEST_DURABLE_NET_SNAPSHOT_SEGMENTED=1
    TURBORAFT_TEST_DURABLE_NET_SNAPSHOT_FAILURE=1
    TURBORAFT_ACE23_FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../fixtures")
target_include_directories(turboraft_flowmq13_cnet_segmented_fault PRIVATE
    "${CMAKE_CURRENT_LIST_DIR}/../../src/storage")
target_link_libraries(turboraft_flowmq13_cnet_segmented_fault PRIVATE
    TurboRaft::CNet TurboRaft::Multicore TurboRaft::ChunkReceivers
    TurboRaft::WalStorage Salts::Platform Salts::TinyTest)
add_test(NAME turboraft.flowmq13.cnet_segmented_fault
         COMMAND turboraft_flowmq13_cnet_segmented_fault)
set_tests_properties(turboraft.flowmq13.cnet_segmented_fault
                     PROPERTIES TIMEOUT 55)

# Native CTest owns lifecycle-stress selection across Linux epoll, Windows
# IOCP and macOS Kqueue. A repeat is a NEW stand-alone test process with a
# fresh TLS/Owner/WAL fixture, never a retry of an admitted Raft operation.
set_tests_properties(
    turboraft.flowmq13.cnet_durable_snapshot
    turboraft.flowmq13.cnet_snapshot_fault
    turboraft.flowmq13.cnet_segmented_snapshot
    turboraft.flowmq13.cnet_segmented_fault
    PROPERTIES LABELS "cnet-snapshot-lifecycle")

# Exercise the *installed* SnapshotReceiver's durable duplicate semantics:
# the repeated final chunk is accepted without a second install/commit, but
# its full-size ACK still refers to a previously installed snapshot.
add_executable(turboraft_flowmq13_snapshot_replay
               ../core/test_raft_snapshot_receiver.c)
target_link_libraries(turboraft_flowmq13_snapshot_replay PRIVATE
    TurboRaft::SnapshotReceiver Salts::TinyTest)
add_test(NAME turboraft.flowmq13.snapshot_replay
         COMMAND turboraft_flowmq13_snapshot_replay)
set_tests_properties(turboraft.flowmq13.snapshot_replay
                     PROPERTIES TIMEOUT 25)

# Real verified Node3 physical TLS N -> N+1 reconnect with an actual
# WAL-durable Group103 completion held past N teardown. Old Origin is
# rejected before new CNet send credit, and ONLY explicit application
# retransmission through N+1 may return an already-installed full ACK.
# The SnapshotReceiver must not re-run its WAL install on the duplicate.
add_executable(turboraft_flowmq13_cnet_snapshot_reconnect
               ../ace23_cnet_consumer/test_distinct_peer_tls.c)
target_compile_definitions(turboraft_flowmq13_cnet_snapshot_reconnect PRIVATE
    TURBORAFT_TEST_DURABLE_NET_SNAPSHOT=1
    TURBORAFT_TEST_DURABLE_NET_SNAPSHOT_RECONNECT=1
    TURBORAFT_ACE23_FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../fixtures")
target_link_libraries(turboraft_flowmq13_cnet_snapshot_reconnect PRIVATE
    TurboRaft::CNet TurboRaft::Multicore TurboRaft::ChunkReceivers
    TurboRaft::WalStorage Salts::Platform Salts::TinyTest)
add_test(NAME turboraft.flowmq13.cnet_snapshot_reconnect
         COMMAND turboraft_flowmq13_cnet_snapshot_reconnect)
set_tests_properties(turboraft.flowmq13.cnet_snapshot_reconnect
                     PROPERTIES TIMEOUT 65
                                LABELS "cnet-snapshot-lifecycle")

# A real mTLS Node3 connection N sends only offset0..12 and receives one
# accepted *non-durable* SNAPSHOT_ACK. Tear down both physical N endpoints,
# re-authenticate N+1 with the same exact Group103 owner and retained in-RAM
# SnapshotReceiver, reject N's delayed partial receipt before send admission,
# and require explicit host restart-from-zero followed by offset12..24.
# Only the final new-generation receipt may publish WalStorage and ACK.
# No automated replay, raw runtime-state persistence, alternate socket owner
# or additional Group mailbox is created.
add_executable(turboraft_flowmq13_cnet_midstream_reconnect
               ../ace23_cnet_consumer/test_distinct_peer_tls.c)
target_compile_definitions(turboraft_flowmq13_cnet_midstream_reconnect PRIVATE
    TURBORAFT_TEST_DURABLE_NET_SNAPSHOT=1
    TURBORAFT_TEST_DURABLE_NET_SNAPSHOT_SEGMENTED=1
    TURBORAFT_TEST_DURABLE_NET_SNAPSHOT_RECONNECT=1
    TURBORAFT_TEST_DURABLE_NET_SNAPSHOT_MIDSTREAM_RECONNECT=1
    TURBORAFT_ACE23_FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../fixtures")
target_link_libraries(turboraft_flowmq13_cnet_midstream_reconnect PRIVATE
    TurboRaft::CNet TurboRaft::Multicore TurboRaft::ChunkReceivers
    TurboRaft::WalStorage Salts::Platform Salts::TinyTest)
add_test(NAME turboraft.flowmq13.cnet_midstream_reconnect
         COMMAND turboraft_flowmq13_cnet_midstream_reconnect)
set_tests_properties(turboraft.flowmq13.cnet_midstream_reconnect
                     PROPERTIES TIMEOUT 65
                                LABELS "cnet-snapshot-lifecycle")


# Linux-only whole-process crash and fresh verified mTLS peer/WAL recovery.
# The crash writer and recovery writer share only an OS file path passed by
# a separate fork+exec controller. No CNet/Owner/Component runtime handle
# is persisted; exact published SDK targets are linked by BOTH workers.
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  add_executable(turboraft_flowmq13_cnet_crash_writer
                 ../ace23_cnet_consumer/test_distinct_peer_tls.c)
  target_compile_definitions(turboraft_flowmq13_cnet_crash_writer PRIVATE
      TURBORAFT_TEST_PROCESS_ROLE=1
      TURBORAFT_TEST_PROCESS_CRASH_WRITER=1
      TURBORAFT_TEST_DURABLE_NET_SNAPSHOT=1
      TURBORAFT_TEST_DURABLE_NET_SNAPSHOT_SEGMENTED=1
      TURBORAFT_TEST_DURABLE_NET_SNAPSHOT_RECONNECT=1
      TURBORAFT_TEST_DURABLE_NET_SNAPSHOT_MIDSTREAM_RECONNECT=1
      TURBORAFT_ACE23_FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../fixtures")
  target_link_libraries(turboraft_flowmq13_cnet_crash_writer PRIVATE
      TurboRaft::CNet TurboRaft::Multicore TurboRaft::ChunkReceivers
      TurboRaft::WalStorage Salts::Platform)

  add_executable(turboraft_flowmq13_cnet_recover_writer
                 ../ace23_cnet_consumer/test_distinct_peer_tls.c)
  target_compile_definitions(turboraft_flowmq13_cnet_recover_writer PRIVATE
      TURBORAFT_TEST_PROCESS_ROLE=1
      TURBORAFT_TEST_PROCESS_REOPEN=1
      TURBORAFT_TEST_DURABLE_NET_SNAPSHOT=1
      TURBORAFT_TEST_DURABLE_NET_SNAPSHOT_SEGMENTED=1
      TURBORAFT_ACE23_FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../fixtures")
  target_link_libraries(turboraft_flowmq13_cnet_recover_writer PRIVATE
      TurboRaft::CNet TurboRaft::Multicore TurboRaft::ChunkReceivers
      TurboRaft::WalStorage Salts::Platform)

  add_executable(turboraft_flowmq13_cnet_process_restart
                 test_cnet_process_restart.c)
  target_link_libraries(turboraft_flowmq13_cnet_process_restart PRIVATE
      TurboRaft::WalStorage)
  add_test(NAME turboraft.flowmq13.cnet_process_restart
           COMMAND turboraft_flowmq13_cnet_process_restart
                   "$<TARGET_FILE:turboraft_flowmq13_cnet_crash_writer>"
                   "$<TARGET_FILE:turboraft_flowmq13_cnet_recover_writer>")
  set_tests_properties(turboraft.flowmq13.cnet_process_restart
                       PROPERTIES TIMEOUT 65
                                  LABELS "cnet-snapshot-lifecycle;cnet-process-crash;wal-recovery")
endif()

# Independent installed-SDK 64 MiB end-to-end SnapshotSender -> Raft wire
# codec -> SnapshotReceiver/stream sink. No 64 MiB heap allocation or socket
# simulation: sender uses a deterministic read_at source, fixed 64-KiB chunks,
# four explicitly reserved in-flight slots, and cumulative ACK wire frames.
# Linux also gates ru_maxrss <= 48 MiB (not a malloc tracing substitute).
add_executable(turboraft_flowmq13_large_snapshot_stream
               test_large_snapshot_bounded.c)
target_link_libraries(turboraft_flowmq13_large_snapshot_stream PRIVATE
    TurboRaft::SnapshotSender TurboRaft::SnapshotReceiver
    TurboRaft::Core Salts::Core)
add_test(NAME turboraft.flowmq13.large_snapshot_stream
         COMMAND turboraft_flowmq13_large_snapshot_stream)
set_tests_properties(turboraft.flowmq13.large_snapshot_stream
                     PROPERTIES TIMEOUT 90
                                LABELS "snapshot-stream-bounded")

# Three actual independent Raft Core instances exchange Ready messages via
# deterministic bounded SIMULATED links (NOT actual CNet or inter-host TCP).
# Qualify that 2-of-3 majority commits survive an isolated former leader,
# its Core destruction/restart from durable Ready and exact eight-entry log
# convergence after healing. Explicitly named simulation prevents a false
# claim of real distributed quorum or automatic network catch-up.
add_executable(turboraft_flowmq13_three_node_core
               ../core/test_raft_cluster_simulation.c)
target_link_libraries(turboraft_flowmq13_three_node_core PRIVATE
    TurboRaft::Core Salts::TinyTest)
add_test(NAME turboraft.flowmq13.three_node_core
         COMMAND turboraft_flowmq13_three_node_core)
set_tests_properties(turboraft.flowmq13.three_node_core
                     PROPERTIES TIMEOUT 65
                                LABELS "raft-3node-simulation")
