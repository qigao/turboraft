cmake_minimum_required(VERSION 3.23)
project(TurboRaftAce23WalProcessCrash LANGUAGES C)

# Explicitly Linux-only: the test uses fork/exec/waitpid and SIGKILL to
# terminate the real writer process without any clean shutdown sequence.
if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
  message(FATAL_ERROR "Snapshot process-crash qualification requires Linux")
endif()
if(NOT DEFINED ENV{TURBORAFT_ROOT} OR
   "$ENV{TURBORAFT_ROOT}" STREQUAL "" OR
   NOT IS_DIRECTORY "$ENV{TURBORAFT_ROOT}")
  message(FATAL_ERROR "TURBORAFT_ROOT must name the installed storage SDK")
endif()
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)
enable_testing()

file(TO_CMAKE_PATH "$ENV{TURBORAFT_ROOT}" _wal_crash_root)
find_package(TurboRaft 0.3.1 EXACT CONFIG REQUIRED
             COMPONENTS WalStorage SnapshotReceiver
             PATHS "${_wal_crash_root}"
             NO_DEFAULT_PATH NO_CMAKE_FIND_ROOT_PATH)

# The executable links the *installed* TurboRaft::WalStorage binary. Only the
# private test-only replacement hook declaration is read from the source tree.
# This is NOT an installed public-ABI consumer or a new exported test seam.
add_executable(turboraft_wal_process_crash test_snapshot_process_crash.c)
target_include_directories(turboraft_wal_process_crash PRIVATE
    "${CMAKE_CURRENT_LIST_DIR}/../../src/storage")
target_link_libraries(turboraft_wal_process_crash PRIVATE TurboRaft::WalStorage)

foreach(phase IN ITEMS snapshot segment manifest)
  foreach(point IN ITEMS before after)
    add_test(NAME "turboraft.ace23.wal_crash_${phase}_${point}"
             COMMAND turboraft_wal_process_crash "${phase}" "${point}")
    set_tests_properties("turboraft.ace23.wal_crash_${phase}_${point}"
                         PROPERTIES TIMEOUT 30
                                    LABELS "snapshot;process-crash;restart")
  endforeach()
endforeach()

# Real Linux SIGKILL while one SnapshotReceiver has consumed ONLY offset 0..12:
# buffered mode owns transient in-RAM bytes; streamed mode deliberately
# fsyncs a test-local 12-byte raw staging file but never promotes it to WAL.
# The post-SIGKILL process explicitly rejects tail-only resume, discards any
# orphan, retransmits from offset 0, publishes one WAL Snapshot via the
# original installed SnapshotReceiver, then commits/reopens the Raft suffix.
add_executable(turboraft_partial_snapshot_process_crash
               test_partial_snapshot_process_crash.c)
target_link_libraries(turboraft_partial_snapshot_process_crash PRIVATE
    TurboRaft::SnapshotReceiver TurboRaft::WalStorage)
foreach(mode IN ITEMS buffered streamed)
  add_test(NAME "turboraft.ace23.partial_snapshot_crash_${mode}"
           COMMAND turboraft_partial_snapshot_process_crash "${mode}")
  set_tests_properties("turboraft.ace23.partial_snapshot_crash_${mode}"
                       PROPERTIES TIMEOUT 35
                                  LABELS "snapshot;process-crash;restart;receive")
endforeach()
