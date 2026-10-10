cmake_minimum_required(VERSION 3.23)
project(TurboRaftAce23StorageInstalledConsumer LANGUAGES C CXX)

if(NOT DEFINED ENV{TURBORAFT_ROOT} OR
   "$ENV{TURBORAFT_ROOT}" STREQUAL "" OR
   NOT IS_DIRECTORY "$ENV{TURBORAFT_ROOT}")
  message(FATAL_ERROR "TURBORAFT_ROOT must name the exact installed storage SDK")
endif()
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
enable_testing()

find_package(TurboRaft 0.3.1 EXACT CONFIG REQUIRED
             COMPONENTS ChunkReceivers SnapshotReceiver DataStream
             PATHS "$ENV{TURBORAFT_ROOT}"
             NO_DEFAULT_PATH NO_CMAKE_FIND_ROOT_PATH)

add_executable(turboraft_ace23_chunk_receivers test_chunk_receivers.c)
target_link_libraries(turboraft_ace23_chunk_receivers
                      PRIVATE TurboRaft::ChunkReceivers Salts::TinyTest)
add_test(NAME turboraft.ace23_chunk_receivers
         COMMAND turboraft_ace23_chunk_receivers)
set_tests_properties(turboraft.ace23_chunk_receivers PROPERTIES TIMEOUT 25)

add_executable(turboraft_ace23_chunk_owner test_chunk_owner.c)
target_link_libraries(turboraft_ace23_chunk_owner
                      PRIVATE TurboRaft::ChunkReceivers
                              Salts::Platform Salts::TinyTest)
add_test(NAME turboraft.ace23_chunk_owner
         COMMAND turboraft_ace23_chunk_owner)
set_tests_properties(turboraft.ace23_chunk_owner PROPERTIES TIMEOUT 25)
