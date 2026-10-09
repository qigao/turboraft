cmake_minimum_required(VERSION 3.23)
project(TurboRaftAce23CNetInstalledConsumer LANGUAGES C CXX)

if(NOT DEFINED ENV{TURBORAFT_ROOT} OR
   "$ENV{TURBORAFT_ROOT}" STREQUAL "" OR
   NOT IS_DIRECTORY "$ENV{TURBORAFT_ROOT}")
  message(FATAL_ERROR "TURBORAFT_ROOT must be the exact installed CNet SDK")
endif()
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
enable_testing()

find_package(TurboRaft 0.3.1 EXACT CONFIG REQUIRED COMPONENTS CNet
             PATHS "$ENV{TURBORAFT_ROOT}"
             NO_DEFAULT_PATH NO_CMAKE_FIND_ROOT_PATH)

add_executable(turboraft_ace23_cnet_c ../core/test_raft_cnet_peer.c)
target_link_libraries(turboraft_ace23_cnet_c
                      PRIVATE TurboRaft::CNet Salts::TinyTest)
add_test(NAME turboraft.ace23_cnet_installed_c
         COMMAND turboraft_ace23_cnet_c)

add_executable(turboraft_ace23_cnet_identity test_identity.c)
target_link_libraries(turboraft_ace23_cnet_identity
                      PRIVATE TurboRaft::CNet Salts::TinyTest)
add_test(NAME turboraft.ace23_cnet_identity_policy
         COMMAND turboraft_ace23_cnet_identity)

add_executable(turboraft_ace23_cnet_identity_tls test_identity_tls.c)
target_link_libraries(turboraft_ace23_cnet_identity_tls
                      PRIVATE TurboRaft::CNet Salts::Platform Salts::TinyTest)
target_compile_definitions(turboraft_ace23_cnet_identity_tls PRIVATE
    TURBORAFT_ACE23_FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../fixtures")
add_test(NAME turboraft.ace23_cnet_identity_tls
         COMMAND turboraft_ace23_cnet_identity_tls)
set_tests_properties(turboraft.ace23_cnet_identity_tls PROPERTIES TIMEOUT 35)

add_executable(turboraft_ace23_peer_channel test_peer_channel.c)
target_link_libraries(turboraft_ace23_peer_channel
                      PRIVATE TurboRaft::CNet Salts::TinyTest)
target_compile_definitions(turboraft_ace23_peer_channel PRIVATE
    TURBORAFT_ACE23_FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../fixtures")
add_test(NAME turboraft.ace23_peer_channel
         COMMAND turboraft_ace23_peer_channel)
set_tests_properties(turboraft.ace23_peer_channel PROPERTIES TIMEOUT 35)

add_executable(turboraft_ace23_managed_peer test_managed_peer.c)
target_link_libraries(turboraft_ace23_managed_peer
                      PRIVATE TurboRaft::CNet Salts::Platform Salts::TinyTest)
target_compile_definitions(turboraft_ace23_managed_peer PRIVATE
    TURBORAFT_ACE23_FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../fixtures")
add_test(NAME turboraft.ace23_managed_peer
         COMMAND turboraft_ace23_managed_peer)
set_tests_properties(turboraft.ace23_managed_peer PROPERTIES TIMEOUT 45)

add_executable(turboraft_ace23_peer_directory test_peer_directory.c)
target_link_libraries(turboraft_ace23_peer_directory
                      PRIVATE TurboRaft::CNet Salts::Platform Salts::TinyTest)
add_test(NAME turboraft.ace23_peer_directory
         COMMAND turboraft_ace23_peer_directory)
set_tests_properties(turboraft.ace23_peer_directory PROPERTIES TIMEOUT 15)

add_executable(turboraft_ace23_distinct_peer_tls test_distinct_peer_tls.c)
target_link_libraries(turboraft_ace23_distinct_peer_tls
                      PRIVATE TurboRaft::CNet Salts::TinyTest)
target_compile_definitions(turboraft_ace23_distinct_peer_tls PRIVATE
    TURBORAFT_ACE23_FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../fixtures")
add_test(NAME turboraft.ace23_distinct_peer_tls
         COMMAND turboraft_ace23_distinct_peer_tls)
set_tests_properties(turboraft.ace23_distinct_peer_tls PROPERTIES TIMEOUT 45)

add_executable(turboraft_ace23_cnet_cpp cnet_cpp.cpp)
target_link_libraries(turboraft_ace23_cnet_cpp PRIVATE TurboRaft::CNet)
add_test(NAME turboraft.ace23_cnet_installed_cpp
         COMMAND turboraft_ace23_cnet_cpp)
