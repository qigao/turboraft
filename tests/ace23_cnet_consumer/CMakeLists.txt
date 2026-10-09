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

add_executable(turboraft_ace23_cnet_cpp cnet_cpp.cpp)
target_link_libraries(turboraft_ace23_cnet_cpp PRIVATE TurboRaft::CNet)
add_test(NAME turboraft.ace23_cnet_installed_cpp
         COMMAND turboraft_ace23_cnet_cpp)
