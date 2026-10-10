cmake_minimum_required(VERSION 3.23)
project(TurboRaftAce23InstalledConsumer LANGUAGES C CXX)

if(NOT DEFINED ENV{TURBORAFT_ROOT} OR
   "$ENV{TURBORAFT_ROOT}" STREQUAL "" OR
   NOT IS_DIRECTORY "$ENV{TURBORAFT_ROOT}")
  message(FATAL_ERROR "TURBORAFT_ROOT must be the exact installed new-only SDK")
endif()
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
enable_testing()

find_package(TurboRaft 0.3.1 EXACT CONFIG REQUIRED COMPONENTS Component
             PATHS "$ENV{TURBORAFT_ROOT}"
             NO_DEFAULT_PATH NO_CMAKE_FIND_ROOT_PATH)

add_executable(turboraft_ace23_installed_c ../core/test_raft_component.c)
target_link_libraries(turboraft_ace23_installed_c
                      PRIVATE TurboRaft::Component Salts::TinyTest)
add_test(NAME turboraft.ace23_installed_c
         COMMAND turboraft_ace23_installed_c)

add_executable(turboraft_ace23_installed_cpp component_cpp.cpp)
target_link_libraries(turboraft_ace23_installed_cpp
                      PRIVATE TurboRaft::Component)
add_test(NAME turboraft.ace23_installed_cpp
         COMMAND turboraft_ace23_installed_cpp)
