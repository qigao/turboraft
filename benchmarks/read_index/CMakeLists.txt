cmake_minimum_required(VERSION 3.24)
project(turboraft_read_index_benchmark C)

if(NOT DEFINED ENV{TURBORAFT_ROOT} OR "$ENV{TURBORAFT_ROOT}" STREQUAL "" OR
   NOT IS_DIRECTORY "$ENV{TURBORAFT_ROOT}")
  message(FATAL_ERROR "TURBORAFT_ROOT must name the installed TurboRaft prefix")
endif()

file(TO_CMAKE_PATH "$ENV{TURBORAFT_ROOT}" TURBORAFT_PACKAGE_ROOT)

find_package(TurboRaft CONFIG REQUIRED COMPONENTS Service WalStorage
             PATHS "${TURBORAFT_PACKAGE_ROOT}" NO_DEFAULT_PATH)

add_executable(turboraft_read_index_benchmark benchmark_read_index.c)
target_link_libraries(turboraft_read_index_benchmark
                      PRIVATE TurboRaft::Service TurboRaft::WalStorage)
