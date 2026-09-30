cmake_minimum_required(VERSION 3.24)
project(turboraft_control_plane_package_consumer C)

if(NOT DEFINED ENV{TURBORAFT_ROOT} OR "$ENV{TURBORAFT_ROOT}" STREQUAL "" OR
   NOT IS_DIRECTORY "$ENV{TURBORAFT_ROOT}")
  message(FATAL_ERROR "TURBORAFT_ROOT must name the installed TurboRaft prefix")
endif()
if(NOT DEFINED ENV{CHTTP_ROOT} OR "$ENV{CHTTP_ROOT}" STREQUAL "" OR
   NOT IS_DIRECTORY "$ENV{CHTTP_ROOT}")
  message(FATAL_ERROR "CHTTP_ROOT must name the installed standalone CHttp prefix")
endif()

file(TO_CMAKE_PATH "$ENV{TURBORAFT_ROOT}" TURBORAFT_PACKAGE_ROOT)

find_package(TurboRaft CONFIG REQUIRED COMPONENTS ControlPlane
             PATHS "${TURBORAFT_PACKAGE_ROOT}" NO_DEFAULT_PATH)

if(NOT TARGET TurboRaft::ControlPlane)
  message(FATAL_ERROR "TurboRaft ControlPlane target was not exported")
endif()
if(NOT TARGET CHttp::Server)
  message(FATAL_ERROR "Standalone CHttp::Server dependency was not resolved")
endif()

add_executable(turboraft_control_plane_package_consumer main.c)
target_link_libraries(turboraft_control_plane_package_consumer
                      PRIVATE TurboRaft::ControlPlane)
