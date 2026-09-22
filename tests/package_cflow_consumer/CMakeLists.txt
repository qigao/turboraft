cmake_minimum_required(VERSION 3.24)
project(turboraft_cflow_package_consumer C)

if(NOT DEFINED ENV{TURBORAFT_ROOT} OR "$ENV{TURBORAFT_ROOT}" STREQUAL "" OR
   NOT IS_DIRECTORY "$ENV{TURBORAFT_ROOT}")
  message(FATAL_ERROR "TURBORAFT_ROOT must name the installed TurboRaft prefix")
endif()

find_package(TurboRaft CONFIG REQUIRED COMPONENTS CFlowStateMachine
             PATHS "$ENV{TURBORAFT_ROOT}" NO_DEFAULT_PATH)

if(NOT TARGET TurboRaft::CFlowStateMachine)
  message(FATAL_ERROR "TurboRaft CFlowStateMachine target was not exported")
endif()

add_executable(turboraft_cflow_package_consumer main.c)
target_link_libraries(turboraft_cflow_package_consumer
                      PRIVATE TurboRaft::CFlowStateMachine)
