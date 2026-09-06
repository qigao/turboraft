cmake_minimum_required(VERSION 3.24)
project(turboraft_redis_state_machine_package_consumer C)

if(NOT DEFINED ENV{TURBORAFT_ROOT} OR "$ENV{TURBORAFT_ROOT}" STREQUAL "" OR
   NOT IS_DIRECTORY "$ENV{TURBORAFT_ROOT}")
  message(FATAL_ERROR "TURBORAFT_ROOT must name the installed TurboRaft prefix")
endif()

find_package(TurboRaft CONFIG REQUIRED COMPONENTS TurboDbRedisStateMachine
             PATHS "$ENV{TURBORAFT_ROOT}" NO_DEFAULT_PATH)

if(NOT TARGET TurboRaft::TurboDbRedisStateMachine)
  message(FATAL_ERROR "TurboRaft Redis state-machine target was not exported")
endif()

add_library(turboraft_redis_state_machine_consumer INTERFACE)
target_link_libraries(turboraft_redis_state_machine_consumer
                      INTERFACE TurboRaft::TurboDbRedisStateMachine)
