cmake_minimum_required(VERSION 3.23)
project(turboraft_flowmq_package_consumer C)

if(NOT DEFINED ENV{TURBORAFT_ROOT} OR "$ENV{TURBORAFT_ROOT}" STREQUAL "")
  message(FATAL_ERROR "TURBORAFT_ROOT must name the installed TurboRaft prefix")
endif()

find_package(TurboRaft 0.2 CONFIG REQUIRED COMPONENTS FlowMQ
             PATHS "$ENV{TURBORAFT_ROOT}" NO_DEFAULT_PATH)

add_executable(turboraft_flowmq_package_consumer main.c)
target_link_libraries(turboraft_flowmq_package_consumer
                      PRIVATE TurboRaft::FlowMQ)
