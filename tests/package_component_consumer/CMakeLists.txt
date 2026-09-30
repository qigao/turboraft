cmake_minimum_required(VERSION 3.24)
project(turboraft_component_package_consumer C)

if(NOT DEFINED TURBORAFT_COMPONENT OR TURBORAFT_COMPONENT STREQUAL "")
  message(FATAL_ERROR "TURBORAFT_COMPONENT is required")
endif()
if(NOT DEFINED ENV{TURBORAFT_ROOT} OR "$ENV{TURBORAFT_ROOT}" STREQUAL "" OR
   NOT IS_DIRECTORY "$ENV{TURBORAFT_ROOT}")
  message(FATAL_ERROR "TURBORAFT_ROOT must name the installed TurboRaft prefix")
endif()

set(_turboraft_components
    Core
    Service
    SnapshotReceiver
    SnapshotSender
    DataStream
    WalStorage
    CNet
    SnapshotManager
    CFlowStateMachine
    TextSyntax
    TextQueryService
    TextProtocolDebug
    ReplayDriver
    FlowMQ
    ControlPlane
    TurboDbRedisStateMachine)

set(_turboraft_headers
    "turboraft/raft_core.h"
    "turboraft/raft_service.h"
    "turboraft/raft_snapshot_receiver.h"
    "turboraft/raft_snapshot_sender.h"
    "turboraft/raft_data_stream.h"
    "turboraft/raft_wal_storage.h"
    "turboraft/raft_cnet_peer.h"
    "turboraft/raft_snapshot_manager.h"
    "turboraft/raft_cflow_state_machine.h"
    "turboraft/text_syntax.h"
    "turboraft/text_query_service.h"
    "turboraft/text_protocol_executor.h"
    "turboraft/text_replay_core_driver.h"
    "turboraft/raft_flowmq_peer_service.h"
    "turboraft/raft_control_plane.h"
    "turboraft/turbodb_redis_state_machine.h")

list(FIND _turboraft_components "${TURBORAFT_COMPONENT}" _component_index)
if(_component_index EQUAL -1)
  message(FATAL_ERROR "Unsupported TURBORAFT_COMPONENT: ${TURBORAFT_COMPONENT}")
endif()
list(GET _turboraft_headers ${_component_index} _component_header)

file(TO_CMAKE_PATH "$ENV{TURBORAFT_ROOT}" TURBORAFT_PACKAGE_ROOT)
find_package(TurboRaft CONFIG REQUIRED COMPONENTS ${TURBORAFT_COMPONENT}
             PATHS "${TURBORAFT_PACKAGE_ROOT}" NO_DEFAULT_PATH)

if(NOT TARGET TurboRaft::${TURBORAFT_COMPONENT})
  message(FATAL_ERROR
          "TurboRaft::${TURBORAFT_COMPONENT} was not exported by the installed SDK")
endif()

add_executable(turboraft_component_package_consumer main.c)
target_compile_definitions(
  turboraft_component_package_consumer
  PRIVATE TURBORAFT_COMPONENT_HEADER="${_component_header}")
target_link_libraries(
  turboraft_component_package_consumer
  PRIVATE TurboRaft::${TURBORAFT_COMPONENT})
