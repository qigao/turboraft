cmake_minimum_required(VERSION 3.23)

project(TurboRaftPackageConsumer LANGUAGES C)

include(${CMAKE_CURRENT_LIST_DIR}/../../cmake/CmakeUtils.cmake)

find_package(
  TurboRaft CONFIG REQUIRED
  COMPONENTS Core CFlowStateMachine Service SnapshotReceiver SnapshotSender
             SnapshotManager CNet ControlPlane FlowMQ)

add_executable(turboraft_package_consumer main.c)
target_link_libraries(turboraft_package_consumer
                      PRIVATE TurboRaft::Core
                              TurboRaft::CFlowStateMachine
                              TurboRaft::Service
                              TurboRaft::SnapshotReceiver
                              TurboRaft::SnapshotSender
                              TurboRaft::SnapshotManager
                              TurboRaft::CNet
                              TurboRaft::ControlPlane
                              TurboRaft::FlowMQ)

cmake_config_directory_targets(FOLDER "tests/package")
