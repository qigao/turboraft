cmake_minimum_required(VERSION 3.23)

project(TurboRaftPackageConsumer LANGUAGES C)

find_package(TurboRaft CONFIG REQUIRED)

add_executable(turboraft_package_consumer main.c)
target_link_libraries(turboraft_package_consumer
                      PRIVATE TurboRaft::Core
                              TurboRaft::Service
                              TurboRaft::SnapshotReceiver
                              TurboRaft::SnapshotSender)

foreach(optional_target IN ITEMS SQLiteStorage CoroNet SnapshotManager ServiceOwner ControlPlane)
    if(TARGET TurboRaft::${optional_target})
        target_link_libraries(turboraft_package_consumer
                              PRIVATE TurboRaft::${optional_target})
    endif()
endforeach()
