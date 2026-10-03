cmake_minimum_required(VERSION 3.24)
project(turboraft_upgrade_compat C)

if(NOT DEFINED ENV{TURBORAFT_ROOT} OR "$ENV{TURBORAFT_ROOT}" STREQUAL "" OR
   NOT IS_DIRECTORY "$ENV{TURBORAFT_ROOT}")
  message(FATAL_ERROR "TURBORAFT_ROOT must name the installed TurboRaft SDK")
endif()
if(NOT DEFINED COMPAT_ROLE OR
   NOT (COMPAT_ROLE STREQUAL "producer" OR COMPAT_ROLE STREQUAL "consumer"))
  message(FATAL_ERROR "COMPAT_ROLE must be producer or consumer")
endif()

file(TO_CMAKE_PATH "$ENV{TURBORAFT_ROOT}" TURBORAFT_PACKAGE_ROOT)
find_package(TurboRaft CONFIG REQUIRED COMPONENTS Core WalStorage
             PATHS "${TURBORAFT_PACKAGE_ROOT}" NO_DEFAULT_PATH)

if(COMPAT_ROLE STREQUAL "producer")
  add_executable(turboraft_v020_fixture_producer producer.c)
  target_link_libraries(turboraft_v020_fixture_producer
                        PRIVATE TurboRaft::WalStorage)
else()
  add_executable(turboraft_current_fixture_consumer consumer.c)
  target_link_libraries(turboraft_current_fixture_consumer
                        PRIVATE TurboRaft::WalStorage)
endif()
