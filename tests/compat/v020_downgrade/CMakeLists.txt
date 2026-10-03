cmake_minimum_required(VERSION 3.24)
project(turboraft_v020_downgrade_fixture C)

if(NOT DEFINED ENV{TURBORAFT_ROOT} OR "$ENV{TURBORAFT_ROOT}" STREQUAL "" OR
   NOT IS_DIRECTORY "$ENV{TURBORAFT_ROOT}")
  message(FATAL_ERROR "TURBORAFT_ROOT must name the current installed TurboRaft SDK")
endif()

file(TO_CMAKE_PATH "$ENV{TURBORAFT_ROOT}" TURBORAFT_PACKAGE_ROOT)
find_package(TurboRaft CONFIG REQUIRED COMPONENTS WalStorage
             PATHS "${TURBORAFT_PACKAGE_ROOT}" NO_DEFAULT_PATH)

add_executable(turboraft_v020_downgrade_fixture fixture.c)
target_link_libraries(turboraft_v020_downgrade_fixture
                      PRIVATE TurboRaft::WalStorage)
