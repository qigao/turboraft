cmake_minimum_required(VERSION 3.23)
project(turboraft_upgrade_fixture C)

if(NOT DEFINED ENV{TURBORAFT_ROOT} OR "$ENV{TURBORAFT_ROOT}" STREQUAL "" OR
   NOT IS_DIRECTORY "$ENV{TURBORAFT_ROOT}")
  message(FATAL_ERROR "TURBORAFT_ROOT must name an installed TurboRaft SDK")
endif()

file(TO_CMAKE_PATH "$ENV{TURBORAFT_ROOT}" TURBORAFT_UPGRADE_ROOT)

# WalStorage's exported static link closure uses both packages.
find_package(OpenSSL REQUIRED)
find_package(xxHash CONFIG REQUIRED)
find_package(TurboRaft CONFIG REQUIRED COMPONENTS Core WalStorage
             PATHS "${TURBORAFT_UPGRADE_ROOT}" NO_DEFAULT_PATH)

add_executable(turboraft_upgrade_fixture wal_wire_fixture.c)
target_compile_features(turboraft_upgrade_fixture PRIVATE c_std_11)
target_link_libraries(turboraft_upgrade_fixture
                      PRIVATE TurboRaft::Core TurboRaft::WalStorage)
