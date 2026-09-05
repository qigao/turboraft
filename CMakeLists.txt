cmake_minimum_required(VERSION 3.23)

if(POLICY CMP0144)
  cmake_policy(SET CMP0144 OLD)
endif()
if(POLICY CMP0167)
  cmake_policy(SET CMP0167 OLD)
endif()

project(
  TurboRaft
  VERSION 0.1.0
  DESCRIPTION "Raft consensus library for the Salts ecosystem"
  HOMEPAGE_URL "https://github.com/qigao/turboraft"
  LANGUAGES C)

include(CMakePackageConfigHelpers)
include(GNUInstallDirs)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_COLOR_DIAGNOSTICS ON)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")

get_filename_component(CMAKE_INSTALL_PREFIX "${CMAKE_INSTALL_PREFIX}" ABSOLUTE)

if(MSVC)
  add_compile_options(/experimental:c11atomics)
endif()

set_property(GLOBAL PROPERTY USE_FOLDERS ON)
set_property(GLOBAL PROPERTY PREDEFINED_TARGETS_FOLDER "cmake")

include(CMakeOptions.cmake)
include(cmake/Sanitizers.cmake)
include(cmake/CmakeUtils.cmake)
add_subdirectory(tools)
include(cmake/FindTools.cmake)

if(NOT RE2C_EXECUTABLE)
  message(FATAL_ERROR
          "TurboRaft text syntax requires a runnable re2c executable")
endif()

if(NOT EXISTS "${LEMPAR}")
  message(FATAL_ERROR "TurboRaft text syntax requires the Lemon parser template")
endif()

if(ENABLE_MSVC_ANALYZE)
  if(NOT MSVC)
    message(FATAL_ERROR "ENABLE_MSVC_ANALYZE requires MSVC")
  endif()
  add_compile_options(/W4 /WX /external:anglebrackets /analyze
                      /analyze:external-)
endif()

include(CTest)

foreach(_turboraft_required_root IN ITEMS SALTS_ROOT SALTS_UTILS_ROOT
                                           FLOWMQ_ROOT)
  if(NOT DEFINED ENV{${_turboraft_required_root}} OR
     "$ENV{${_turboraft_required_root}}" STREQUAL "")
    message(FATAL_ERROR
            "Required dependency root is not defined: ${_turboraft_required_root}")
  endif()
  if(NOT IS_DIRECTORY "$ENV{${_turboraft_required_root}}")
    message(FATAL_ERROR
            "${_turboraft_required_root} is not a directory: $ENV{${_turboraft_required_root}}")
  endif()
endforeach()

find_package(Salts CONFIG REQUIRED
             PATHS "$ENV{SALTS_ROOT}" NO_DEFAULT_PATH)
find_package(SaltsUtils CONFIG REQUIRED
             PATHS "$ENV{SALTS_UTILS_ROOT}" NO_DEFAULT_PATH)
find_package(FlowMQ CONFIG REQUIRED
             PATHS "$ENV{FLOWMQ_ROOT}" NO_DEFAULT_PATH)
find_package(OpenSSL REQUIRED)
find_package(xxHash CONFIG REQUIRED)

foreach(_turboraft_required_target IN ITEMS Salts::CNet Salts::CHTTP
                                             Salts::CRPC FlowMQ::FlowMQ)
  if(NOT TARGET ${_turboraft_required_target})
    message(FATAL_ERROR
            "Required target is unavailable: ${_turboraft_required_target}")
  endif()
endforeach()

if(CMAKE_CROSSCOMPILING)
  set(_turboraft_tbe_root_name SALTS_UTILS_HOST_ROOT)
else()
  set(_turboraft_tbe_root_name SALTS_UTILS_ROOT)
endif()
if(NOT DEFINED ENV{${_turboraft_tbe_root_name}} OR
   "$ENV{${_turboraft_tbe_root_name}}" STREQUAL "" OR
   NOT IS_DIRECTORY "$ENV{${_turboraft_tbe_root_name}}")
  message(FATAL_ERROR
          "${_turboraft_tbe_root_name} must name the SaltsUtils host install prefix")
endif()
file(REAL_PATH "$ENV{${_turboraft_tbe_root_name}}" _turboraft_tbe_root_real)
unset(TURBORAFT_TBE_COMPILER CACHE)
unset(TURBORAFT_TBE_COMPILER)
find_program(TURBORAFT_TBE_COMPILER NAMES tbe_compiler tbe_compiler.exe
             PATHS "${_turboraft_tbe_root_real}/bin" NO_DEFAULT_PATH
             DOC "SaltsUtils TBE schema compiler")
if(NOT TURBORAFT_TBE_COMPILER)
  message(
    FATAL_ERROR "tbe_compiler is required to generate the TurboRaft wire codec")
endif()
file(REAL_PATH "${TURBORAFT_TBE_COMPILER}" _turboraft_tbe_compiler_real)
cmake_path(IS_PREFIX _turboraft_tbe_root_real
           "${_turboraft_tbe_compiler_real}" NORMALIZE
           _turboraft_tbe_compiler_in_root)
if(NOT _turboraft_tbe_compiler_in_root)
  message(FATAL_ERROR
          "tbe_compiler resolved outside ${_turboraft_tbe_root_name}: ${_turboraft_tbe_compiler_real}")
endif()
set(TURBORAFT_TBE_COMPILER "${_turboraft_tbe_compiler_real}")

set(TURBORAFT_WIRE_SCHEMA
    "${CMAKE_CURRENT_SOURCE_DIR}/schema/turboraft_wire.schema")
set(TURBORAFT_WIRE_GENERATED_DIR
    "${CMAKE_CURRENT_SOURCE_DIR}/src/wire/generated")
set(TURBORAFT_WIRE_GENERATED_HEADER
    "${TURBORAFT_WIRE_GENERATED_DIR}/turboraft_wire_tbe.h")
set(TURBORAFT_WIRE_GENERATED_SOURCE
    "${TURBORAFT_WIRE_GENERATED_DIR}/turboraft_wire_tbe.c")
add_custom_command(
  OUTPUT "${TURBORAFT_WIRE_GENERATED_HEADER}"
         "${TURBORAFT_WIRE_GENERATED_SOURCE}"
  COMMAND
    "${TURBORAFT_TBE_COMPILER}" "${TURBORAFT_WIRE_SCHEMA}" --lang c --output
    "${TURBORAFT_WIRE_GENERATED_HEADER}" --source-output
    "${TURBORAFT_WIRE_GENERATED_SOURCE}"
  DEPENDS "${TURBORAFT_WIRE_SCHEMA}" "${TURBORAFT_TBE_COMPILER}"
  COMMENT "Generating TurboRaft wire codec from schema"
  VERBATIM)

set(TURBORAFT_WIRE_V3_SCHEMA
    "${CMAKE_CURRENT_SOURCE_DIR}/schema/turboraft_wire_v3.schema")
set(TURBORAFT_WIRE_V3_GENERATED_HEADER
    "${TURBORAFT_WIRE_GENERATED_DIR}/turboraft_wire_v3_tbe.h")
set(TURBORAFT_WIRE_V3_GENERATED_SOURCE
    "${TURBORAFT_WIRE_GENERATED_DIR}/turboraft_wire_v3_tbe.c")
add_custom_command(
  OUTPUT "${TURBORAFT_WIRE_V3_GENERATED_HEADER}"
         "${TURBORAFT_WIRE_V3_GENERATED_SOURCE}"
  COMMAND
    "${TURBORAFT_TBE_COMPILER}" "${TURBORAFT_WIRE_V3_SCHEMA}" --lang c --output
    "${TURBORAFT_WIRE_V3_GENERATED_HEADER}" --source-output
    "${TURBORAFT_WIRE_V3_GENERATED_SOURCE}"
  DEPENDS "${TURBORAFT_WIRE_V3_SCHEMA}" "${TURBORAFT_TBE_COMPILER}"
  COMMENT "Generating TurboRaft v3 batch wire codec from schema"
  VERBATIM)

add_library(
  turboraft_core STATIC
  src/core/raft_core.c
  src/core/raft_configuration.c
  src/core/raft_membership.c
  src/core/raft_membership_transition.c
  src/core/raft_peer_set.c
  src/core/raft_log.c
  src/runtime/raft_runtime.c
  src/wire/raft_wire_codec.c
  "${TURBORAFT_WIRE_GENERATED_SOURCE}"
  "${TURBORAFT_WIRE_V3_GENERATED_SOURCE}")
add_library(TurboRaft::Core ALIAS turboraft_core)
set_target_properties(turboraft_core PROPERTIES EXPORT_NAME Core)

target_include_directories(
  turboraft_core
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
  PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src/core>
          $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src/wire/generated>)

target_link_libraries(
  turboraft_core
  PUBLIC Salts::Core
  PRIVATE Salts::CSTL Salts::DataBind)

add_library(turboraft_service STATIC src/service/raft_service.c)
add_library(TurboRaft::Service ALIAS turboraft_service)
set_target_properties(turboraft_service PROPERTIES EXPORT_NAME Service)
target_include_directories(
  turboraft_service
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
target_link_libraries(turboraft_service PUBLIC TurboRaft::Core)

add_library(turboraft_snapshot_receiver STATIC
            src/snapshot/raft_snapshot_receiver.c)
add_library(TurboRaft::SnapshotReceiver ALIAS turboraft_snapshot_receiver)
set_target_properties(turboraft_snapshot_receiver PROPERTIES EXPORT_NAME
                                                             SnapshotReceiver)
target_include_directories(
  turboraft_snapshot_receiver
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
target_link_libraries(
  turboraft_snapshot_receiver
  PUBLIC TurboRaft::Core
  PRIVATE OpenSSL::Crypto)

add_library(turboraft_snapshot_sender STATIC
            src/snapshot/raft_snapshot_sender.c)
add_library(TurboRaft::SnapshotSender ALIAS turboraft_snapshot_sender)
set_target_properties(turboraft_snapshot_sender PROPERTIES EXPORT_NAME
                                                           SnapshotSender)
target_include_directories(
  turboraft_snapshot_sender
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
target_link_libraries(
  turboraft_snapshot_sender
  PUBLIC TurboRaft::Core
  PRIVATE OpenSSL::Crypto)

add_library(turboraft_data_stream STATIC
            src/stream/raft_data_stream.c)
cmake_config_target(turboraft_data_stream
                    NO_INSTALL
                    ALIAS TurboRaft::DataStream
                    EXPORT_NAME DataStream
                    FOLDER "libraries/stream")
target_include_directories(
  turboraft_data_stream
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
target_link_libraries(
  turboraft_data_stream
  PUBLIC TurboRaft::Core
  PRIVATE OpenSSL::Crypto)

add_library(turboraft_wal_storage STATIC
            src/storage/raft_wal_storage.c)
cmake_config_target(turboraft_wal_storage
                    NO_INSTALL
                    ALIAS TurboRaft::WalStorage
                    EXPORT_NAME WalStorage
                    FOLDER "libraries/storage")
target_include_directories(
  turboraft_wal_storage
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
target_link_libraries(turboraft_wal_storage
                      PUBLIC TurboRaft::Core
                      PRIVATE Salts::Core xxHash::xxhash)

if(TURBORAFT_ENABLE_TURBODB_REDIS_STATE_MACHINE)
  if(NOT DEFINED ENV{TURBODB_ROOT} OR
     "$ENV{TURBODB_ROOT}" STREQUAL "" OR
     NOT IS_DIRECTORY "$ENV{TURBODB_ROOT}")
    message(FATAL_ERROR
            "TURBODB_ROOT must name the TurboDB install prefix when Redis state machine support is enabled")
  endif()
  find_package(TurboDB CONFIG REQUIRED
               PATHS "$ENV{TURBODB_ROOT}" NO_DEFAULT_PATH)
  add_library(turboraft_turbodb_redis_state_machine STATIC
              src/integrations/turbodb_redis_state_machine.c)
  cmake_config_target(turboraft_turbodb_redis_state_machine
                      NO_INSTALL
                      ALIAS TurboRaft::TurboDbRedisStateMachine
                      EXPORT_NAME TurboDbRedisStateMachine
                      FOLDER "libraries/integrations")
  target_include_directories(
    turboraft_turbodb_redis_state_machine
    PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
           $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
  target_link_libraries(turboraft_turbodb_redis_state_machine
                        PUBLIC TurboRaft::Core TurboDB::Redis)
endif()

add_library(turboraft_snapshot_coordinator STATIC
            src/service/raft_snapshot_coordinator.c)
target_include_directories(turboraft_snapshot_coordinator
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src/service)
target_link_libraries(turboraft_snapshot_coordinator
                      PUBLIC TurboRaft::SnapshotSender)

add_library(turboraft_snapshot_installer STATIC
            src/service/raft_snapshot_installer.c)
target_include_directories(turboraft_snapshot_installer
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src/service)
target_link_libraries(turboraft_snapshot_installer
                      PUBLIC TurboRaft::WalStorage)

add_library(turboraft_service_wal_reload STATIC
            src/service/raft_service_wal_reload.c)
target_include_directories(turboraft_service_wal_reload
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src/service)
target_link_libraries(turboraft_service_wal_reload
                      PUBLIC TurboRaft::Service TurboRaft::WalStorage)

find_package(Threads REQUIRED)

if(TARGET Salts::CNet)
  add_library(turboraft_cnet STATIC
              src/transport/raft_cnet_peer.c
              src/transport/raft_transport.c
              src/transport/raft_peer_handshake.c)
  add_library(TurboRaft::CNet ALIAS turboraft_cnet)
  set_target_properties(turboraft_cnet PROPERTIES EXPORT_NAME CNet)
  target_include_directories(
    turboraft_cnet
    PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
           $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
  target_link_libraries(
    turboraft_cnet
    PUBLIC TurboRaft::Core Salts::CNet
    PRIVATE Salts::CSTL)

  add_library(turboraft_snapshot_peer STATIC
              src/service/raft_snapshot_peer.c)
  target_include_directories(turboraft_snapshot_peer
                             PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src/service)
  target_link_libraries(turboraft_snapshot_peer
                        PUBLIC TurboRaft::CNet
                               turboraft_snapshot_coordinator)

  add_library(turboraft_snapshot_manager STATIC
              src/service/raft_snapshot_manager.c
              src/service/raft_snapshot_peer.c
              src/service/raft_snapshot_coordinator.c)
  add_library(TurboRaft::SnapshotManager ALIAS turboraft_snapshot_manager)
  set_target_properties(turboraft_snapshot_manager
                        PROPERTIES EXPORT_NAME SnapshotManager)
  target_include_directories(
    turboraft_snapshot_manager
    PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
           $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src/service)
  target_link_libraries(turboraft_snapshot_manager
                        PUBLIC TurboRaft::CNet TurboRaft::SnapshotSender)

else()
  message(FATAL_ERROR "Required target is unavailable: Salts::CNet")
endif()

if(TARGET Salts::CNet AND TARGET FlowMQ::FlowMQ)
  add_library(turboraft_flowmq STATIC
              src/transport/raft_flowmq_peer_service.c)
  add_library(TurboRaft::FlowMQ ALIAS turboraft_flowmq)
  set_target_properties(turboraft_flowmq PROPERTIES EXPORT_NAME FlowMQ)
  target_include_directories(
    turboraft_flowmq
    PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
           $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
  target_link_libraries(
    turboraft_flowmq
    PUBLIC TurboRaft::CNet FlowMQ::FlowMQ
    PRIVATE Salts::Core Salts::CSTL)
else()
  message(FATAL_ERROR "Required target is unavailable: FlowMQ::FlowMQ")
endif()

if(TARGET Salts::CNet AND TARGET Salts::CHTTP AND TARGET Salts::CRPC)
  add_library(
  turboraft_control_plane STATIC
  src/control/raft_control_audit.c src/control/raft_control_plane.c)
add_library(TurboRaft::ControlPlane ALIAS turboraft_control_plane)
set_target_properties(turboraft_control_plane PROPERTIES EXPORT_NAME
                                                         ControlPlane)
target_include_directories(
  turboraft_control_plane
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
target_link_libraries(
  turboraft_control_plane
  PUBLIC TurboRaft::Service Salts::CHTTP Salts::CRPC)

if(BUILD_TESTS)
  add_executable(turboraft_control_plane_tests
                 tests/control/test_raft_control_plane.c)
  target_link_libraries(
    turboraft_control_plane_tests
    PRIVATE TurboRaft::ControlPlane Salts::CHTTP Salts::CRPC Salts::TinyTest)
  add_test(NAME turboraft.control_plane COMMAND turboraft_control_plane_tests)
  cmake_config_target(turboraft_control_plane_tests
                      NO_INSTALL
                      FOLDER "tests/control")

  endif()
else()
  message(FATAL_ERROR "Required Salts CNet/CHTTP/CRPC targets are unavailable")
endif()

set(TURBORAFT_TEXT_SYNTAX_DIR "${CMAKE_CURRENT_SOURCE_DIR}/src/text_syntax")

cmake_add_grammar(
  turboraft_text_query
  LEXER_RE
  "${TURBORAFT_TEXT_SYNTAX_DIR}/query_lexer.re"
  GRAMMAR_Y
  "${TURBORAFT_TEXT_SYNTAX_DIR}/query_grammar.y"
  LEXER_OUTPUT
  turboraft_text_query_lexer_gen.c
  LEXER_DEPENDS_ON_GRAMMAR
  FOLDER
  "TurboRaft/text-syntax")
cmake_add_grammar(
  turboraft_text_protocol_debug
  LEXER_RE
  "${TURBORAFT_TEXT_SYNTAX_DIR}/protocol_debug_lexer.re"
  GRAMMAR_Y
  "${TURBORAFT_TEXT_SYNTAX_DIR}/protocol_debug_grammar.y"
  LEXER_OUTPUT
  turboraft_text_protocol_debug_lexer_gen.c
  LEXER_DEPENDS_ON_GRAMMAR
  FOLDER
  "TurboRaft/text-syntax")
cmake_add_grammar(
  turboraft_text_replay
  LEXER_RE
  "${TURBORAFT_TEXT_SYNTAX_DIR}/replay_lexer.re"
  GRAMMAR_Y
  "${TURBORAFT_TEXT_SYNTAX_DIR}/replay_grammar.y"
  LEXER_OUTPUT
  turboraft_text_replay_lexer_gen.c
  LEXER_DEPENDS_ON_GRAMMAR
  FOLDER
  "TurboRaft/text-syntax")

add_library(
  turboraft_text_syntax STATIC
  src/text_syntax/text_syntax.c
  src/text_syntax/text_query_executor.c
  src/text_syntax/text_replay_executor.c
  "${turboraft_text_query_LEXER_GEN}"
  "${turboraft_text_query_GRAMMAR_GEN}"
  "${turboraft_text_protocol_debug_LEXER_GEN}"
  "${turboraft_text_protocol_debug_GRAMMAR_GEN}"
  "${turboraft_text_replay_LEXER_GEN}"
  "${turboraft_text_replay_GRAMMAR_GEN}"
  "${turboraft_text_query_GRAMMAR_H}"
  "${turboraft_text_protocol_debug_GRAMMAR_H}"
  "${turboraft_text_replay_GRAMMAR_H}")
if(MSVC)
  set_source_files_properties(
    "${turboraft_text_query_GRAMMAR_GEN}"
    "${turboraft_text_protocol_debug_GRAMMAR_GEN}"
    "${turboraft_text_replay_GRAMMAR_GEN}" PROPERTIES COMPILE_OPTIONS "/wd4189")
endif()
add_library(TurboRaft::TextSyntax ALIAS turboraft_text_syntax)
set_target_properties(turboraft_text_syntax PROPERTIES EXPORT_NAME TextSyntax)
target_include_directories(
  turboraft_text_syntax
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
  PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/src/text_syntax"
          "${CMAKE_CURRENT_BINARY_DIR}")
target_link_libraries(turboraft_text_syntax PUBLIC Salts::Core)

add_library(turboraft_text_query_service STATIC
            src/text_syntax/text_query_service.c)
add_library(TurboRaft::TextQueryService ALIAS turboraft_text_query_service)
set_target_properties(turboraft_text_query_service PROPERTIES EXPORT_NAME
                                                               TextQueryService)
target_include_directories(
  turboraft_text_query_service
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
target_link_libraries(turboraft_text_query_service
                      PUBLIC TurboRaft::TextSyntax TurboRaft::Service)

if(TARGET Salts::CHTTP AND TARGET Salts::CRPC)
  add_executable(turboraft_console tools/turboraft_console.c)
  target_link_libraries(turboraft_console
                        PRIVATE TurboRaft::TextSyntax Salts::CHTTP Salts::CRPC)
  cmake_config_target(turboraft_console
                      NO_INSTALL
                      FOLDER "TurboRaft/tools")
else()
  message(FATAL_ERROR "Required Salts CHTTP/CRPC targets are unavailable")
endif()

add_library(turboraft_text_protocol_debug STATIC
            src/text_syntax/text_protocol_executor.c)
add_library(TurboRaft::TextProtocolDebug ALIAS turboraft_text_protocol_debug)
set_target_properties(turboraft_text_protocol_debug PROPERTIES EXPORT_NAME
                                                               TextProtocolDebug)
target_include_directories(
  turboraft_text_protocol_debug
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
target_link_libraries(turboraft_text_protocol_debug
                      PUBLIC TurboRaft::TextSyntax TurboRaft::Core)

add_library(turboraft_replay_driver STATIC
            src/text_syntax/text_replay_core_driver.c)
add_library(TurboRaft::ReplayDriver ALIAS turboraft_replay_driver)
set_target_properties(turboraft_replay_driver PROPERTIES EXPORT_NAME
                                                                ReplayDriver)
target_include_directories(
  turboraft_replay_driver
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
target_link_libraries(turboraft_replay_driver PUBLIC TurboRaft::Core)

add_executable(turboraft_repl tools/turboraft_repl.c)
target_link_libraries(
  turboraft_repl
  PRIVATE TurboRaft::TextSyntax TurboRaft::ReplayDriver)
cmake_config_target(turboraft_repl
                    NO_INSTALL
                    FOLDER "TurboRaft/tools")

add_subdirectory(tests)

set(TURBORAFT_INSTALL_TARGETS
    turboraft_core turboraft_service turboraft_snapshot_receiver
    turboraft_data_stream turboraft_wal_storage
    turboraft_snapshot_sender)
if(TARGET turboraft_cnet)
  list(APPEND TURBORAFT_INSTALL_TARGETS turboraft_cnet)
endif()
if(TARGET turboraft_snapshot_manager)
  list(APPEND TURBORAFT_INSTALL_TARGETS turboraft_snapshot_manager)
endif()
if(TARGET turboraft_flowmq)
  list(APPEND TURBORAFT_INSTALL_TARGETS turboraft_flowmq)
endif()
if(TARGET turboraft_control_plane)
  list(APPEND TURBORAFT_INSTALL_TARGETS turboraft_control_plane)
endif()
list(APPEND TURBORAFT_INSTALL_TARGETS turboraft_text_syntax
     turboraft_text_query_service turboraft_text_protocol_debug
     turboraft_replay_driver)

set(TURBORAFT_HAVE_CNET OFF)
if(TARGET turboraft_cnet)
  set(TURBORAFT_HAVE_CNET ON)
endif()
set(TURBORAFT_HAVE_CONTROL_PLANE OFF)
if(TARGET turboraft_control_plane)
  set(TURBORAFT_HAVE_CONTROL_PLANE ON)
endif()
set(TURBORAFT_HAVE_FLOWMQ OFF)
if(TARGET turboraft_flowmq)
  set(TURBORAFT_HAVE_FLOWMQ ON)
endif()

configure_package_config_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/cmake/TurboRaftConfig.cmake.in"
  "${CMAKE_CURRENT_BINARY_DIR}/TurboRaftConfig.cmake"
  INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/TurboRaft")
write_basic_package_version_file(
  "${CMAKE_CURRENT_BINARY_DIR}/TurboRaftConfigVersion.cmake"
  VERSION "${PROJECT_VERSION}"
  COMPATIBILITY SameMajorVersion)

install(
  TARGETS ${TURBORAFT_INSTALL_TARGETS}
  EXPORT TurboRaftTargets
  ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
  LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
  RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}")
install(DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/include/turboraft"
        DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")
install(
  EXPORT TurboRaftTargets
  FILE TurboRaftTargets.cmake
  NAMESPACE TurboRaft::
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/TurboRaft")
install(FILES "${CMAKE_CURRENT_BINARY_DIR}/TurboRaftConfig.cmake"
              "${CMAKE_CURRENT_BINARY_DIR}/TurboRaftConfigVersion.cmake"
        DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/TurboRaft")

if(BUILD_BENCHMARKS)
  add_subdirectory(benchmarks)
endif()

if(BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()

cmake_config_directory_targets(FOLDER "libraries")
