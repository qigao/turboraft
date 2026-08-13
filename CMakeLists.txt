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
  DESCRIPTION "Raft consensus library for the TurboNet ecosystem"
  HOMEPAGE_URL "https://github.com/qigao/turbonet"
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
include(cmake/CMakeUtils.cmake)
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

find_package(TurboUtils CONFIG REQUIRED PATHS "${TURBO_UTILS_ROOT}")
find_package(TurboParser CONFIG REQUIRED PATHS "${TURBOPARSER_ROOT}")
find_package(OpenSSL REQUIRED)

find_program(
  TURBORAFT_TBE_COMPILER
  NAMES tbe_compiler
  HINTS "${TURBOPARSER_ROOT}/bin"
        "${CMAKE_CURRENT_SOURCE_DIR}/../turbo-parser/build/Msvc-Release/bin"
        "${CMAKE_CURRENT_SOURCE_DIR}/../turbo-parser/build/Msvc/bin"
  DOC "TurboParser TBE schema compiler")
if(NOT TURBORAFT_TBE_COMPILER)
  message(
    FATAL_ERROR "tbe_compiler is required to generate the TurboRaft wire codec")
endif()

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
  PUBLIC TurboUtils::Core
  PRIVATE TurboParser::DataBind)

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

add_library(turboraft_snapshot_coordinator STATIC
            src/service/raft_snapshot_coordinator.c)
target_include_directories(turboraft_snapshot_coordinator
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src/service)
target_link_libraries(turboraft_snapshot_coordinator
                      PUBLIC TurboRaft::SnapshotSender)

if(TURBORAFT_BUILD_SQLITE_STORAGE)
  find_package(unofficial-sqlite3 CONFIG REQUIRED)
  add_library(turboraft_sqlite_storage STATIC src/storage/raft_sqlite_storage.c)
  add_library(TurboRaft::SQLiteStorage ALIAS turboraft_sqlite_storage)
  set_target_properties(turboraft_sqlite_storage PROPERTIES EXPORT_NAME
                                                            SQLiteStorage)
  target_include_directories(
    turboraft_sqlite_storage
    PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
           $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
  target_link_libraries(
    turboraft_sqlite_storage
    PUBLIC TurboRaft::Core
    PRIVATE unofficial::sqlite3::sqlite3)

  add_library(turboraft_snapshot_installer STATIC
              src/service/raft_snapshot_installer.c)
  target_include_directories(turboraft_snapshot_installer
                             PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src/service)
  target_link_libraries(turboraft_snapshot_installer
                        PUBLIC TurboRaft::SQLiteStorage)

  add_library(turboraft_service_sqlite_reload STATIC
              src/service/raft_service_sqlite_reload.c)
  target_include_directories(turboraft_service_sqlite_reload
                             PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src/service)
  target_link_libraries(turboraft_service_sqlite_reload
                        PUBLIC TurboRaft::Service TurboRaft::SQLiteStorage)
endif()

find_package(Threads REQUIRED)
find_package(TurboNet CONFIG QUIET PATHS "${TURBO_NET_ROOT}")
find_package(TurboHttp CONFIG QUIET PATHS "${TURBO_HTTP_ROOT}")
find_package(FlowMQ CONFIG QUIET PATHS "${FLOWMQ_ROOT}")

if(TurboNet_FOUND)
  add_library(
    turboraft_coronet STATIC
    src/transport/raft_coronet_transport.c
    src/transport/raft_coronet_handshake.c
    src/transport/raft_coronet_connector.c
    src/transport/raft_coronet_dial_scheduler.c
    src/transport/raft_coronet_inbound_service.c
    src/transport/raft_coronet_identity_registry.c
    src/transport/raft_coronet_peer_service.c
    src/transport/raft_peer_handshake.c)
  add_library(TurboRaft::CoroNet ALIAS turboraft_coronet)
  set_target_properties(turboraft_coronet PROPERTIES EXPORT_NAME CoroNet)
  target_include_directories(
    turboraft_coronet
    PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
           $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
  target_link_libraries(
    turboraft_coronet
    PUBLIC TurboRaft::Core TurboNet::CoroNet
    PRIVATE TurboRaft::SnapshotReceiver)

  add_library(turboraft_snapshot_peer STATIC src/service/raft_snapshot_peer.c)
  target_include_directories(turboraft_snapshot_peer
                             PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src/service)
  target_link_libraries(
    turboraft_snapshot_peer PUBLIC turboraft_snapshot_coordinator
                                   TurboRaft::CoroNet)

  add_library(
    turboraft_snapshot_manager STATIC
    src/service/raft_snapshot_manager.c src/service/raft_snapshot_peer.c
    src/service/raft_snapshot_coordinator.c)
  add_library(TurboRaft::SnapshotManager ALIAS turboraft_snapshot_manager)
  set_target_properties(turboraft_snapshot_manager PROPERTIES EXPORT_NAME
                                                              SnapshotManager)
  target_include_directories(
    turboraft_snapshot_manager
    PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
           $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src/service)
  target_link_libraries(
    turboraft_snapshot_manager
    PUBLIC TurboRaft::CoroNet TurboRaft::SnapshotSender
    PRIVATE TurboRaft::SnapshotReceiver)
else()
  message(
    STATUS
    "TurboNet not found; CoroNet transport, snapshot manager, and service "
    "owner adapters are disabled")
endif()

if(TurboNet_FOUND AND TARGET FlowMQ::FlowMQ)
  add_library(turboraft_flowmq STATIC
              src/transport/raft_flowmq_peer_service.c)
  add_library(TurboRaft::FlowMQ ALIAS turboraft_flowmq)
  set_target_properties(turboraft_flowmq PROPERTIES EXPORT_NAME FlowMQ)
  target_include_directories(
    turboraft_flowmq
    PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
           $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
  target_link_libraries(turboraft_flowmq
                        PUBLIC TurboRaft::CoroNet FlowMQ::FlowMQ)
else()
  message(STATUS "FlowMQ::FlowMQ not found; FlowMQ peer transport is disabled")
endif()

if(TurboNet_FOUND AND TurboHttp_FOUND)
  add_library(
  turboraft_control_plane STATIC
  src/control/raft_control_audit.c src/control/raft_control_owner_bridge.c
  src/control/raft_control_operation.c src/control/raft_control_plane.c)
add_library(TurboRaft::ControlPlane ALIAS turboraft_control_plane)
set_target_properties(turboraft_control_plane PROPERTIES EXPORT_NAME
                                                         ControlPlane)
target_include_directories(
  turboraft_control_plane
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
target_link_libraries(
  turboraft_control_plane
  PUBLIC TurboRaft::Service
  PRIVATE TurboHttp::Iris TurboParser::Parser)

if(BUILD_TESTS)
  add_executable(turboraft_control_plane_tests
                 tests/control/test_raft_control_plane.c)
  target_link_libraries(
    turboraft_control_plane_tests
    PRIVATE TurboRaft::ControlPlane TurboHttp::Iris TurboUtils::TinyTest)
  add_custom_command(
    TARGET turboraft_control_plane_tests
    POST_BUILD
    COMMAND
      ${CMAKE_COMMAND} -E copy_if_different $<TARGET_FILE:TurboHttp::Iris>
      $<TARGET_FILE_DIR:turboraft_control_plane_tests>)
  add_test(NAME turboraft.control_plane COMMAND turboraft_control_plane_tests)

  add_executable(turboraft_control_plane_http_tests
                 tests/control/test_raft_control_plane_http.c)
  target_link_libraries(
    turboraft_control_plane_http_tests
    PRIVATE TurboRaft::ControlPlane TurboHttp::Iris TurboHttp::TurboHttp
            TurboUtils::TinyTest)
  add_custom_command(
    TARGET turboraft_control_plane_http_tests
    POST_BUILD
    COMMAND
      ${CMAKE_COMMAND} -E copy_if_different $<TARGET_FILE:TurboHttp::Iris>
      $<TARGET_FILE:TurboHttp::TurboHttp>
      $<TARGET_FILE_DIR:turboraft_control_plane_http_tests>)
  add_test(NAME turboraft.control_plane_http
           COMMAND turboraft_control_plane_http_tests)
  endif()
else()
  message(
    STATUS
    "TurboNet/TurboHttp not found together; the TurboRaft control plane is "
    "disabled")
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
target_link_libraries(turboraft_text_syntax PUBLIC TurboUtils::Core)

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

if(TurboHttp_FOUND)
  add_executable(turboraft_console tools/turboraft_console.c)
  target_link_libraries(turboraft_console
                        PRIVATE TurboRaft::TextSyntax TurboHttp::TurboHttp)
  cmake_config_target(turboraft_console
                      NO_INSTALL
                      FOLDER "TurboRaft/tools")
else()
  message(STATUS "TurboHttp not found; turboraft_console is disabled")
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
  PRIVATE TurboRaft::TextSyntax TurboRaft::ReplayDriver TurboParser::Parser)
cmake_config_target(turboraft_repl
                    NO_INSTALL
                    FOLDER "TurboRaft/tools")

add_subdirectory(tests)

if(TurboNet_FOUND)
  add_library(turboraft_service_owner STATIC src/service/raft_service_owner.c)
  add_library(TurboRaft::ServiceOwner ALIAS turboraft_service_owner)
  set_target_properties(turboraft_service_owner PROPERTIES EXPORT_NAME
                                                           ServiceOwner)
  target_include_directories(
    turboraft_service_owner
    PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
           $<INSTALL_INTERFACE:include>)
  target_link_libraries(turboraft_service_owner PUBLIC TurboRaft::Service
                                                       TurboNet::CoroNet)
  if(BUILD_TESTS AND TARGET turboraft_service_tests)
    target_link_libraries(turboraft_service_tests
                          PRIVATE TurboRaft::ServiceOwner)
    target_compile_definitions(turboraft_service_tests
                               PRIVATE TURBORAFT_HAS_SERVICE_OWNER=1)
  endif()
endif()
if(TurboNet_FOUND AND TurboHttp_FOUND)
  target_link_libraries(turboraft_control_plane
                        PUBLIC TurboRaft::ServiceOwner)
  target_compile_definitions(turboraft_control_plane
                             PRIVATE TURBORAFT_HAS_SERVICE_OWNER=1)
  if(BUILD_TESTS)
    add_executable(turboraft_control_owner_bridge_tests
                   tests/control/test_raft_control_owner_bridge.c)
    target_link_libraries(
      turboraft_control_owner_bridge_tests PRIVATE TurboRaft::ControlPlane
                                                     TurboUtils::TinyTest)
    target_compile_definitions(turboraft_control_owner_bridge_tests
                               PRIVATE TURBORAFT_HAS_SERVICE_OWNER=1)
    add_test(NAME turboraft.control_owner_bridge
             COMMAND turboraft_control_owner_bridge_tests)
  endif()
endif()

set(TURBORAFT_INSTALL_TARGETS
    turboraft_core turboraft_service turboraft_snapshot_receiver
    turboraft_snapshot_sender)
if(TARGET turboraft_sqlite_storage)
  list(APPEND TURBORAFT_INSTALL_TARGETS turboraft_sqlite_storage)
endif()
if(TARGET turboraft_coronet)
  list(APPEND TURBORAFT_INSTALL_TARGETS turboraft_coronet
       turboraft_snapshot_manager turboraft_service_owner)
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

set(TURBORAFT_HAVE_CORONET OFF)
if(TARGET turboraft_coronet)
  set(TURBORAFT_HAVE_CORONET ON)
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
