add_library(
        turboraft_core STATIC
        src/core/raft_core.c src/core/raft_configuration.c src/core/raft_membership.c
        src/core/raft_membership_transition.c src/core/raft_peer_set.c src/core/raft_log.c
        src/runtime/raft_runtime.c src/runtime/raft_apply_runtime.c src/wire/raft_wire_codec.c
)
add_library(TurboRaft::Core ALIAS turboraft_core)
set_target_properties(turboraft_core PROPERTIES EXPORT_NAME Core)

target_include_directories(
        turboraft_core
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
               $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
        PRIVATE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src/core>
)

target_link_libraries(
        turboraft_core
        PUBLIC Salts::Core
        PRIVATE CMeta::CSTL Salts::DataBind $<BUILD_INTERFACE:turboraft_wire_schema>
)

add_library(turboraft_cflow_state_machine STATIC src/integrations/raft_cflow_state_machine.c)
add_library(TurboRaft::CFlowStateMachine ALIAS turboraft_cflow_state_machine)
set_target_properties(turboraft_cflow_state_machine PROPERTIES EXPORT_NAME CFlowStateMachine)
target_include_directories(
        turboraft_cflow_state_machine PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                                             $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_link_libraries(turboraft_cflow_state_machine PUBLIC TurboRaft::Core CMeta::CFlow)

add_library(turboraft_service STATIC src/service/raft_service.c)
add_library(TurboRaft::Service ALIAS turboraft_service)
set_target_properties(turboraft_service PROPERTIES EXPORT_NAME Service)
target_include_directories(
        turboraft_service PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                                 $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_link_libraries(turboraft_service PUBLIC TurboRaft::Core)

add_library(turboraft_multicore STATIC src/runtime/raft_multicore.c)
add_library(TurboRaft::Multicore ALIAS turboraft_multicore)
set_target_properties(
        turboraft_multicore PROPERTIES EXPORT_NAME Multicore FOLDER "libraries/runtime"
)
target_include_directories(
        turboraft_multicore PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                                   $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_link_libraries(
        turboraft_multicore
        PUBLIC TurboRaft::Service
        PRIVATE Salts::Core
)

add_library(turboraft_snapshot_receiver STATIC src/snapshot/raft_snapshot_receiver.c)
add_library(TurboRaft::SnapshotReceiver ALIAS turboraft_snapshot_receiver)
set_target_properties(turboraft_snapshot_receiver PROPERTIES EXPORT_NAME SnapshotReceiver)
target_include_directories(
        turboraft_snapshot_receiver PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                                           $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_link_libraries(
        turboraft_snapshot_receiver
        PUBLIC TurboRaft::Core
        PRIVATE Salts::Core Salts::Crypto
)
add_library(turboraft_snapshot_sender STATIC src/snapshot/raft_snapshot_sender.c)
add_library(TurboRaft::SnapshotSender ALIAS turboraft_snapshot_sender)
set_target_properties(turboraft_snapshot_sender PROPERTIES EXPORT_NAME SnapshotSender)
target_include_directories(
        turboraft_snapshot_sender PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                                         $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_link_libraries(
        turboraft_snapshot_sender
        PUBLIC TurboRaft::Core
        PRIVATE Salts::Core Salts::Crypto
)
add_library(turboraft_data_stream STATIC src/stream/raft_data_stream.c)
cmake_config_target(
        turboraft_data_stream ALIAS TurboRaft::DataStream EXPORT_NAME DataStream FOLDER
        "libraries/stream"
)
target_include_directories(
        turboraft_data_stream PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                                     $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_link_libraries(
        turboraft_data_stream
        PUBLIC TurboRaft::Core
        PRIVATE Salts::Core Salts::Crypto
)
add_library(turboraft_wal_storage STATIC src/storage/raft_wal_storage.c)
cmake_config_target(
        turboraft_wal_storage ALIAS TurboRaft::WalStorage EXPORT_NAME WalStorage FOLDER
        "libraries/storage"
)
target_include_directories(
        turboraft_wal_storage PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                                     $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_link_libraries(
        turboraft_wal_storage
        PUBLIC TurboRaft::Core
        PRIVATE Salts::Core Salts::Crypto xxHash::xxhash
)
add_library(turboraft_snapshot_coordinator STATIC src/service/raft_snapshot_coordinator.c)
target_include_directories(
        turboraft_snapshot_coordinator PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src/service
)
target_link_libraries(turboraft_snapshot_coordinator PUBLIC TurboRaft::SnapshotSender)
add_library(turboraft_snapshot_installer STATIC src/service/raft_snapshot_installer.c)
target_include_directories(
        turboraft_snapshot_installer PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src/service
)
target_link_libraries(turboraft_snapshot_installer PUBLIC TurboRaft::WalStorage)
add_library(turboraft_service_wal_reload STATIC src/service/raft_service_wal_reload.c)
target_include_directories(
        turboraft_service_wal_reload PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src/service
)
target_link_libraries(turboraft_service_wal_reload PUBLIC TurboRaft::Service TurboRaft::WalStorage)

find_package(Threads REQUIRED)
add_library(
        turboraft_cnet STATIC src/transport/raft_cnet_peer.c src/transport/raft_group_queue.c
                              src/transport/raft_transport.c src/transport/raft_peer_handshake.c
)
add_library(TurboRaft::CNet ALIAS turboraft_cnet)
set_target_properties(turboraft_cnet PROPERTIES EXPORT_NAME CNet)
target_include_directories(
        turboraft_cnet PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                              $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_link_libraries(
        turboraft_cnet
        PUBLIC TurboRaft::Core Salts::CNet
        PRIVATE CMeta::CSTL CMeta::CMeta
)
add_library(turboraft_snapshot_peer STATIC src/service/raft_snapshot_peer.c)
target_include_directories(turboraft_snapshot_peer PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src/service)
target_link_libraries(turboraft_snapshot_peer PUBLIC TurboRaft::CNet turboraft_snapshot_coordinator)
add_library(
        turboraft_snapshot_manager STATIC
        src/service/raft_snapshot_manager.c src/service/raft_snapshot_peer.c
        src/service/raft_snapshot_coordinator.c
)
add_library(TurboRaft::SnapshotManager ALIAS turboraft_snapshot_manager)
set_target_properties(turboraft_snapshot_manager PROPERTIES EXPORT_NAME SnapshotManager)
target_include_directories(
        turboraft_snapshot_manager
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
               $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src/service
)
target_link_libraries(turboraft_snapshot_manager PUBLIC TurboRaft::CNet TurboRaft::SnapshotSender)

file(READ "${PROJECT_SOURCE_DIR}/schema/turboraft_node.schema" TURBORAFT_NODE_SCHEMA_TEXT)
set_property(
        DIRECTORY
        APPEND
        PROPERTY CMAKE_CONFIGURE_DEPENDS "${PROJECT_SOURCE_DIR}/schema/turboraft_node.schema"
)
string(REPLACE "\\" "\\\\" TURBORAFT_NODE_SCHEMA_TEXT "${TURBORAFT_NODE_SCHEMA_TEXT}")
string(REPLACE "\"" "\\\"" TURBORAFT_NODE_SCHEMA_TEXT "${TURBORAFT_NODE_SCHEMA_TEXT}")
string(REPLACE "\n" "\\n\"\n\"" TURBORAFT_NODE_SCHEMA_TEXT "${TURBORAFT_NODE_SCHEMA_TEXT}")
configure_file(src/runtime/node_schema.h.in generated/node_schema.h @ONLY)
add_library(turboraft_node_config STATIC src/runtime/raft_node_config.c)
add_library(TurboRaft::NodeConfig ALIAS turboraft_node_config)
set_target_properties(
        turboraft_node_config PROPERTIES EXPORT_NAME NodeConfig FOLDER "libraries/runtime"
)
target_include_directories(
        turboraft_node_config
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
               $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
        PRIVATE "${CMAKE_CURRENT_BINARY_DIR}/generated"
)
target_link_libraries(turboraft_node_config PUBLIC TurboRaft::Multicore Salts::DataBind)

add_library(
        turboraft_control_plane STATIC src/control/raft_control_audit.c
                                       src/control/raft_control_plane.c
)
add_library(TurboRaft::ControlPlane ALIAS turboraft_control_plane)
set_target_properties(turboraft_control_plane PROPERTIES EXPORT_NAME ControlPlane)
target_include_directories(
        turboraft_control_plane PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                                       $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_link_libraries(
        turboraft_control_plane
        PUBLIC TurboRaft::Service CHttp::App
        PRIVATE $<BUILD_INTERFACE:turboraft_control_schema>
)
if(BUILD_TESTS)
        add_executable(turboraft_control_plane_tests tests/control/test_raft_control_plane.c)
        target_link_libraries(
                turboraft_control_plane_tests PRIVATE TurboRaft::ControlPlane CHttp::Client
                                                      Salts::JsonParser Salts::TinyTest
        )
        add_test(NAME turboraft.control_plane COMMAND turboraft_control_plane_tests)
        cmake_config_target(turboraft_control_plane_tests FOLDER "tests/control")
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
        RE2C_EXECUTABLE
        "${RE2C_EXECUTABLE}"
        LEMON_TARGET
        Lemon::Lemon
        LEMON_TEMPLATE
        "${LEMPAR}"
        FOLDER
        "TurboRaft/text-syntax"
)
cmake_add_grammar(
        turboraft_text_protocol_debug
        LEXER_RE
        "${TURBORAFT_TEXT_SYNTAX_DIR}/protocol_debug_lexer.re"
        GRAMMAR_Y
        "${TURBORAFT_TEXT_SYNTAX_DIR}/protocol_debug_grammar.y"
        LEXER_OUTPUT
        turboraft_text_protocol_debug_lexer_gen.c
        LEXER_DEPENDS_ON_GRAMMAR
        RE2C_EXECUTABLE
        "${RE2C_EXECUTABLE}"
        LEMON_TARGET
        Lemon::Lemon
        LEMON_TEMPLATE
        "${LEMPAR}"
        FOLDER
        "TurboRaft/text-syntax"
)
cmake_add_grammar(
        turboraft_text_replay
        LEXER_RE
        "${TURBORAFT_TEXT_SYNTAX_DIR}/replay_lexer.re"
        GRAMMAR_Y
        "${TURBORAFT_TEXT_SYNTAX_DIR}/replay_grammar.y"
        LEXER_OUTPUT
        turboraft_text_replay_lexer_gen.c
        LEXER_DEPENDS_ON_GRAMMAR
        RE2C_EXECUTABLE
        "${RE2C_EXECUTABLE}"
        LEMON_TARGET
        Lemon::Lemon
        LEMON_TEMPLATE
        "${LEMPAR}"
        FOLDER
        "TurboRaft/text-syntax"
)
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
        "${turboraft_text_replay_GRAMMAR_H}"
)
if(MSVC)
        set_source_files_properties(
                "${turboraft_text_query_GRAMMAR_GEN}" "${turboraft_text_protocol_debug_GRAMMAR_GEN}"
                "${turboraft_text_replay_GRAMMAR_GEN}" PROPERTIES COMPILE_OPTIONS "/wd4189"
        )
endif()
add_library(TurboRaft::TextSyntax ALIAS turboraft_text_syntax)
set_target_properties(turboraft_text_syntax PROPERTIES EXPORT_NAME TextSyntax)
target_include_directories(
        turboraft_text_syntax
        PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
               $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
        PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/src/text_syntax" "${CMAKE_CURRENT_BINARY_DIR}"
)
target_link_libraries(turboraft_text_syntax PUBLIC Salts::Core)
add_library(turboraft_text_query_service STATIC src/text_syntax/text_query_service.c)
add_library(TurboRaft::TextQueryService ALIAS turboraft_text_query_service)
set_target_properties(turboraft_text_query_service PROPERTIES EXPORT_NAME TextQueryService)
target_include_directories(
        turboraft_text_query_service PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                                            $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_link_libraries(turboraft_text_query_service PUBLIC TurboRaft::TextSyntax TurboRaft::Service)
add_library(turboraft_text_protocol_debug STATIC src/text_syntax/text_protocol_executor.c)
add_library(TurboRaft::TextProtocolDebug ALIAS turboraft_text_protocol_debug)
set_target_properties(turboraft_text_protocol_debug PROPERTIES EXPORT_NAME TextProtocolDebug)
target_include_directories(
        turboraft_text_protocol_debug PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                                             $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_link_libraries(turboraft_text_protocol_debug PUBLIC TurboRaft::TextSyntax TurboRaft::Core)
add_library(turboraft_replay_driver STATIC src/text_syntax/text_replay_core_driver.c)
add_library(TurboRaft::ReplayDriver ALIAS turboraft_replay_driver)
set_target_properties(turboraft_replay_driver PROPERTIES EXPORT_NAME ReplayDriver)
target_include_directories(
        turboraft_replay_driver PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                                       $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)
target_link_libraries(turboraft_replay_driver PUBLIC TurboRaft::Core)
if(BUILD_TESTS)
        add_subdirectory(tests)
endif()

set(TURBORAFT_INSTALL_TARGETS
    turboraft_core
    turboraft_service
    turboraft_multicore
    turboraft_snapshot_receiver
    turboraft_node_config
    turboraft_snapshot_sender
    turboraft_data_stream
    turboraft_wal_storage
    turboraft_cflow_state_machine
    turboraft_text_syntax
    turboraft_text_query_service
    turboraft_text_protocol_debug
    turboraft_replay_driver
    turboraft_cnet
    turboraft_snapshot_manager
    turboraft_control_plane
)
set(TURBORAFT_HAVE_CNET ON)
set(TURBORAFT_HAVE_CONTROL_PLANE ON)

configure_package_config_file(
        "${PROJECT_SOURCE_DIR}/cmake/TurboRaftConfig.cmake.in"
        "${CMAKE_CURRENT_BINARY_DIR}/TurboRaftConfig.cmake"
        INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/TurboRaft"
)
write_basic_package_version_file(
        "${CMAKE_CURRENT_BINARY_DIR}/TurboRaftConfigVersion.cmake"
        VERSION "${PROJECT_VERSION}"
        COMPATIBILITY SameMinorVersion
)

install(
        TARGETS ${TURBORAFT_INSTALL_TARGETS}
        EXPORT TurboRaftTargets
        ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
        LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
        RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
)
install(DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/include/turboraft"
        DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
)
install(FILES "${PROJECT_SOURCE_DIR}/schema/turboraft_node.schema" DESTINATION "${CMAKE_INSTALL_DATADIR}/turboraft/schema")
install(FILES "${PROJECT_SOURCE_DIR}/docs/PUBLIC_CALLBACK_CONTRACTS.json"
        DESTINATION "${CMAKE_INSTALL_DATADIR}/turboraft/abi"
)
install(
        EXPORT TurboRaftTargets
        FILE TurboRaftTargets.cmake
        NAMESPACE TurboRaft::
        DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/TurboRaft"
)
install(FILES "${CMAKE_CURRENT_BINARY_DIR}/TurboRaftConfig.cmake"
              "${CMAKE_CURRENT_BINARY_DIR}/TurboRaftConfigVersion.cmake"
        DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/TurboRaft"
)

if(BUILD_BENCHMARKS)
        add_subdirectory(benchmarks)
endif()

if(BUILD_EXAMPLES)
        add_subdirectory(examples)
endif()

cmake_config_directory_targets(FOLDER "libraries")
