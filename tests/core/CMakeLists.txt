add_executable(turboraft_core_tests test_raft_core.c)
target_link_libraries(turboraft_core_tests PRIVATE TurboRaft::Core
                                                   TurboUtils::TinyTest)
add_test(NAME turboraft.core COMMAND turboraft_core_tests)

add_executable(turboraft_operation_status_tests
               test_raft_operation_status.c)
target_link_libraries(turboraft_operation_status_tests
                      PRIVATE TurboRaft::Core TurboUtils::TinyTest)
add_test(NAME turboraft.operation_status
         COMMAND turboraft_operation_status_tests)

add_executable(turboraft_control_operation_tests
               test_raft_control_operation.c
               ../../src/control/raft_control_operation.c)
target_include_directories(turboraft_control_operation_tests
                           PRIVATE ../../src/control)
target_link_libraries(turboraft_control_operation_tests
                      PRIVATE TurboRaft::Core TurboUtils::TinyTest)
add_test(NAME turboraft.control_operation
         COMMAND turboraft_control_operation_tests)

add_executable(turboraft_control_audit_tests
               test_raft_control_audit.c
               ../../src/control/raft_control_audit.c)
target_link_libraries(turboraft_control_audit_tests
                      PRIVATE TurboRaft::Core TurboUtils::TinyTest)
add_test(NAME turboraft.control_audit
         COMMAND turboraft_control_audit_tests)

add_executable(turboraft_check_quorum_tests test_raft_check_quorum.c)
target_link_libraries(turboraft_check_quorum_tests
                      PRIVATE TurboRaft::Core TurboUtils::TinyTest)
add_test(NAME turboraft.check_quorum COMMAND turboraft_check_quorum_tests)

add_executable(turboraft_cluster_simulation_tests
               test_raft_cluster_simulation.c)
target_link_libraries(turboraft_cluster_simulation_tests
                      PRIVATE TurboRaft::Core TurboUtils::TinyTest)
add_test(NAME turboraft.cluster_simulation
         COMMAND turboraft_cluster_simulation_tests)

add_executable(turboraft_log_tests test_raft_log.c)
target_include_directories(turboraft_log_tests PRIVATE
                           ${PROJECT_SOURCE_DIR}/src/core)
target_link_libraries(turboraft_log_tests PRIVATE TurboRaft::Core
                                                  TurboUtils::TinyTest)
add_test(NAME turboraft.log COMMAND turboraft_log_tests)

add_executable(turboraft_replication_tests test_raft_replication.c)
target_link_libraries(turboraft_replication_tests PRIVATE TurboRaft::Core
                                                          TurboUtils::TinyTest)
add_test(NAME turboraft.replication COMMAND turboraft_replication_tests)

add_executable(turboraft_leadership_transfer_tests
               test_raft_leadership_transfer.c)
target_link_libraries(turboraft_leadership_transfer_tests
                      PRIVATE TurboRaft::Core TurboUtils::TinyTest)
add_test(NAME turboraft.leadership_transfer
         COMMAND turboraft_leadership_transfer_tests)

add_executable(turboraft_read_index_tests test_raft_read_index.c)
target_link_libraries(turboraft_read_index_tests PRIVATE TurboRaft::Core
                                                         TurboUtils::TinyTest)
add_test(NAME turboraft.read_index COMMAND turboraft_read_index_tests)

add_executable(turboraft_learner_tests test_raft_learner.c)
target_link_libraries(turboraft_learner_tests PRIVATE TurboRaft::Core
                                                      TurboUtils::TinyTest)
add_test(NAME turboraft.learner COMMAND turboraft_learner_tests)

add_executable(turboraft_configuration_tests test_raft_configuration.c)
target_include_directories(turboraft_configuration_tests
                           PRIVATE ${PROJECT_SOURCE_DIR}/src/core)
target_link_libraries(turboraft_configuration_tests
                      PRIVATE TurboRaft::Core TurboUtils::TinyTest)
add_test(NAME turboraft.configuration COMMAND turboraft_configuration_tests)

add_executable(turboraft_configuration_wire_tests
               test_raft_configuration_wire.c)
target_include_directories(turboraft_configuration_wire_tests
                           PRIVATE ${PROJECT_SOURCE_DIR}/src/core)
target_link_libraries(turboraft_configuration_wire_tests
                      PRIVATE TurboRaft::Core TurboUtils::TinyTest)
add_test(NAME turboraft.configuration_wire
         COMMAND turboraft_configuration_wire_tests)

add_executable(turboraft_membership_tests test_raft_membership.c)
target_include_directories(turboraft_membership_tests
                           PRIVATE ${PROJECT_SOURCE_DIR}/src/core)
target_link_libraries(turboraft_membership_tests
                      PRIVATE TurboRaft::Core TurboUtils::TinyTest)
add_test(NAME turboraft.membership COMMAND turboraft_membership_tests)

add_executable(turboraft_membership_transition_tests
               test_raft_membership_transition.c)
target_include_directories(turboraft_membership_transition_tests
                           PRIVATE ${PROJECT_SOURCE_DIR}/src/core)
target_link_libraries(turboraft_membership_transition_tests
                      PRIVATE TurboRaft::Core TurboUtils::TinyTest)
add_test(NAME turboraft.membership_transition
         COMMAND turboraft_membership_transition_tests)

add_executable(turboraft_configuration_log_tests
               test_raft_configuration_log.c)
target_include_directories(turboraft_configuration_log_tests
                           PRIVATE ${PROJECT_SOURCE_DIR}/src/core)
target_link_libraries(turboraft_configuration_log_tests
                      PRIVATE TurboRaft::Core TurboUtils::TinyTest)
add_test(NAME turboraft.configuration_log
         COMMAND turboraft_configuration_log_tests)

add_executable(turboraft_peer_set_tests test_raft_peer_set.c)
target_include_directories(turboraft_peer_set_tests
                           PRIVATE ${PROJECT_SOURCE_DIR}/src/core)
target_link_libraries(turboraft_peer_set_tests
                      PRIVATE TurboRaft::Core TurboUtils::TinyTest)
add_test(NAME turboraft.peer_set COMMAND turboraft_peer_set_tests)

add_executable(turboraft_joint_consensus_tests test_raft_joint_consensus.c)
target_link_libraries(turboraft_joint_consensus_tests
                      PRIVATE TurboRaft::Core TurboUtils::TinyTest)
add_test(NAME turboraft.joint_consensus
         COMMAND turboraft_joint_consensus_tests)

add_executable(turboraft_joint_recovery_tests test_raft_joint_recovery.c)
target_include_directories(turboraft_joint_recovery_tests
                           PRIVATE ${PROJECT_SOURCE_DIR}/src/core)
target_link_libraries(turboraft_joint_recovery_tests
                      PRIVATE TurboRaft::Core TurboUtils::TinyTest)
add_test(NAME turboraft.joint_recovery
         COMMAND turboraft_joint_recovery_tests)

add_executable(turboraft_recovery_tests test_raft_recovery.c)
target_link_libraries(turboraft_recovery_tests PRIVATE TurboRaft::Core
                                                       TurboUtils::TinyTest)
add_test(NAME turboraft.recovery COMMAND turboraft_recovery_tests)

add_executable(turboraft_runtime_tests test_raft_runtime.c)
target_link_libraries(turboraft_runtime_tests PRIVATE TurboRaft::Core
                                                      TurboUtils::TinyTest)
add_test(NAME turboraft.runtime COMMAND turboraft_runtime_tests)

add_executable(turboraft_service_tests test_raft_service.c)
target_link_libraries(turboraft_service_tests PRIVATE TurboRaft::Service
                                                      TurboUtils::TinyTest)
add_test(NAME turboraft.service COMMAND turboraft_service_tests)

add_executable(turboraft_service_diagnostics_tests
               test_raft_service_diagnostics.c)
target_link_libraries(turboraft_service_diagnostics_tests
                      PRIVATE TurboRaft::Service TurboUtils::TinyTest)
add_test(NAME turboraft.service_diagnostics
         COMMAND turboraft_service_diagnostics_tests)

add_executable(turboraft_snapshot_policy_tests
               test_raft_snapshot_policy.c)
target_link_libraries(turboraft_snapshot_policy_tests
                      PRIVATE TurboRaft::Service TurboUtils::TinyTest)
add_test(NAME turboraft.snapshot_policy
         COMMAND turboraft_snapshot_policy_tests)

add_executable(turboraft_compaction_replication_tests
               test_raft_compaction_replication.c)
target_link_libraries(turboraft_compaction_replication_tests
                      PRIVATE TurboRaft::Core TurboUtils::TinyTest)
add_test(NAME turboraft.compaction_replication
         COMMAND turboraft_compaction_replication_tests)

add_executable(turboraft_snapshot_installer_tests
               test_raft_snapshot_installer.c)
  target_include_directories(
    turboraft_snapshot_installer_tests
    PRIVATE ${PROJECT_SOURCE_DIR}/src/service)
  target_link_libraries(turboraft_snapshot_installer_tests
                        PRIVATE turboraft_snapshot_installer
                                TurboUtils::TinyTest)
  add_test(NAME turboraft.snapshot_installer
           COMMAND turboraft_snapshot_installer_tests)

add_executable(turboraft_service_wal_reload_tests
               test_raft_service_wal_reload.c)
  target_include_directories(
    turboraft_service_wal_reload_tests
    PRIVATE ${PROJECT_SOURCE_DIR}/src/service)
target_link_libraries(turboraft_service_wal_reload_tests
                        PRIVATE turboraft_service_wal_reload
                                turboraft_snapshot_installer
                                TurboUtils::TinyTest)
add_test(NAME turboraft.service_wal_reload
         COMMAND turboraft_service_wal_reload_tests)

  add_executable(turboraft_multiprocess_node
                 ../chaos/raft_multiprocess_node.c)
  target_link_libraries(turboraft_multiprocess_node
                        PRIVATE TurboRaft::Service
                                TurboRaft::WalStorage)

  add_executable(turboraft_multiprocess_chaos_tests
                 ../chaos/test_raft_multiprocess_chaos.c)
  target_link_libraries(turboraft_multiprocess_chaos_tests
                        PRIVATE TurboRaft::Core
                                TurboUtils::Core
                                TurboUtils::Platform
                                TurboUtils::TinyTest)
  add_test(NAME turboraft.multiprocess_chaos
           COMMAND turboraft_multiprocess_chaos_tests)
  set_tests_properties(
    turboraft.multiprocess_chaos
    PROPERTIES ENVIRONMENT
               "TURBORAFT_CHAOS_NODE=$<TARGET_FILE:turboraft_multiprocess_node>"
               TIMEOUT 180
               LABELS "chaos;multiprocess")

add_executable(turboraft_wire_tests test_raft_wire_codec.c)
target_link_libraries(turboraft_wire_tests PRIVATE TurboRaft::Core
                                                   TurboUtils::TinyTest)
add_test(NAME turboraft.wire COMMAND turboraft_wire_tests)

add_executable(turboraft_data_stream_tests test_raft_data_stream.c)
target_link_libraries(turboraft_data_stream_tests
                      PRIVATE TurboRaft::DataStream TurboUtils::TinyTest)
cmake_config_target(turboraft_data_stream_tests
                    NO_INSTALL
                    FOLDER "tests/core")
add_test(NAME turboraft.data_stream COMMAND turboraft_data_stream_tests)

add_executable(turboraft_wal_storage_tests test_raft_wal_storage.c)
target_link_libraries(turboraft_wal_storage_tests
                      PRIVATE TurboRaft::WalStorage TurboUtils::TinyTest)
cmake_config_target(turboraft_wal_storage_tests
                    NO_INSTALL
                    FOLDER "tests/core")
add_test(NAME turboraft.wal_storage COMMAND turboraft_wal_storage_tests)

add_executable(turboraft_wire_fuzz_corpus_tests
               test_raft_wire_fuzz_corpus.c
               ../fuzz/raft_wire_fuzz.c)
target_include_directories(
  turboraft_wire_fuzz_corpus_tests
  PRIVATE ${PROJECT_SOURCE_DIR}/tests/fuzz)
target_link_libraries(turboraft_wire_fuzz_corpus_tests
                      PRIVATE TurboRaft::Core TurboUtils::TinyTest)
add_test(NAME turboraft.wire_fuzz_corpus
         COMMAND turboraft_wire_fuzz_corpus_tests)

if(TURBORAFT_BUILD_FUZZERS)
  if(NOT CMAKE_C_COMPILER_ID MATCHES "Clang")
    message(FATAL_ERROR
            "TURBORAFT_BUILD_FUZZERS requires Clang with libFuzzer")
  endif()
  add_executable(turboraft_wire_fuzzer ../fuzz/raft_wire_fuzz.c)
  target_include_directories(
    turboraft_wire_fuzzer PRIVATE ${PROJECT_SOURCE_DIR}/tests/fuzz)
  target_link_libraries(turboraft_wire_fuzzer PRIVATE TurboRaft::Core)
  if(CMAKE_C_COMPILER_FRONTEND_VARIANT STREQUAL "MSVC")
    execute_process(
      COMMAND ${CMAKE_C_COMPILER} --print-resource-dir
      OUTPUT_VARIABLE TURBORAFT_CLANG_RESOURCE_DIR
      OUTPUT_STRIP_TRAILING_WHITESPACE
      RESULT_VARIABLE TURBORAFT_CLANG_RESOURCE_RESULT)
    if(NOT TURBORAFT_CLANG_RESOURCE_RESULT EQUAL 0)
      message(FATAL_ERROR "Unable to locate the Clang runtime directory")
    endif()
    set(TURBORAFT_CLANG_RUNTIME_DIR
        "${TURBORAFT_CLANG_RESOURCE_DIR}/lib/windows")
    set(TURBORAFT_FUZZER_RUNTIME
        "${TURBORAFT_CLANG_RUNTIME_DIR}/clang_rt.fuzzer-x86_64.lib")
    set(TURBORAFT_ASAN_RUNTIME
        "${TURBORAFT_CLANG_RUNTIME_DIR}/clang_rt.asan_dynamic-x86_64.lib")
    set(TURBORAFT_ASAN_THUNK
        "${TURBORAFT_CLANG_RUNTIME_DIR}/clang_rt.asan_dynamic_runtime_thunk-x86_64.lib")
    foreach(runtime IN ITEMS TURBORAFT_FUZZER_RUNTIME
                             TURBORAFT_ASAN_RUNTIME
                             TURBORAFT_ASAN_THUNK)
      if(NOT EXISTS "${${runtime}}")
        message(FATAL_ERROR "Required Clang runtime is missing: ${${runtime}}")
      endif()
    endforeach()
    target_compile_options(
      turboraft_wire_fuzzer PRIVATE -fsanitize=fuzzer,address,undefined /Oy-)
    target_compile_options(
      turboraft_core PRIVATE -fsanitize=fuzzer,address,undefined /Oy-)
    target_link_libraries(turboraft_wire_fuzzer PRIVATE
                          "${TURBORAFT_ASAN_RUNTIME}")
    target_link_options(
      turboraft_wire_fuzzer PRIVATE
      "/WHOLEARCHIVE:${TURBORAFT_FUZZER_RUNTIME}"
      "/INCLUDE:__asan_seh_interceptor"
      "/WHOLEARCHIVE:${TURBORAFT_ASAN_THUNK}"
      /INFERASANLIBS:NO)
  else()
    target_compile_options(
      turboraft_wire_fuzzer PRIVATE -fsanitize=fuzzer,address,undefined
                                    -fno-omit-frame-pointer)
    target_compile_options(
      turboraft_core PRIVATE -fsanitize=fuzzer,address,undefined
                             -fno-omit-frame-pointer)
    target_link_options(
      turboraft_wire_fuzzer PRIVATE -fsanitize=fuzzer,address,undefined)
  endif()
endif()

add_executable(turboraft_snapshot_receiver_tests
               test_raft_snapshot_receiver.c)
target_link_libraries(turboraft_snapshot_receiver_tests
                      PRIVATE TurboRaft::SnapshotReceiver
                              TurboUtils::TinyTest)
add_test(NAME turboraft.snapshot_receiver
         COMMAND turboraft_snapshot_receiver_tests)

add_executable(turboraft_snapshot_sender_tests
               test_raft_snapshot_sender.c)
target_link_libraries(turboraft_snapshot_sender_tests
                      PRIVATE TurboRaft::SnapshotSender
                              TurboUtils::TinyTest)
add_test(NAME turboraft.snapshot_sender
         COMMAND turboraft_snapshot_sender_tests)

add_executable(turboraft_snapshot_coordinator_tests
               test_raft_snapshot_coordinator.c)
target_include_directories(
  turboraft_snapshot_coordinator_tests
  PRIVATE ${PROJECT_SOURCE_DIR}/src/service)
target_link_libraries(turboraft_snapshot_coordinator_tests
                      PRIVATE turboraft_snapshot_coordinator
                              TurboRaft::SnapshotReceiver
                              TurboUtils::TinyTest)
add_test(NAME turboraft.snapshot_coordinator
         COMMAND turboraft_snapshot_coordinator_tests)

if(TARGET turboraft_coronet)
  add_executable(turboraft_snapshot_peer_tests
                 test_raft_snapshot_peer.c)
  target_include_directories(
    turboraft_snapshot_peer_tests
    PRIVATE ${PROJECT_SOURCE_DIR}/src/service)
  target_link_libraries(turboraft_snapshot_peer_tests
                        PRIVATE turboraft_snapshot_peer
                                TurboRaft::SnapshotReceiver
                                TurboUtils::TinyTest)
  add_test(NAME turboraft.snapshot_peer
           COMMAND turboraft_snapshot_peer_tests)

  add_executable(turboraft_snapshot_manager_tests
                 test_raft_snapshot_manager.c)
  target_include_directories(
    turboraft_snapshot_manager_tests
    PRIVATE ${PROJECT_SOURCE_DIR}/src/service)
  target_link_libraries(turboraft_snapshot_manager_tests
                        PRIVATE turboraft_snapshot_manager
                                TurboRaft::SnapshotReceiver
                                TurboUtils::TinyTest)
  add_test(NAME turboraft.snapshot_manager
           COMMAND turboraft_snapshot_manager_tests)

  add_executable(turboraft_snapshot_manager_runtime_tests
                 test_raft_snapshot_manager_runtime.c)
  target_include_directories(
    turboraft_snapshot_manager_runtime_tests
    PRIVATE ${PROJECT_SOURCE_DIR}/src/service)
  target_link_libraries(turboraft_snapshot_manager_runtime_tests
                        PRIVATE turboraft_snapshot_manager
                                TurboRaft::SnapshotReceiver
                                TurboUtils::TinyTest)
  add_test(NAME turboraft.snapshot_manager_runtime
           COMMAND turboraft_snapshot_manager_runtime_tests)

  add_executable(turboraft_coronet_transport_tests
                 test_raft_coronet_transport.c)
  target_link_libraries(turboraft_coronet_transport_tests
                        PRIVATE TurboRaft::CoroNet TurboUtils::TinyTest)
  add_test(NAME turboraft.coronet_transport
           COMMAND turboraft_coronet_transport_tests)

  add_executable(turboraft_peer_handshake_tests test_raft_peer_handshake.c)
  target_link_libraries(turboraft_peer_handshake_tests
                        PRIVATE TurboRaft::CoroNet TurboUtils::TinyTest)

  add_executable(turboraft_wire_upgrade_tests test_raft_wire_upgrade.c)
  target_link_libraries(turboraft_wire_upgrade_tests
                        PRIVATE TurboRaft::CoroNet TurboUtils::TinyTest)
  add_test(NAME turboraft.wire_upgrade COMMAND turboraft_wire_upgrade_tests)
  add_test(NAME turboraft.peer_handshake
           COMMAND turboraft_peer_handshake_tests)

  add_executable(turboraft_coronet_mtls_tests test_raft_coronet_mtls.c)
  target_compile_definitions(
    turboraft_coronet_mtls_tests
    PRIVATE TURBORAFT_TEST_FIXTURE_DIR="${PROJECT_SOURCE_DIR}/tests/fixtures")
  target_link_libraries(turboraft_coronet_mtls_tests
                        PRIVATE TurboRaft::CoroNet TurboUtils::Platform
                                TurboUtils::TinyTest)
  if(WIN32)
    target_link_libraries(turboraft_coronet_mtls_tests PRIVATE ws2_32)
  endif()
  add_test(NAME turboraft.coronet_mtls COMMAND turboraft_coronet_mtls_tests)

  add_executable(turboraft_coronet_mtls_rejection_tests
                 test_raft_coronet_mtls_rejection.c)
  target_compile_definitions(
    turboraft_coronet_mtls_rejection_tests
    PRIVATE TURBORAFT_TEST_FIXTURE_DIR="${PROJECT_SOURCE_DIR}/tests/fixtures")
  target_link_libraries(turboraft_coronet_mtls_rejection_tests
                        PRIVATE TurboRaft::CoroNet TurboUtils::Platform
                                TurboUtils::TinyTest)
  if(WIN32)
    target_link_libraries(turboraft_coronet_mtls_rejection_tests
                          PRIVATE ws2_32)
  endif()
  add_test(NAME turboraft.coronet_mtls_rejection
           COMMAND turboraft_coronet_mtls_rejection_tests)

  add_executable(turboraft_coronet_identity_registry_tests
                 test_raft_coronet_identity_registry.c)
  target_link_libraries(turboraft_coronet_identity_registry_tests
                        PRIVATE TurboRaft::CoroNet TurboUtils::TinyTest)
  add_test(NAME turboraft.coronet_identity_registry
           COMMAND turboraft_coronet_identity_registry_tests)

  add_executable(turboraft_coronet_peer_service_tests
                 test_raft_coronet_peer_service.c)
  target_link_libraries(turboraft_coronet_peer_service_tests
                        PRIVATE TurboRaft::CoroNet TurboUtils::TinyTest)
  add_test(NAME turboraft.coronet_peer_service
           COMMAND turboraft_coronet_peer_service_tests)

  add_executable(turboraft_coronet_peer_service_mtls_tests
                 test_raft_coronet_peer_service_mtls.c)
  target_compile_definitions(
    turboraft_coronet_peer_service_mtls_tests
    PRIVATE TURBORAFT_TEST_FIXTURE_DIR="${PROJECT_SOURCE_DIR}/tests/fixtures")
  target_link_libraries(turboraft_coronet_peer_service_mtls_tests
                        PRIVATE TurboRaft::CoroNet
                                TurboRaft::SnapshotReceiver
                                TurboRaft::SnapshotSender
                                TurboUtils::Platform
                                TurboUtils::TinyTest)
  if(WIN32)
    target_link_libraries(turboraft_coronet_peer_service_mtls_tests
                          PRIVATE ws2_32)
  endif()
  add_test(NAME turboraft.coronet_peer_service_mtls
           COMMAND turboraft_coronet_peer_service_mtls_tests)

  add_executable(turboraft_coronet_three_node_mtls_tests
                 test_raft_coronet_three_node_mtls.c)
  target_compile_definitions(
    turboraft_coronet_three_node_mtls_tests
    PRIVATE TURBORAFT_TEST_FIXTURE_DIR="${PROJECT_SOURCE_DIR}/tests/fixtures")
  target_link_libraries(turboraft_coronet_three_node_mtls_tests
                        PRIVATE TurboRaft::CoroNet TurboUtils::Platform
                                TurboUtils::TinyTest)
  if(WIN32)
    target_link_libraries(turboraft_coronet_three_node_mtls_tests
                          PRIVATE ws2_32)
  endif()
  add_test(NAME turboraft.coronet_three_node_mtls
           COMMAND turboraft_coronet_three_node_mtls_tests)

  if(TARGET turboraft_flowmq)
    add_executable(turboraft_flowmq_peer_service_tests
                   test_raft_flowmq_peer_service.c)
    target_compile_definitions(
      turboraft_flowmq_peer_service_tests
      PRIVATE TURBORAFT_TEST_FIXTURE_DIR="${PROJECT_SOURCE_DIR}/tests/fixtures")
    target_link_libraries(turboraft_flowmq_peer_service_tests
                          PRIVATE TurboRaft::FlowMQ TurboRaft::DataStream
                                  TurboUtils::Platform
                                  TurboUtils::TinyTest)
    if(WIN32)
      target_link_libraries(turboraft_flowmq_peer_service_tests PRIVATE ws2_32)
      add_custom_command(
        TARGET turboraft_flowmq_peer_service_tests
        POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
                $<TARGET_FILE:TurboUtils::Core>
                $<TARGET_FILE:TurboParser::Parser>
                $<TARGET_FILE:TurboParser::DataBind>
                $<TARGET_FILE:TurboNet::CoroNet>
                $<TARGET_FILE:FlowMQ::FlowMQ>
                $<TARGET_FILE_DIR:turboraft_flowmq_peer_service_tests>)
    endif()
    add_test(NAME turboraft.flowmq_peer_service
             COMMAND turboraft_flowmq_peer_service_tests)
    set_tests_properties(turboraft.flowmq_peer_service
                         PROPERTIES TIMEOUT 30)
  endif()
endif()

cmake_config_directory_targets(FOLDER "tests/core")
