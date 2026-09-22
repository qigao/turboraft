add_executable(turboraft_core_tests test_raft_core.c)
target_link_libraries(turboraft_core_tests PRIVATE TurboRaft::Core
                                                   Salts::TinyTest)
add_test(NAME turboraft.core COMMAND turboraft_core_tests)

add_executable(turboraft_operation_status_tests
               test_raft_operation_status.c)
target_link_libraries(turboraft_operation_status_tests
                      PRIVATE TurboRaft::Core Salts::TinyTest)
add_test(NAME turboraft.operation_status
         COMMAND turboraft_operation_status_tests)

add_executable(turboraft_control_operation_tests
               test_raft_control_operation.c
               ../../src/control/raft_control_operation.c)
target_include_directories(turboraft_control_operation_tests
                           PRIVATE ../../src/control)
target_link_libraries(turboraft_control_operation_tests
                      PRIVATE TurboRaft::Core Salts::TinyTest)
add_test(NAME turboraft.control_operation
         COMMAND turboraft_control_operation_tests)

add_executable(turboraft_control_audit_tests
               test_raft_control_audit.c
               ../../src/control/raft_control_audit.c)
target_link_libraries(turboraft_control_audit_tests
                      PRIVATE TurboRaft::Core Salts::TinyTest)
add_test(NAME turboraft.control_audit
         COMMAND turboraft_control_audit_tests)

add_executable(turboraft_check_quorum_tests test_raft_check_quorum.c)
target_link_libraries(turboraft_check_quorum_tests
                      PRIVATE TurboRaft::Core Salts::TinyTest)
add_test(NAME turboraft.check_quorum COMMAND turboraft_check_quorum_tests)

add_executable(turboraft_cluster_simulation_tests
               test_raft_cluster_simulation.c)
target_link_libraries(turboraft_cluster_simulation_tests
                      PRIVATE TurboRaft::Core Salts::TinyTest)
add_test(NAME turboraft.cluster_simulation
         COMMAND turboraft_cluster_simulation_tests)

add_executable(turboraft_log_tests test_raft_log.c)
target_include_directories(turboraft_log_tests PRIVATE
                           ${PROJECT_SOURCE_DIR}/src/core)
target_link_libraries(turboraft_log_tests PRIVATE TurboRaft::Core
                                                  Salts::TinyTest)
add_test(NAME turboraft.log COMMAND turboraft_log_tests)

add_executable(turboraft_replication_tests test_raft_replication.c)
target_link_libraries(turboraft_replication_tests PRIVATE TurboRaft::Core
                                                          Salts::TinyTest)
add_test(NAME turboraft.replication COMMAND turboraft_replication_tests)

add_executable(turboraft_leadership_transfer_tests
               test_raft_leadership_transfer.c)
target_link_libraries(turboraft_leadership_transfer_tests
                      PRIVATE TurboRaft::Core Salts::TinyTest)
add_test(NAME turboraft.leadership_transfer
         COMMAND turboraft_leadership_transfer_tests)

add_executable(turboraft_read_index_tests test_raft_read_index.c)
target_link_libraries(turboraft_read_index_tests PRIVATE TurboRaft::Core
                                                         Salts::TinyTest)
add_test(NAME turboraft.read_index COMMAND turboraft_read_index_tests)

add_executable(turboraft_learner_tests test_raft_learner.c)
target_link_libraries(turboraft_learner_tests PRIVATE TurboRaft::Core
                                                      Salts::TinyTest)
add_test(NAME turboraft.learner COMMAND turboraft_learner_tests)

add_executable(turboraft_configuration_tests test_raft_configuration.c)
target_include_directories(turboraft_configuration_tests
                           PRIVATE ${PROJECT_SOURCE_DIR}/src/core)
target_link_libraries(turboraft_configuration_tests
                      PRIVATE TurboRaft::Core Salts::TinyTest)
add_test(NAME turboraft.configuration COMMAND turboraft_configuration_tests)

add_executable(turboraft_configuration_wire_tests
               test_raft_configuration_wire.c)
target_include_directories(turboraft_configuration_wire_tests
                           PRIVATE ${PROJECT_SOURCE_DIR}/src/core)
target_link_libraries(turboraft_configuration_wire_tests
                      PRIVATE TurboRaft::Core Salts::TinyTest)
add_test(NAME turboraft.configuration_wire
         COMMAND turboraft_configuration_wire_tests)

add_executable(turboraft_membership_tests test_raft_membership.c)
target_include_directories(turboraft_membership_tests
                           PRIVATE ${PROJECT_SOURCE_DIR}/src/core)
target_link_libraries(turboraft_membership_tests
                      PRIVATE TurboRaft::Core Salts::TinyTest)
add_test(NAME turboraft.membership COMMAND turboraft_membership_tests)

add_executable(turboraft_membership_transition_tests
               test_raft_membership_transition.c)
target_include_directories(turboraft_membership_transition_tests
                           PRIVATE ${PROJECT_SOURCE_DIR}/src/core)
target_link_libraries(turboraft_membership_transition_tests
                      PRIVATE TurboRaft::Core Salts::TinyTest)
add_test(NAME turboraft.membership_transition
         COMMAND turboraft_membership_transition_tests)

add_executable(turboraft_configuration_log_tests
               test_raft_configuration_log.c)
target_include_directories(turboraft_configuration_log_tests
                           PRIVATE ${PROJECT_SOURCE_DIR}/src/core)
target_link_libraries(turboraft_configuration_log_tests
                      PRIVATE TurboRaft::Core Salts::TinyTest)
add_test(NAME turboraft.configuration_log
         COMMAND turboraft_configuration_log_tests)

add_executable(turboraft_peer_set_tests test_raft_peer_set.c)
target_include_directories(turboraft_peer_set_tests
                           PRIVATE ${PROJECT_SOURCE_DIR}/src/core)
target_link_libraries(turboraft_peer_set_tests
                      PRIVATE TurboRaft::Core Salts::TinyTest)
add_test(NAME turboraft.peer_set COMMAND turboraft_peer_set_tests)

add_executable(turboraft_joint_consensus_tests test_raft_joint_consensus.c)
target_link_libraries(turboraft_joint_consensus_tests
                      PRIVATE TurboRaft::Core Salts::TinyTest)
add_test(NAME turboraft.joint_consensus
         COMMAND turboraft_joint_consensus_tests)

add_executable(turboraft_joint_recovery_tests test_raft_joint_recovery.c)
target_include_directories(turboraft_joint_recovery_tests
                           PRIVATE ${PROJECT_SOURCE_DIR}/src/core)
target_link_libraries(turboraft_joint_recovery_tests
                      PRIVATE TurboRaft::Core Salts::TinyTest)
add_test(NAME turboraft.joint_recovery
         COMMAND turboraft_joint_recovery_tests)

add_executable(turboraft_recovery_tests test_raft_recovery.c)
target_link_libraries(turboraft_recovery_tests PRIVATE TurboRaft::Core
                                                       Salts::TinyTest)
add_test(NAME turboraft.recovery COMMAND turboraft_recovery_tests)

add_executable(turboraft_apply_ack_tests test_raft_apply_ack.c)
target_link_libraries(turboraft_apply_ack_tests PRIVATE TurboRaft::Core
                                                       Salts::TinyTest)
add_test(NAME turboraft.apply_ack COMMAND turboraft_apply_ack_tests)

add_executable(turboraft_runtime_tests test_raft_runtime.c)
target_link_libraries(turboraft_runtime_tests PRIVATE TurboRaft::Core
                                                      TurboRaft::DataStream
                                                      Salts::TinyTest)
add_test(NAME turboraft.runtime COMMAND turboraft_runtime_tests)

add_executable(turboraft_apply_runtime_tests
               test_raft_apply_runtime.c)
target_link_libraries(turboraft_apply_runtime_tests
                      PRIVATE TurboRaft::Core Salts::TinyTest)
add_test(NAME turboraft.apply_runtime COMMAND turboraft_apply_runtime_tests)

add_executable(turboraft_cflow_state_machine_tests
               test_raft_cflow_state_machine.c)
target_link_libraries(turboraft_cflow_state_machine_tests
                      PRIVATE TurboRaft::CFlowStateMachine Salts::TinyTest)
add_test(NAME turboraft.cflow_state_machine
         COMMAND turboraft_cflow_state_machine_tests)

add_executable(turboraft_cflow_snapshot_replay_tests
               test_raft_cflow_snapshot_replay.c)
target_link_libraries(turboraft_cflow_snapshot_replay_tests
                      PRIVATE TurboRaft::CFlowStateMachine Salts::TinyTest)
add_test(NAME turboraft.cflow_snapshot_replay
         COMMAND turboraft_cflow_snapshot_replay_tests)

if(TURBORAFT_ENABLE_ORM_SQLITE_FIXTURES)
  if(NOT DEFINED ENV{TURBODB_ROOT} OR
     "$ENV{TURBODB_ROOT}" STREQUAL "" OR
     NOT IS_DIRECTORY "$ENV{TURBODB_ROOT}")
    message(FATAL_ERROR
            "TURBODB_ROOT must name the TurboDB install prefix when ORM SQLite fixtures are enabled")
  endif()
  find_package(Orm CONFIG REQUIRED
               PATHS "$ENV{TURBODB_ROOT}" NO_DEFAULT_PATH)
  if(NOT TARGET Orm::C)
    message(FATAL_ERROR "The TurboDB install does not provide Orm::C")
  endif()

  add_executable(turboraft_cflow_orm_sqlite_recovery_tests
                 test_raft_cflow_orm_sqlite_recovery.c)
  target_link_libraries(
    turboraft_cflow_orm_sqlite_recovery_tests
    PRIVATE TurboRaft::CFlowStateMachine Orm::C Salts::Coroutine Salts::TinyTest)
  add_test(NAME turboraft.cflow_orm_sqlite_recovery
           COMMAND turboraft_cflow_orm_sqlite_recovery_tests)
  set_tests_properties(turboraft.cflow_orm_sqlite_recovery
                       PROPERTIES LABELS "orm;sqlite;recovery")

  add_executable(turboraft_orm_sqlite_crash_node
                 ../chaos/raft_orm_sqlite_crash_node.c)
  target_link_libraries(turboraft_orm_sqlite_crash_node PRIVATE Orm::C)

  add_executable(turboraft_orm_sqlite_crash_tests
                 ../chaos/test_raft_orm_sqlite_crash.c)
  target_link_libraries(turboraft_orm_sqlite_crash_tests
                        PRIVATE Salts::Core Salts::Platform Salts::TinyTest)
  add_test(NAME turboraft.orm_sqlite_crash
           COMMAND turboraft_orm_sqlite_crash_tests)
  set_tests_properties(
    turboraft.orm_sqlite_crash
    PROPERTIES ENVIRONMENT
               "TURBORAFT_ORM_SQLITE_CRASH_NODE=$<TARGET_FILE:turboraft_orm_sqlite_crash_node>"
               TIMEOUT 60
               LABELS "orm;sqlite;crash")
endif()

add_executable(turboraft_service_tests test_raft_service.c)
target_link_libraries(turboraft_service_tests PRIVATE TurboRaft::Service
                                                      Salts::TinyTest)
add_test(NAME turboraft.service COMMAND turboraft_service_tests)

add_executable(turboraft_service_diagnostics_tests
               test_raft_service_diagnostics.c)
target_link_libraries(turboraft_service_diagnostics_tests
                      PRIVATE TurboRaft::Service Salts::TinyTest)
add_test(NAME turboraft.service_diagnostics
         COMMAND turboraft_service_diagnostics_tests)

add_executable(turboraft_snapshot_group_isolation_tests
               test_raft_snapshot_group_isolation.c)
target_include_directories(turboraft_snapshot_group_isolation_tests
                           PRIVATE ${PROJECT_SOURCE_DIR}/src/transport)
target_link_libraries(turboraft_snapshot_group_isolation_tests
                      PRIVATE TurboRaft::CNet
                              TurboRaft::SnapshotSender
                              TurboRaft::SnapshotReceiver
                              Salts::TinyTest)
add_test(NAME turboraft.snapshot_group_isolation
         COMMAND turboraft_snapshot_group_isolation_tests)

add_executable(turboraft_proposal_batch_tests
               test_raft_proposal_batch.c)
target_link_libraries(turboraft_proposal_batch_tests
                      PRIVATE TurboRaft::Service Salts::TinyTest)
add_test(NAME turboraft.proposal_batch
         COMMAND turboraft_proposal_batch_tests)

add_executable(turboraft_snapshot_policy_tests
               test_raft_snapshot_policy.c)
target_link_libraries(turboraft_snapshot_policy_tests
                      PRIVATE TurboRaft::Service Salts::TinyTest)
add_test(NAME turboraft.snapshot_policy
         COMMAND turboraft_snapshot_policy_tests)

add_executable(turboraft_compaction_replication_tests
               test_raft_compaction_replication.c)
target_link_libraries(turboraft_compaction_replication_tests
                      PRIVATE TurboRaft::Core Salts::TinyTest)
add_test(NAME turboraft.compaction_replication
         COMMAND turboraft_compaction_replication_tests)

add_executable(turboraft_snapshot_installer_tests
               test_raft_snapshot_installer.c)
  target_include_directories(
    turboraft_snapshot_installer_tests
    PRIVATE ${PROJECT_SOURCE_DIR}/src/service)
  target_link_libraries(turboraft_snapshot_installer_tests
                        PRIVATE turboraft_snapshot_installer
                                Salts::TinyTest)
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
                                Salts::TinyTest)
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
                                Salts::Core
                                Salts::Platform
                                Salts::TinyTest)
  add_test(NAME turboraft.multiprocess_chaos
           COMMAND turboraft_multiprocess_chaos_tests)
  set_tests_properties(
    turboraft.multiprocess_chaos
    PROPERTIES ENVIRONMENT
               "TURBORAFT_CHAOS_NODE=$<TARGET_FILE:turboraft_multiprocess_node>"
               TIMEOUT 600
               LABELS "chaos;multiprocess")

add_executable(turboraft_wire_tests test_raft_wire_codec.c)
target_link_libraries(turboraft_wire_tests PRIVATE TurboRaft::Core
                                                   Salts::TinyTest)
add_test(NAME turboraft.wire COMMAND turboraft_wire_tests)

add_executable(turboraft_data_stream_tests test_raft_data_stream.c)
target_link_libraries(turboraft_data_stream_tests
                      PRIVATE TurboRaft::DataStream Salts::TinyTest)
cmake_config_target(turboraft_data_stream_tests
                    NO_INSTALL
                    FOLDER "tests/core")
add_test(NAME turboraft.data_stream COMMAND turboraft_data_stream_tests)

add_executable(turboraft_wal_storage_tests test_raft_wal_storage.c)
target_link_libraries(turboraft_wal_storage_tests
                      PRIVATE TurboRaft::WalStorage Salts::TinyTest)
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
                      PRIVATE TurboRaft::Core Salts::TinyTest)
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
                              Salts::TinyTest)
add_test(NAME turboraft.snapshot_receiver
         COMMAND turboraft_snapshot_receiver_tests)

add_executable(turboraft_snapshot_sender_tests
               test_raft_snapshot_sender.c)
target_link_libraries(turboraft_snapshot_sender_tests
                      PRIVATE TurboRaft::SnapshotSender
                              Salts::TinyTest)
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
                              Salts::TinyTest)
add_test(NAME turboraft.snapshot_coordinator
         COMMAND turboraft_snapshot_coordinator_tests)

if(TARGET turboraft_cnet)
  add_executable(turboraft_group_queue_tests test_raft_group_queue.c)
  target_include_directories(turboraft_group_queue_tests
                             PRIVATE ${PROJECT_SOURCE_DIR}/src/transport)
  target_link_libraries(turboraft_group_queue_tests
                        PRIVATE TurboRaft::CNet Salts::TinyTest)
  add_test(NAME turboraft.group_queue COMMAND turboraft_group_queue_tests)

  add_executable(turboraft_multigroup_wire_tests
                 test_raft_multigroup_wire.c)
  target_link_libraries(turboraft_multigroup_wire_tests
                        PRIVATE TurboRaft::CNet Salts::TinyTest)
  add_test(NAME turboraft.multigroup_wire
           COMMAND turboraft_multigroup_wire_tests)

  add_executable(turboraft_snapshot_peer_tests test_raft_snapshot_peer.c)
  target_include_directories(turboraft_snapshot_peer_tests
                             PRIVATE ${PROJECT_SOURCE_DIR}/src/service)
  target_link_libraries(turboraft_snapshot_peer_tests
                        PRIVATE turboraft_snapshot_peer
                                TurboRaft::SnapshotReceiver Salts::TinyTest)
  add_test(NAME turboraft.snapshot_peer COMMAND turboraft_snapshot_peer_tests)

  add_executable(turboraft_snapshot_manager_tests
                 test_raft_snapshot_manager.c)
  target_link_libraries(turboraft_snapshot_manager_tests
                        PRIVATE TurboRaft::SnapshotManager
                                TurboRaft::SnapshotReceiver Salts::TinyTest)
  add_test(NAME turboraft.snapshot_manager
           COMMAND turboraft_snapshot_manager_tests)

  add_executable(turboraft_snapshot_manager_runtime_tests
                 test_raft_snapshot_manager_runtime.c)
  target_link_libraries(turboraft_snapshot_manager_runtime_tests
                        PRIVATE TurboRaft::SnapshotManager
                                TurboRaft::SnapshotReceiver Salts::TinyTest)
  add_test(NAME turboraft.snapshot_manager_runtime
           COMMAND turboraft_snapshot_manager_runtime_tests)

  add_executable(turboraft_transport_tests test_raft_transport.c)
  target_link_libraries(turboraft_transport_tests
                        PRIVATE TurboRaft::CNet Salts::TinyTest)
  add_test(NAME turboraft.transport COMMAND turboraft_transport_tests)

  add_executable(turboraft_cnet_peer_tests test_raft_cnet_peer.c)
  target_link_libraries(turboraft_cnet_peer_tests
                        PRIVATE TurboRaft::CNet Salts::TinyTest)
  add_test(NAME turboraft.cnet_peer COMMAND turboraft_cnet_peer_tests)

  add_executable(turboraft_transport_contract_tests
                 test_raft_transport_contract.c)
  target_link_libraries(turboraft_transport_contract_tests
                        PRIVATE TurboRaft::CNet Salts::TinyTest)
  add_test(NAME turboraft.transport_contract
           COMMAND turboraft_transport_contract_tests)

  add_executable(turboraft_peer_handshake_tests test_raft_peer_handshake.c)
  target_link_libraries(turboraft_peer_handshake_tests
                        PRIVATE TurboRaft::CNet Salts::TinyTest)
  add_test(NAME turboraft.peer_handshake
           COMMAND turboraft_peer_handshake_tests)
endif()

if(TARGET turboraft_flowmq)
  add_executable(turboraft_flowmq_peer_service_tests
                 test_raft_flowmq_peer_service.c)
  target_link_libraries(turboraft_flowmq_peer_service_tests
                        PRIVATE TurboRaft::FlowMQ
                                TurboRaft::SnapshotSender
                                TurboRaft::SnapshotReceiver
                                Salts::CNet
                                Salts::TinyTest)
  target_compile_definitions(
    turboraft_flowmq_peer_service_tests
    PRIVATE TURBORAFT_TEST_FIXTURE_DIR="${PROJECT_SOURCE_DIR}/tests/fixtures")
  add_test(NAME turboraft.flowmq_peer_service
           COMMAND turboraft_flowmq_peer_service_tests)
  set_tests_properties(turboraft.flowmq_peer_service PROPERTIES TIMEOUT 30)
endif()

if(TARGET turboraft_turbodb_redis_state_machine)
  add_executable(turboraft_turbodb_redis_state_machine_tests
                 test_turbodb_redis_state_machine.c)
  target_link_libraries(turboraft_turbodb_redis_state_machine_tests
                        PRIVATE TurboRaft::TurboDbRedisStateMachine
                                Salts::TinyTest)
  add_test(NAME turboraft.turbodb_redis_state_machine
           COMMAND turboraft_turbodb_redis_state_machine_tests)
endif()
cmake_config_directory_targets(FOLDER "tests/core")
