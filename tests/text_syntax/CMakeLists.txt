add_executable(turboraft_text_syntax_tests test_text_syntax.c)
target_link_libraries(turboraft_text_syntax_tests
                      PRIVATE TurboRaft::TextSyntax Salts::TinyTest)
add_test(NAME turboraft.text_syntax COMMAND turboraft_text_syntax_tests)

add_executable(turboraft_text_query_executor_tests
               test_text_query_executor.c)
target_link_libraries(turboraft_text_query_executor_tests
                      PRIVATE TurboRaft::TextSyntax Salts::TinyTest)
add_test(NAME turboraft.text_query_executor
         COMMAND turboraft_text_query_executor_tests)

add_executable(turboraft_text_query_service_tests
               test_text_query_service.c)
target_link_libraries(turboraft_text_query_service_tests
                      PRIVATE TurboRaft::TextQueryService Salts::TinyTest)
add_test(NAME turboraft.text_query_service
         COMMAND turboraft_text_query_service_tests)

add_executable(turboraft_text_protocol_executor_tests
               test_text_protocol_executor.c)
target_link_libraries(turboraft_text_protocol_executor_tests
                      PRIVATE TurboRaft::TextProtocolDebug Salts::TinyTest)
add_test(NAME turboraft.text_protocol_executor
         COMMAND turboraft_text_protocol_executor_tests)

add_executable(turboraft_replay_driver_tests
               test_text_replay_core_driver.c)
target_link_libraries(turboraft_replay_driver_tests
                      PRIVATE TurboRaft::ReplayDriver TurboRaft::TextSyntax
                              Salts::TinyTest)
add_test(NAME turboraft.replay_driver
         COMMAND turboraft_replay_driver_tests)

cmake_config_directory_targets(FOLDER "tests/text_syntax")
