add_executable(turboraft_text_syntax_tests test_text_syntax.c)
target_link_libraries(turboraft_text_syntax_tests
                      PRIVATE TurboRaft::TextSyntax TurboUtils::TinyTest)
add_test(NAME turboraft.text_syntax COMMAND turboraft_text_syntax_tests)

add_executable(turboraft_text_query_executor_tests
               test_text_query_executor.c)
target_link_libraries(turboraft_text_query_executor_tests
                      PRIVATE TurboRaft::TextSyntax TurboUtils::TinyTest)
add_test(NAME turboraft.text_query_executor
         COMMAND turboraft_text_query_executor_tests)

add_executable(turboraft_text_query_service_tests
               test_text_query_service.c)
target_link_libraries(turboraft_text_query_service_tests
                      PRIVATE TurboRaft::TextQueryService TurboUtils::TinyTest)
add_test(NAME turboraft.text_query_service
         COMMAND turboraft_text_query_service_tests)

add_executable(turboraft_text_protocol_executor_tests
               test_text_protocol_executor.c)
target_link_libraries(turboraft_text_protocol_executor_tests
                      PRIVATE TurboRaft::TextProtocolDebug TurboUtils::TinyTest)
add_test(NAME turboraft.text_protocol_executor
         COMMAND turboraft_text_protocol_executor_tests)
