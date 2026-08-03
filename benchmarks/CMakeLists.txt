add_executable(turboraft_benchmarks benchmark_turboraft.c)
target_link_libraries(turboraft_benchmarks
                      PRIVATE TurboRaft::Core TurboUtils::TinyTest)

if(TARGET TurboRaft::SQLiteStorage)
  target_link_libraries(turboraft_benchmarks
                        PRIVATE TurboRaft::SQLiteStorage)
  target_compile_definitions(turboraft_benchmarks
                             PRIVATE TURBORAFT_BENCHMARK_SQLITE=1)
endif()

set_target_properties(turboraft_benchmarks PROPERTIES FOLDER "benchmarks")

if(WIN32)
  add_custom_command(
    TARGET turboraft_benchmarks POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
            $<TARGET_RUNTIME_DLLS:turboraft_benchmarks>
            $<TARGET_FILE_DIR:turboraft_benchmarks>
    COMMAND_EXPAND_LISTS)
endif()
