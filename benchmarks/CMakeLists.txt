add_executable(turboraft_benchmarks benchmark_turboraft.c)
target_link_libraries(
  turboraft_benchmarks PRIVATE TurboRaft::Core TurboRaft::WalStorage
                               Salts::TinyTest)
target_compile_definitions(turboraft_benchmarks
                           PRIVATE TURBORAFT_BENCHMARK_WAL=1)

cmake_config_target(turboraft_benchmarks FOLDER "benchmarks")
add_test(NAME turboraft.benchmark.baselines COMMAND turboraft_benchmarks)
set_tests_properties(turboraft.benchmark.baselines PROPERTIES LABELS benchmark
                                                              TIMEOUT 120)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  add_executable(turboraft_proposal_path_benchmark benchmark_proposal_path.c)
  target_link_libraries(turboraft_proposal_path_benchmark
                        PRIVATE TurboRaft::Service TurboRaft::WalStorage)
  cmake_config_target(turboraft_proposal_path_benchmark FOLDER "benchmarks")
endif()
add_executable(turboraft_multicore_benchmark benchmark_multicore.c)
target_link_libraries(turboraft_multicore_benchmark PRIVATE TurboRaft::Multicore
                                                            Salts::TinyTest)
if(WIN32)
  target_link_libraries(turboraft_multicore_benchmark PRIVATE psapi)
endif()
cmake_config_target(turboraft_multicore_benchmark FOLDER "benchmarks")
add_test(NAME turboraft.benchmark.multicore
         COMMAND turboraft_multicore_benchmark)
set_tests_properties(turboraft.benchmark.multicore PROPERTIES LABELS benchmark
                                                              TIMEOUT 60)
