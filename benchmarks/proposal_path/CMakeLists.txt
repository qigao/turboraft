cmake_minimum_required(VERSION 3.24)
project(turboraft_durable_fsync_benchmark C)

enable_testing()

find_package(TurboRaft CONFIG REQUIRED COMPONENTS Service WalStorage
             PATHS "$ENV{TURBORAFT_ROOT}" NO_DEFAULT_PATH)

add_executable(turboraft_durable_fsync_benchmark
               ../benchmark_proposal_path.c)
target_link_libraries(turboraft_durable_fsync_benchmark
                      PRIVATE TurboRaft::Service TurboRaft::WalStorage)

add_test(NAME turboraft.benchmark_durable_fsync COMMAND turboraft_durable_fsync_benchmark)
set_tests_properties(turboraft.benchmark_durable_fsync PROPERTIES LABELS "benchmark")
