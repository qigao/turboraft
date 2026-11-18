cmake_minimum_required(VERSION 3.24)
project(turboraft_read_index_benchmark C)

enable_testing()

find_package(TurboRaft CONFIG REQUIRED COMPONENTS Service WalStorage
             PATHS "$ENV{TURBORAFT_ROOT}" NO_DEFAULT_PATH)

add_executable(turboraft_read_index_benchmark benchmark_read_index.c)
target_link_libraries(turboraft_read_index_benchmark
                      PRIVATE TurboRaft::Service TurboRaft::WalStorage)

add_test(NAME turboraft.benchmark_read_index COMMAND turboraft_read_index_benchmark)
set_tests_properties(turboraft.benchmark_read_index PROPERTIES LABELS "benchmark")
