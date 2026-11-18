cmake_minimum_required(VERSION 3.24)
project(turboraft_storage_resource_benchmark C)

enable_testing()

find_package(TurboRaft CONFIG REQUIRED COMPONENTS WalStorage
             PATHS "$ENV{TURBORAFT_ROOT}" NO_DEFAULT_PATH)

add_executable(turboraft_storage_resource_benchmark
               benchmark_storage_resource.c)
target_link_libraries(turboraft_storage_resource_benchmark
                      PRIVATE TurboRaft::WalStorage)

add_test(NAME turboraft.benchmark_storage_resource COMMAND turboraft_storage_resource_benchmark)
set_tests_properties(turboraft.benchmark_storage_resource PROPERTIES LABELS "benchmark")
