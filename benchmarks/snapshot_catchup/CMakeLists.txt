cmake_minimum_required(VERSION 3.24)
project(turboraft_snapshot_catchup_benchmark C)

enable_testing()

find_package(
  TurboRaft CONFIG REQUIRED
  COMPONENTS Core SnapshotSender SnapshotReceiver
  PATHS "$ENV{TURBORAFT_ROOT}" NO_DEFAULT_PATH)

add_executable(turboraft_snapshot_catchup_benchmark
               benchmark_snapshot_catchup.c)
target_link_libraries(
  turboraft_snapshot_catchup_benchmark
  PRIVATE TurboRaft::Core TurboRaft::SnapshotSender TurboRaft::SnapshotReceiver)

add_test(NAME turboraft.benchmark_snapshot_catchup COMMAND turboraft_snapshot_catchup_benchmark)
set_tests_properties(turboraft.benchmark_snapshot_catchup PROPERTIES LABELS "benchmark")
