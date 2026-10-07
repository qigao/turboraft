add_executable(turboraft_dsl_embed_demo dsl_embed_demo.c)
target_link_libraries(
  turboraft_dsl_embed_demo
  PRIVATE TurboRaft::TextSyntax TurboRaft::ReplayDriver)

cmake_config_directory_targets(FOLDER "examples")
add_executable(turboraft_multicore_node multicore_node.c)
target_link_libraries(turboraft_multicore_node PRIVATE TurboRaft::NodeConfig TurboRaft::WalStorage)
