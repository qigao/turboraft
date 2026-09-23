add_executable(lemon lemon.c)
cmake_config_target(lemon
                    NO_INSTALL
                    ALIAS TurboRaft::Lemon
                    FOLDER "tools")
target_compile_features(lemon PRIVATE c_std_11)

# Lemon is a build-host parser generator, not a shipped TurboRaft runtime
# target. Global product sanitizers must not instrument a generator that runs
# during the build itself; otherwise generator-internal findings prevent the
# instrumented product/tests from being built at all.
if(NOT MSVC AND
   (ENABLE_SANITIZER_ADDRESS OR ENABLE_SANITIZER_UNDEFINED OR
    ENABLE_SANITIZER_LEAK OR ENABLE_SANITIZER_THREAD OR
    ENABLE_SANITIZER_MEMORY))
  target_compile_options(lemon PRIVATE -fno-sanitize=all)
  target_link_options(lemon PRIVATE -fno-sanitize=all)
endif()
