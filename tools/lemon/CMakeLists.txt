add_executable(lemon lemon.c)
cmake_config_target(lemon
                    NO_INSTALL
                    ALIAS TurboNet::Lemon
                    FOLDER "tools")
target_compile_features(lemon PRIVATE c_std_11)
