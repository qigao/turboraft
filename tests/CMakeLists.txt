if(MSVC AND ENABLE_MSVC_ANALYZE)
  # Fixed-capacity Raft fixtures intentionally live on the stack in tests.
  # Keep every other analyzer warning fatal without weakening library targets.
  add_compile_options(/wd6262)
endif()

add_subdirectory(core)

if(BUILD_TESTS)
  add_subdirectory(text_syntax)
endif()
