# Find re2c
find_program(RE2C_EXECUTABLE re2c)
if(NOT RE2C_EXECUTABLE)
    message(WARNING "re2c not found - some lexers might not be generated")
endif()

# Lemon is always built from the repository so generated parsers use the
# pinned template and implementation.
if(CMAKE_CROSSCOMPILING)
    message(FATAL_ERROR
        "TurboRaft text syntax requires a native host build for tools/lemon")
endif()
if(NOT TARGET lemon)
    message(FATAL_ERROR
        "TurboRaft text syntax requires the project-provided lemon target")
endif()
set(LEMON_EXECUTABLE "$<TARGET_FILE:lemon>")
set(LEMON_DEPENDS lemon)
message(STATUS "Using project-provided lemon target")

# Set path to lemon parser template
set(LEMPAR "${CMAKE_SOURCE_DIR}/tools/lemon/lempar.c" CACHE PATH "Path to lemon parser template")

# Note: These variables are set in the root scope and will be inherited 
# by all subdirectories added via add_subdirectory().

message(STATUS "Tools detection:")
message(STATUS "  re2c: ${RE2C_EXECUTABLE}")
message(STATUS "  lemon: ${LEMON_EXECUTABLE}")
message(STATUS "  lempar: ${LEMPAR}")
