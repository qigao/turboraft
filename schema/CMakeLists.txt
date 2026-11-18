find_program(
        TURBORAFT_IDLC
        NAMES salts-idlc salts-idlc.exe
        PATHS "$ENV{SALTS_UTILS_ROOT}/bin"
        NO_DEFAULT_PATH REQUIRED
        DOC "SaltsUtils IDL/schema compiler"
)
set(turboraft_idlc_command "${TURBORAFT_IDLC}")
if(CMAKE_HOST_APPLE)
  list(PREPEND turboraft_idlc_command "${CMAKE_COMMAND}" -E env
       --modify "DYLD_LIBRARY_PATH=path_list_prepend:$ENV{SALTS_BASE_ROOT}/lib" --)
endif()

set(TURBORAFT_WIRE_SCHEMA "${CMAKE_CURRENT_SOURCE_DIR}/turboraft_wire.schema")
set(TURBORAFT_WIRE_GENERATED_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated/wire")
set(TURBORAFT_WIRE_GENERATED_HEADER "${TURBORAFT_WIRE_GENERATED_DIR}/turboraft_wire_tbe.h")
set(TURBORAFT_WIRE_GENERATED_SOURCE "${TURBORAFT_WIRE_GENERATED_DIR}/turboraft_wire_tbe.c")
add_custom_command(
        OUTPUT "${TURBORAFT_WIRE_GENERATED_HEADER}" "${TURBORAFT_WIRE_GENERATED_SOURCE}"
        COMMAND "${CMAKE_COMMAND}" -E make_directory "${TURBORAFT_WIRE_GENERATED_DIR}"
        COMMAND
                ${turboraft_idlc_command} "${TURBORAFT_WIRE_SCHEMA}" --lang c --binary-codec --output
                "${TURBORAFT_WIRE_GENERATED_HEADER}" --source-output
                "${TURBORAFT_WIRE_GENERATED_SOURCE}"
        DEPENDS "${TURBORAFT_WIRE_SCHEMA}" "${TURBORAFT_IDLC}"
        COMMENT "Generating current TurboRaft wire codec from schema"
        VERBATIM
)
source_group(
        "Generated\\Wire" FILES "${TURBORAFT_WIRE_GENERATED_HEADER}"
                                "${TURBORAFT_WIRE_GENERATED_SOURCE}"
)
add_library(
        turboraft_wire_schema OBJECT "${TURBORAFT_WIRE_GENERATED_HEADER}"
                                     "${TURBORAFT_WIRE_GENERATED_SOURCE}"
)
target_include_directories(turboraft_wire_schema PUBLIC "${TURBORAFT_WIRE_GENERATED_DIR}")
target_link_libraries(turboraft_wire_schema PRIVATE Salts::DataBind)
cmake_config_target(turboraft_wire_schema FOLDER "schema")

set(TURBORAFT_CONTROL_SCHEMA "${CMAKE_CURRENT_SOURCE_DIR}/turboraft_control.schema")
set(TURBORAFT_CONTROL_GENERATED_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated/control")
set(TURBORAFT_CONTROL_GENERATED_HEADER "${TURBORAFT_CONTROL_GENERATED_DIR}/turboraft_control.h")
set(TURBORAFT_CONTROL_GENERATED_SOURCE "${TURBORAFT_CONTROL_GENERATED_DIR}/turboraft_control.c")
add_custom_command(
        OUTPUT "${TURBORAFT_CONTROL_GENERATED_HEADER}" "${TURBORAFT_CONTROL_GENERATED_SOURCE}"
        COMMAND "${CMAKE_COMMAND}" -E make_directory "${TURBORAFT_CONTROL_GENERATED_DIR}"
        COMMAND
                ${turboraft_idlc_command} "${TURBORAFT_CONTROL_SCHEMA}" --lang c --output
                "${TURBORAFT_CONTROL_GENERATED_HEADER}" --source-output
                "${TURBORAFT_CONTROL_GENERATED_SOURCE}"
        DEPENDS "${TURBORAFT_CONTROL_SCHEMA}" "${TURBORAFT_IDLC}"
        COMMENT "Generating TurboRaft control-plane DTOs"
        VERBATIM
)
source_group(
        "Generated\\Control" FILES "${TURBORAFT_CONTROL_GENERATED_HEADER}"
                                   "${TURBORAFT_CONTROL_GENERATED_SOURCE}"
)
add_library(
        turboraft_control_schema OBJECT "${TURBORAFT_CONTROL_GENERATED_HEADER}"
                                        "${TURBORAFT_CONTROL_GENERATED_SOURCE}"
)
target_include_directories(turboraft_control_schema PUBLIC "${TURBORAFT_CONTROL_GENERATED_DIR}")
target_link_libraries(turboraft_control_schema PRIVATE Salts::DataBind)
cmake_config_target(turboraft_control_schema FOLDER "schema")
