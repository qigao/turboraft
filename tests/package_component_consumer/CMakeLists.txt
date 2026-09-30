cmake_minimum_required(VERSION 3.24)
project(turboraft_component_package_consumer C)

if(NOT DEFINED TURBORAFT_COMPONENT OR TURBORAFT_COMPONENT STREQUAL "")
  message(FATAL_ERROR "TURBORAFT_COMPONENT is required")
endif()
if(NOT DEFINED ENV{TURBORAFT_ROOT} OR "$ENV{TURBORAFT_ROOT}" STREQUAL "" OR
   NOT IS_DIRECTORY "$ENV{TURBORAFT_ROOT}")
  message(FATAL_ERROR "TURBORAFT_ROOT must name the installed TurboRaft prefix")
endif()

file(READ "${CMAKE_CURRENT_LIST_DIR}/components.json" _components_json)
string(JSON _component_count LENGTH "${_components_json}" components)
set(_component_header "")
if(_component_count GREATER 0)
  math(EXPR _component_last "${_component_count} - 1")
  foreach(_component_index RANGE 0 ${_component_last})
    string(JSON _candidate_name
           GET "${_components_json}" components ${_component_index} name)
    if(_candidate_name STREQUAL TURBORAFT_COMPONENT)
      string(JSON _component_header
             GET "${_components_json}" components ${_component_index} header)
      break()
    endif()
  endforeach()
endif()
if(_component_header STREQUAL "")
  message(FATAL_ERROR "Unsupported TURBORAFT_COMPONENT: ${TURBORAFT_COMPONENT}")
endif()

file(TO_CMAKE_PATH "$ENV{TURBORAFT_ROOT}" TURBORAFT_PACKAGE_ROOT)
find_package(TurboRaft CONFIG REQUIRED COMPONENTS ${TURBORAFT_COMPONENT}
             PATHS "${TURBORAFT_PACKAGE_ROOT}" NO_DEFAULT_PATH)

if(NOT TARGET TurboRaft::${TURBORAFT_COMPONENT})
  message(FATAL_ERROR
          "TurboRaft::${TURBORAFT_COMPONENT} was not exported by the installed SDK")
endif()

file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/component_probe_header.h"
     "#include <${_component_header}>\n")

add_executable(turboraft_component_package_consumer main.c)
target_include_directories(
  turboraft_component_package_consumer
  PRIVATE "${CMAKE_CURRENT_BINARY_DIR}")
target_link_libraries(
  turboraft_component_package_consumer
  PRIVATE TurboRaft::${TURBORAFT_COMPONENT})
