find_package(Python3 COMPONENTS Interpreter QUIET)

if(Python3_Interpreter_FOUND)
  add_test(
    NAME turboraft.incident_bundle
    COMMAND "${Python3_EXECUTABLE}"
            "${CMAKE_CURRENT_SOURCE_DIR}/test_incident_bundle.py"
            "${PROJECT_SOURCE_DIR}/tools/turboraft_incident_bundle.py")
endif()
