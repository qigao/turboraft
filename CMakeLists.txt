cmake_minimum_required(VERSION 3.23)

project(
        TurboRaft
        VERSION 0.3.1
        DESCRIPTION "Raft consensus library for the Salts ecosystem"
        HOMEPAGE_URL "https://github.com/qigao/turboraft"
        LANGUAGES C
)

include(CMakePackageConfigHelpers)
include(GNUInstallDirs)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_COLOR_DIAGNOSTICS ON)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")

if(MSVC)
        add_compile_options(/experimental:c11atomics)
endif()

set_property(GLOBAL PROPERTY USE_FOLDERS ON)
set_property(GLOBAL PROPERTY PREDEFINED_TARGETS_FOLDER "cmake")

include(CMakeOptions.cmake)
include(cmake/Sanitizers.cmake)
include(cmake/CmakeUtils.cmake)

include(cmake/FindTools.cmake)

if(BUILD_TESTS OR BUILD_BENCHMARKS)
        enable_testing()
endif()

# Split SDK versions and roots are selected by the loaded parent environment. CMake does not load
# .env or substitute a different SDK generation.
find_package(
        SaltsBase "$ENV{SALTS_BASE_VERSION}" EXACT CONFIG REQUIRED PATHS "$ENV{SALTS_BASE_ROOT}"
        NO_DEFAULT_PATH NO_CMAKE_FIND_ROOT_PATH
)
find_package(
        CMeta "$ENV{CMETA_VERSION}" EXACT CONFIG REQUIRED PATHS "$ENV{CMETA_ROOT}" NO_DEFAULT_PATH
        NO_CMAKE_FIND_ROOT_PATH
)
find_package(
        SaltsUtils "$ENV{SALTS_UTILS_VERSION}" EXACT CONFIG REQUIRED PATHS "$ENV{SALTS_UTILS_ROOT}"
        NO_DEFAULT_PATH NO_CMAKE_FIND_ROOT_PATH
)

find_package(
        SaltsCNet "$ENV{SALTS_CNET_VERSION}" EXACT CONFIG REQUIRED PATHS "$ENV{SALTS_CNET_ROOT}"
        NO_DEFAULT_PATH NO_CMAKE_FIND_ROOT_PATH
)

find_package(
        Chttp "$ENV{CHTTP_VERSION}" EXACT CONFIG REQUIRED PATHS "$ENV{CHTTP_ROOT}"
        NO_DEFAULT_PATH NO_CMAKE_FIND_ROOT_PATH
)
find_package(xxHash CONFIG REQUIRED)

add_subdirectory(schema)
add_subdirectory(raft)
