# Host tools are selected by the environment after vcpkg-cache restores them.
find_program(RE2C_EXECUTABLE re2c PATHS "$ENV{RE2C_ROOT}/bin"
             NO_CACHE NO_DEFAULT_PATH NO_CMAKE_FIND_ROOT_PATH REQUIRED)
find_program(LEMON_EXECUTABLE lemon PATHS "$ENV{LEMON_ROOT}/bin"
             NO_CACHE NO_DEFAULT_PATH NO_CMAKE_FIND_ROOT_PATH REQUIRED)
find_file(LEMPAR lempar.c PATHS "$ENV{LEMON_ROOT}/share/lemon"
          NO_CACHE NO_DEFAULT_PATH NO_CMAKE_FIND_ROOT_PATH REQUIRED)
set(LEMON_DEPENDS "${LEMON_EXECUTABLE}")
add_executable(Lemon::Lemon IMPORTED GLOBAL)
set_target_properties(Lemon::Lemon PROPERTIES IMPORTED_LOCATION "${LEMON_EXECUTABLE}")
