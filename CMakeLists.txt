# ATL24 SlideRule Plugin top-level CMake build script
cmake_minimum_required (VERSION 3.13.0) # The minimum CMake version is chosen to enable policy CMP0079
project (ATL24 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 23)
#set(CMAKE_CXX_FLAGS "-Wall -Werror -Wshadow ${CMAKE_CXX_FLAGS}")

# Squelch a warning when building on Win32/Cygwin
set (CMAKE_LEGACY_CYGWIN_WIN32 0)

# Set a default build type if none was specified
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(default_build_type "Debug")
endif()

# ATL24 Plugin #
add_library (atl24 MODULE "")
set_target_properties (atl24 PROPERTIES OUTPUT_NAME atl24)
set_target_properties (atl24 PROPERTIES PREFIX "")

# Prerequisites #
find_package (Lua "5.3")

# Version Information #
execute_process (COMMAND git --work-tree ${PROJECT_SOURCE_DIR} --git-dir ${PROJECT_SOURCE_DIR}/.git describe --abbrev --dirty --always --tags --long OUTPUT_VARIABLE BUILDINFO)
string(REGEX REPLACE "\n$" "" BUILDINFO "${BUILDINFO}")
target_compile_definitions (atl24 PUBLIC BUILDINFO="${BUILDINFO}")
file(STRINGS ${PROJECT_SOURCE_DIR}/version.txt TGTVER)
target_compile_definitions (atl24 PUBLIC BINID="${TGTVER}")

# Set Environment Variables #
set (INSTALLDIR /usr/local CACHE STRING "Installation directory for library and executables")
set (CONFDIR ${INSTALLDIR}/etc/sliderule)
set (PLUGINDIR ${INSTALLDIR}/lib/sliderule)
set (ATL24DIR ${INSTALLDIR} CACHE STRING "Location of ATL24 algorithm header-only library")

# Debug Configuration #
if(CMAKE_BUILD_TYPE MATCHES "Debug")
	target_compile_options (atl24 PUBLIC -fsanitize=address -fno-omit-frame-pointer)
endif()

# Source Files #
target_sources(atl24
    PRIVATE
        ${CMAKE_CURRENT_LIST_DIR}/package/atl24.cpp
        ${CMAKE_CURRENT_LIST_DIR}/package/Atl24Writer.cpp
        ${CMAKE_CURRENT_LIST_DIR}/package/BlunderRunner.cpp
)

# Include Directories #
target_include_directories (atl24
    PUBLIC
        ${INSTALLDIR}/include/sliderule
        ${ATL24DIR}/include
        ${LUA_INCLUDE_DIR}
        $<BUILD_INTERFACE:${CMAKE_CURRENT_LIST_DIR}>/package
)

# Plugin Installation #
install (
    TARGETS
        atl24 LIBRARY
    DESTINATION
        ${PLUGINDIR}
)
install (
    FILES
        ${CMAKE_CURRENT_LIST_DIR}/endpoints/atl24g2.lua
    DESTINATION
        ${CONFDIR}/api
)
