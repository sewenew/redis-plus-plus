project(redis++)

cmake_minimum_required(VERSION 3.0.0)

set(CMAKE_CXX_FLAGS "-std=c++11 -Wall -W -Werror -fPIC")

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

set(PROJECT_SOURCE_DIR ${PROJECT_SOURCE_DIR}/src/sw/redis++)

file(GLOB PROJECT_SOURCE_FILES "${PROJECT_SOURCE_DIR}/*.cpp")

set(STATIC_LIB static)
set(SHARED_LIB shared)

add_library(${STATIC_LIB} STATIC ${PROJECT_SOURCE_FILES})
add_library(${SHARED_LIB} SHARED ${PROJECT_SOURCE_FILES})

# hiredis dependency
find_path(HIREDIS_HEADER_PATH hiredis)
target_include_directories(${STATIC_LIB} PUBLIC ${HIREDIS_HEADER_PATH})
target_include_directories(${SHARED_LIB} PUBLIC ${HIREDIS_HEADER_PATH})

find_library(HIREDIS_LIB_PATH hiredis)
target_link_libraries(${SHARED_LIB} ${HIREDIS_LIB_PATH})

set_target_properties(${STATIC_LIB} PROPERTIES OUTPUT_NAME ${PROJECT_NAME})
set_target_properties(${SHARED_LIB} PROPERTIES OUTPUT_NAME ${PROJECT_NAME})

set_target_properties(${STATIC_LIB} PROPERTIES CLEAN_DIRECT_OUTPUT 1)
set_target_properties(${SHARED_LIB} PROPERTIES CLEAN_DIRECT_OUTPUT 1)

# Some special headers.
include(CheckIncludeFileCXX)
check_include_file_cxx(string_view HAVE_STRING_VIEW)
check_include_file_cxx(optional HAVE_OPTIONAL)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/config.h.in ${PROJECT_SOURCE_DIR}/config.h)

# Install static lib.
install(TARGETS ${STATIC_LIB}
        ARCHIVE DESTINATION lib)

# Install shared lib.
install(TARGETS ${SHARED_LIB}
        LIBRARY DESTINATION lib)

#Install headers.
set(HEADER_PATH "sw/redis++")
file(GLOB HEADERS "${PROJECT_SOURCE_DIR}/*.h*")
install(FILES ${HEADERS} DESTINATION ${CMAKE_INSTALL_PREFIX}/include/${HEADER_PATH})
