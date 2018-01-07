project(redis++)

cmake_minimum_required(VERSION 2.4.0)

set(CMAKE_CXX_FLAGS "-std=c++11 -Wall -W -Werror -fPIC -O3")

message("compile options: ${CMAKE_CXX_FLAGS}")
 
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

set(PROJECT_SOURCE_DIR ${PROJECT_SOURCE_DIR}/src/sw/redis++)

file(GLOB PROJECT_SOURCE_FILES "${PROJECT_SOURCE_DIR}/*.cpp")

add_library(${PROJECT_NAME} ${PROJECT_SOURCE_FILES})

#target_link_libraries(${PROJECT_NAME} libhiredis.a)

message("install path: ${CMAKE_INSTALL_PREFIX}")

#Install static lib.
install(TARGETS ${PROJECT_NAME}
        ARCHIVE DESTINATION lib)

#Install headers.
set(HEADER_PATH "sw/redis++")
file(GLOB HEADERS "${PROJECT_SOURCE_DIR}/*.h")
install(FILES ${HEADERS} DESTINATION ${CMAKE_INSTALL_PREFIX}/include/${HEADER_PATH})
