project(test_redis++)

if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    cmake_minimum_required(VERSION 3.0.0)
else()
    cmake_minimum_required(VERSION 2.8.0)
endif()

set(PROJECT_SOURCE_DIR ${PROJECT_SOURCE_DIR}/src/sw/redis++)

file(GLOB PROJECT_SOURCE_FILES "${PROJECT_SOURCE_DIR}/*.cpp")

add_executable(${PROJECT_NAME} ${PROJECT_SOURCE_FILES})

target_link_libraries(${PROJECT_NAME} hiredis)

## solaris socket dependency
IF (CMAKE_SYSTEM_NAME MATCHES "(Solaris|SunOS)" )
    target_link_libraries(${PROJECT_NAME} -lsocket)
ENDIF(CMAKE_SYSTEM_NAME MATCHES "(Solaris|SunOS)" )

find_package(Threads REQUIRED)

target_link_libraries(${PROJECT_NAME} redis-plus-plus-static ${CMAKE_THREAD_LIBS_INIT})
