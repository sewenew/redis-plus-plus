project(test_redis++)

if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    cmake_minimum_required(VERSION 3.0.0)
else()
    cmake_minimum_required(VERSION 2.8.0)
endif()

set(PROJECT_SOURCE_DIR ${PROJECT_SOURCE_DIR}/src/sw/redis++)

file(GLOB PROJECT_SOURCE_FILES "${PROJECT_SOURCE_DIR}/*.cpp")

add_executable(${PROJECT_NAME} ${PROJECT_SOURCE_FILES})

# hiredis dependency
find_path(HIREDIS_HEADER hiredis)
target_include_directories(${PROJECT_NAME} PUBLIC ${HIREDIS_HEADER})

find_library(HIREDIS_STATIC_LIB libhiredis.a)
target_link_libraries(${PROJECT_NAME} ${HIREDIS_STATIC_LIB})

if (REDIS_PLUS_PLUS_USE_TLS)
    find_package(OpenSSL REQUIRED)
    find_library(HIREDIS_TLS_STATIC_LIB libhiredis_ssl.a)
    target_link_libraries(${PROJECT_NAME} ${HIREDIS_TLS_STATIC_LIB} ${OPENSSL_LIBRARIES})
endif()

# redis++ dependency
target_include_directories(${PROJECT_NAME} PUBLIC ../src)

# solaris socket dependency
if (CMAKE_SYSTEM_NAME MATCHES "(Solaris|SunOS)")
    target_link_libraries(${PROJECT_NAME} -lsocket)
endif(CMAKE_SYSTEM_NAME MATCHES "(Solaris|SunOS)")

# Windows socket dependency
if (WIN32)
    target_link_libraries(${PROJECT_NAME} ws2_32)
endif()

find_package(Threads REQUIRED)

if (REDIS_PLUS_PLUS_BUILD_STATIC)
    set(REDIS_PLUS_PLUS_LIB redis-plus-plus-static)
else()
    set(REDIS_PLUS_PLUS_LIB redis-plus-plus-shared)
endif()

target_link_libraries(${PROJECT_NAME} ${REDIS_PLUS_PLUS_LIB} ${CMAKE_THREAD_LIBS_INIT})
