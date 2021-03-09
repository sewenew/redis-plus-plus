project(test_redis++)

cmake_minimum_required(VERSION 3.1)

set(REDIS_PLUS_PLUS_TEST_SOURCES src/sw/redis++/test_main.cpp)

add_executable(${PROJECT_NAME} ${REDIS_PLUS_PLUS_TEST_SOURCES})

# hiredis dependency
find_path(HIREDIS_HEADER hiredis REQUIRED)
target_include_directories(${PROJECT_NAME} PUBLIC ${HIREDIS_HEADER})

find_library(HIREDIS_STATIC_LIB libhiredis.a)
if(NOT HIREDIS_STATIC_LIB)
    find_library(HIREDIS_STATIC_LIB libhiredis_static.a)
endif()
target_link_libraries(${PROJECT_NAME} ${HIREDIS_STATIC_LIB})

if(REDIS_PLUS_PLUS_USE_TLS)
    find_package(OpenSSL REQUIRED)
    find_library(HIREDIS_TLS_STATIC_LIB libhiredis_ssl.a)
    if(NOT HIREDIS_TLS_STATIC_LIB)
        find_library(HIREDIS_TLS_STATIC_LIB libhiredis_ssl_static.a)
    endif()
    target_link_libraries(${PROJECT_NAME} ${HIREDIS_TLS_STATIC_LIB} ${OPENSSL_LIBRARIES})
endif()

# redis++ dependency
target_include_directories(${PROJECT_NAME} PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/../src
        ${CMAKE_CURRENT_SOURCE_DIR}/../${TLS_SUB_DIR})

# solaris socket dependency
if(CMAKE_SYSTEM_NAME MATCHES "(Solaris|SunOS)")
    target_link_libraries(${PROJECT_NAME} -lsocket)
endif(CMAKE_SYSTEM_NAME MATCHES "(Solaris|SunOS)")

# Windows socket dependency
if(WIN32)
    target_link_libraries(${PROJECT_NAME} ws2_32)
endif()

find_package(Threads REQUIRED)

if(REDIS_PLUS_PLUS_BUILD_STATIC)
    set(REDIS_PLUS_PLUS_LIB redis++_static)
else()
    set(REDIS_PLUS_PLUS_LIB redis++)
endif()

target_link_libraries(${PROJECT_NAME} ${REDIS_PLUS_PLUS_LIB} ${CMAKE_THREAD_LIBS_INIT})
