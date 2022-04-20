project(test_redis++)

cmake_minimum_required(VERSION 3.1)

set(REDIS_PLUS_PLUS_TEST_SOURCES src/sw/redis++/test_main.cpp)

add_executable(${PROJECT_NAME} ${REDIS_PLUS_PLUS_TEST_SOURCES})

# hiredis dependency
# hiredis dependency
find_package(hiredis REQUIRED)
if(hiredis_FOUND)
    set(HIREDIS_HEADER ${hiredis_INCLUDE_DIRS})
endif()

if(NOT TARGET hiredis::hiredis)
  # Add target to support older hiredis releases
  add_library(hiredis::hiredis ALIAS hiredis)
endif()

find_path(HIREDIS_HEADER hiredis REQUIRED)
target_include_directories(${PROJECT_NAME} PRIVATE $<BUILD_INTERFACE:${HIREDIS_HEADER}>)
target_link_libraries(${PROJECT_NAME} hiredis::hiredis)

if(REDIS_PLUS_PLUS_USE_TLS)
    find_package(OpenSSL REQUIRED)
    find_library(TEST_HIREDIS_TLS_LIB libhiredis_ssl.a)
    if(NOT TEST_HIREDIS_TLS_LIB)
        find_library(TEST_HIREDIS_TLS_LIB libhiredis_ssl_static.a)
        if(NOT TEST_HIREDIS_TLS_LIB)
            find_library(TEST_HIREDIS_TLS_LIB hiredis_ssl)
        endif()
    endif()
    target_link_libraries(${PROJECT_NAME} ${TEST_HIREDIS_TLS_LIB} ${OPENSSL_LIBRARIES})
endif()

# redis++ dependency
target_include_directories(${PROJECT_NAME} PRIVATE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../src>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../${TLS_SUB_DIR}>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../${CXX_UTILS_DIR}>)

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
