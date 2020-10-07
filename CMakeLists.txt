project(redis++)

if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    cmake_minimum_required(VERSION 3.0.0)
else()
    cmake_minimum_required(VERSION 2.8.0)
endif()

if (NOT DEFINED REDIS_PLUS_PLUS_CXX_STANDARD)
    set(REDIS_PLUS_PLUS_CXX_STANDARD 11)
endif()

message(STATUS "The CXX standard is c++${REDIS_PLUS_PLUS_CXX_STANDARD}")

if (NOT WIN32)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++${REDIS_PLUS_PLUS_CXX_STANDARD} -Wall -W -Werror")
endif()

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

set(PROJECT_SOURCE_DIR ${PROJECT_SOURCE_DIR}/src/sw/redis++)

file(GLOB PROJECT_SOURCE_FILES "${PROJECT_SOURCE_DIR}/*.cpp")

# TLS support
option(REDIS_PLUS_PLUS_USE_TLS "Build with TLS support" OFF)

if (REDIS_PLUS_PLUS_USE_TLS)
    message(STATUS "Build with TLS support")

    set(TLS_SUB_DIR "${PROJECT_SOURCE_DIR}/tls")
else()
    set(TLS_SUB_DIR "${PROJECT_SOURCE_DIR}/no_tls")
endif()

file(GLOB TLS_SOURCE_FILES "${TLS_SUB_DIR}/*.cpp")

# hiredis dependency
find_path(HIREDIS_HEADER hiredis)
find_library(HIREDIS_LIB hiredis)

# Build static library
option(REDIS_PLUS_PLUS_BUILD_STATIC "Build static library" ON)

if (REDIS_PLUS_PLUS_BUILD_STATIC)
    set(STATIC_LIB redis-plus-plus-static)

    add_library(${STATIC_LIB} STATIC ${PROJECT_SOURCE_FILES} ${TLS_SOURCE_FILES})

    target_include_directories(${STATIC_LIB} PUBLIC ${PROJECT_SOURCE_DIR} ${TLS_SUB_DIR})
    target_include_directories(${STATIC_LIB} PUBLIC ${HIREDIS_HEADER})

    if (WIN32)
        target_compile_definitions(${STATIC_LIB} PRIVATE NOMINMAX)
        set_target_properties(${STATIC_LIB} PROPERTIES CXX_STANDARD ${REDIS_PLUS_PLUS_CXX_STANDARD} CXX_EXTENSIONS OFF)
        set_target_properties(${STATIC_LIB} PROPERTIES OUTPUT_NAME ${PROJECT_NAME}_static)
    else()
        set_target_properties(${STATIC_LIB} PROPERTIES OUTPUT_NAME ${PROJECT_NAME})
    endif()

    set_target_properties(${STATIC_LIB} PROPERTIES CLEAN_DIRECT_OUTPUT 1)
    set_target_properties(${STATIC_LIB} PROPERTIES POSITION_INDEPENDENT_CODE ON)
endif()

# Build shared library
option(REDIS_PLUS_PLUS_BUILD_SHARED "Build shared library" ON)

if (REDIS_PLUS_PLUS_BUILD_SHARED)
    set(SHARED_LIB redis-plus-plus-shared)

    add_library(${SHARED_LIB} SHARED ${PROJECT_SOURCE_FILES} ${TLS_SOURCE_FILES})

    target_include_directories(${SHARED_LIB} PUBLIC ${PROJECT_SOURCE_DIR} ${TLS_SUB_DIR})
    target_include_directories(${SHARED_LIB} PUBLIC ${HIREDIS_HEADER})

    if (WIN32)
        target_compile_definitions(${SHARED_LIB} PRIVATE NOMINMAX)
        set_target_properties(${SHARED_LIB} PROPERTIES CXX_STANDARD ${REDIS_PLUS_PLUS_CXX_STANDARD} CXX_EXTENSIONS OFF)
        set_target_properties(${SHARED_LIB} PROPERTIES WINDOWS_EXPORT_ALL_SYMBOLS TRUE)
    endif()

    target_link_libraries(${SHARED_LIB} ${HIREDIS_LIB})

    if (REDIS_PLUS_PLUS_USE_TLS)
        find_library(HIREDIS_TLS_LIB hiredis_ssl)
        target_link_libraries(${SHARED_LIB} ${HIREDIS_TLS_LIB})
    endif()

    set_target_properties(${SHARED_LIB} PROPERTIES OUTPUT_NAME ${PROJECT_NAME})
    set_target_properties(${SHARED_LIB} PROPERTIES CLEAN_DIRECT_OUTPUT 1)
    set_target_properties(${SHARED_LIB} PROPERTIES POSITION_INDEPENDENT_CODE ON)
endif()

option(REDIS_PLUS_PLUS_BUILD_TEST "Build tests for redis++" ON)

if (REDIS_PLUS_PLUS_BUILD_TEST)
    add_subdirectory(test)
endif()

# Install static lib.
if (REDIS_PLUS_PLUS_BUILD_STATIC)
    install(TARGETS ${STATIC_LIB}
            ARCHIVE DESTINATION lib)
endif()

# Install shared lib.
if (REDIS_PLUS_PLUS_BUILD_SHARED)
    if (WIN32)
        install(TARGETS ${SHARED_LIB}
                RUNTIME DESTINATION bin
                ARCHIVE DESTINATION lib)
    else()
        install(TARGETS ${SHARED_LIB}
                LIBRARY DESTINATION lib)
    endif()
endif()

# Install headers.
set(HEADER_PATH "sw/redis++")
file(GLOB HEADERS "${PROJECT_SOURCE_DIR}/*.h*" "${TLS_SUB_DIR}/*.h")
install(FILES ${HEADERS} DESTINATION ${CMAKE_INSTALL_PREFIX}/include/${HEADER_PATH})
