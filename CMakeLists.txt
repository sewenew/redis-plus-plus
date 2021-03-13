cmake_minimum_required(VERSION 3.1)

set(REDIS_PLUS_PLUS_VERSION "1.2.3")
message(STATUS "redis-plus-plus version: ${REDIS_PLUS_PLUS_VERSION}")

project(redis++ LANGUAGES CXX VERSION ${REDIS_PLUS_PLUS_VERSION})

set(REDIS_PLUS_PLUS_DEFAULT_BUILD_TYPE "Release")
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE ${REDIS_PLUS_PLUS_DEFAULT_BUILD_TYPE} CACHE STRING "Set build type" FORCE)
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release" "RelWithDebInfo" "MinSizeRel")
endif()
message(STATUS "redis-plus-plus build type: ${CMAKE_BUILD_TYPE}")

set(REDIS_PLUS_PLUS_DEFAULT_CXX_STANDARD 11)
if(NOT REDIS_PLUS_PLUS_CXX_STANDARD)
    set(REDIS_PLUS_PLUS_CXX_STANDARD ${REDIS_PLUS_PLUS_DEFAULT_CXX_STANDARD} CACHE STRING "Set CXX standard" FORCE)
    set_property(CACHE REDIS_PLUS_PLUS_CXX_STANDARD PROPERTY STRINGS "11" "14" "17" "20")
endif()
message(STATUS "redis-plus-plus build with CXX standard: c++${REDIS_PLUS_PLUS_CXX_STANDARD}")

if(NOT WIN32)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++${REDIS_PLUS_PLUS_CXX_STANDARD}")
endif()

set(REDIS_PLUS_PLUS_SOURCE_DIR src/sw/redis++)

set(REDIS_PLUS_PLUS_SOURCES
        "${REDIS_PLUS_PLUS_SOURCE_DIR}/command.cpp"
        "${REDIS_PLUS_PLUS_SOURCE_DIR}/command_options.cpp"
        "${REDIS_PLUS_PLUS_SOURCE_DIR}/connection.cpp"
        "${REDIS_PLUS_PLUS_SOURCE_DIR}/connection_pool.cpp"
        "${REDIS_PLUS_PLUS_SOURCE_DIR}/crc16.cpp"
        "${REDIS_PLUS_PLUS_SOURCE_DIR}/errors.cpp"
        "${REDIS_PLUS_PLUS_SOURCE_DIR}/pipeline.cpp"
        "${REDIS_PLUS_PLUS_SOURCE_DIR}/redis.cpp"
        "${REDIS_PLUS_PLUS_SOURCE_DIR}/redis_cluster.cpp"
        "${REDIS_PLUS_PLUS_SOURCE_DIR}/reply.cpp"
        "${REDIS_PLUS_PLUS_SOURCE_DIR}/sentinel.cpp"
        "${REDIS_PLUS_PLUS_SOURCE_DIR}/shards.cpp"
        "${REDIS_PLUS_PLUS_SOURCE_DIR}/shards_pool.cpp"
        "${REDIS_PLUS_PLUS_SOURCE_DIR}/subscriber.cpp"
        "${REDIS_PLUS_PLUS_SOURCE_DIR}/transaction.cpp"
)

# TLS support
option(REDIS_PLUS_PLUS_USE_TLS "Build with TLS support" OFF)
message(STATUS "redis-plus-plus TLS support: ${REDIS_PLUS_PLUS_USE_TLS}")

if(REDIS_PLUS_PLUS_USE_TLS)
    set(TLS_SUB_DIR "${REDIS_PLUS_PLUS_SOURCE_DIR}/tls")

    list(APPEND REDIS_PLUS_PLUS_SOURCES "${TLS_SUB_DIR}/tls.cpp")

    set(REDIS_PLUS_PLUS_DEPENDS "hiredis,hiredis_ssl")
else()
    set(TLS_SUB_DIR "${REDIS_PLUS_PLUS_SOURCE_DIR}/no_tls")

    set(REDIS_PLUS_PLUS_DEPENDS "hiredis")
endif()

# hiredis dependency
find_package(hiredis QUIET)
if(hiredis_FOUND)
    list(APPEND REDIS_PLUS_PLUS_HIREDIS_LIBS hiredis::hiredis)

    if(REDIS_PLUS_PLUS_USE_TLS)
        find_package(hiredis_ssl REQUIRED)
        list(APPEND REDIS_PLUS_PLUS_HIREDIS_LIBS hiredis::hiredis_ssl)
    endif()
else()
    find_path(HIREDIS_HEADER hiredis REQUIRED)
    find_library(HIREDIS_LIB hiredis REQUIRED)
    list(APPEND REDIS_PLUS_PLUS_HIREDIS_LIBS ${HIREDIS_LIB})

    if(REDIS_PLUS_PLUS_USE_TLS)
        find_library(HIREDIS_TLS_LIB hiredis_ssl REQUIRED)
        list(APPEND REDIS_PLUS_PLUS_HIREDIS_LIBS ${HIREDIS_TLS_LIB})
    endif()
endif()

# Build static library
option(REDIS_PLUS_PLUS_BUILD_STATIC "Build static library" ON)
message(STATUS "redis-plus-plus build static library: ${REDIS_PLUS_PLUS_BUILD_STATIC}")

if(REDIS_PLUS_PLUS_BUILD_STATIC)
    set(STATIC_LIB redis++_static)

    add_library(${STATIC_LIB} STATIC ${REDIS_PLUS_PLUS_SOURCES})

    list(APPEND REDIS_PLUS_PLUS_TARGETS ${STATIC_LIB})

    target_include_directories(${STATIC_LIB} PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/${REDIS_PLUS_PLUS_SOURCE_DIR}>
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/${TLS_SUB_DIR}>
            $<INSTALL_INTERFACE:include>)

    if(hiredis_FOUND)
        target_link_libraries(${STATIC_LIB} PUBLIC ${REDIS_PLUS_PLUS_HIREDIS_LIBS})
    else()
        target_include_directories(${STATIC_LIB} PUBLIC $<BUILD_INTERFACE:${HIREDIS_HEADER}>)
    endif()

    if (WIN32)
        target_compile_definitions(${STATIC_LIB} PRIVATE NOMINMAX)
        set_target_properties(${STATIC_LIB} PROPERTIES CXX_STANDARD ${REDIS_PLUS_PLUS_CXX_STANDARD})
        set_target_properties(${STATIC_LIB} PROPERTIES OUTPUT_NAME redis++_static)
    else()
        target_compile_options(${STATIC_LIB} PRIVATE "-Wall" "-W" "-Werror")
        set_target_properties(${STATIC_LIB} PROPERTIES OUTPUT_NAME redis++)
    endif()

    set_target_properties(${STATIC_LIB} PROPERTIES CLEAN_DIRECT_OUTPUT 1)
    set_target_properties(${STATIC_LIB} PROPERTIES CXX_EXTENSIONS OFF)

    option(REDIS_PLUS_PLUS_BUILD_STATIC_WITH_PIC "Build static library with position independent code" ON)
    message(STATUS "redis-plus-plus build static library with position independent code: ${REDIS_PLUS_PLUS_BUILD_STATIC_WITH_PIC}")

    if(REDIS_PLUS_PLUS_BUILD_STATIC_WITH_PIC)
        set_target_properties(${STATIC_LIB} PROPERTIES POSITION_INDEPENDENT_CODE ON)
    endif()
endif()

# Build shared library
option(REDIS_PLUS_PLUS_BUILD_SHARED "Build shared library" ON)
message(STATUS "redis-plus-plus build shared library: ${REDIS_PLUS_PLUS_BUILD_SHARED}")

if(REDIS_PLUS_PLUS_BUILD_SHARED)
    set(SHARED_LIB redis++)

    add_library(${SHARED_LIB} SHARED ${REDIS_PLUS_PLUS_SOURCES})

    list(APPEND REDIS_PLUS_PLUS_TARGETS ${SHARED_LIB})

    target_include_directories(${SHARED_LIB} PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/${REDIS_PLUS_PLUS_SOURCE_DIR}>
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/${TLS_SUB_DIR}>
            $<INSTALL_INTERFACE:include>)

    if(hiredis_FOUND)
        target_link_libraries(${SHARED_LIB} PUBLIC ${REDIS_PLUS_PLUS_HIREDIS_LIBS})
    else()
        target_include_directories(${SHARED_LIB} PUBLIC $<BUILD_INTERFACE:${HIREDIS_HEADER}>)
        target_link_libraries(${SHARED_LIB} PUBLIC ${REDIS_PLUS_PLUS_HIREDIS_LIBS})
    endif()

    if(WIN32)
        target_compile_definitions(${SHARED_LIB} PRIVATE NOMINMAX)
        set_target_properties(${SHARED_LIB} PROPERTIES CXX_STANDARD ${REDIS_PLUS_PLUS_CXX_STANDARD})
        set_target_properties(${SHARED_LIB} PROPERTIES WINDOWS_EXPORT_ALL_SYMBOLS TRUE)
    else()
        target_compile_options(${SHARED_LIB} PRIVATE "-Wall" "-W" "-Werror")
    endif()

    set_target_properties(${SHARED_LIB} PROPERTIES OUTPUT_NAME redis++)
    set_target_properties(${SHARED_LIB} PROPERTIES CLEAN_DIRECT_OUTPUT 1)
    set_target_properties(${SHARED_LIB} PROPERTIES CXX_EXTENSIONS OFF)
    set_target_properties(${SHARED_LIB} PROPERTIES POSITION_INDEPENDENT_CODE ON)
    set_target_properties(${SHARED_LIB} PROPERTIES VERSION ${PROJECT_VERSION} SOVERSION ${PROJECT_VERSION_MAJOR})
endif()

option(REDIS_PLUS_PLUS_BUILD_TEST "Build tests for redis++" ON)
message(STATUS "redis-plus-plus build test: ${REDIS_PLUS_PLUS_BUILD_TEST}")

if(REDIS_PLUS_PLUS_BUILD_TEST)
    add_subdirectory(test)
endif()

install(TARGETS ${REDIS_PLUS_PLUS_TARGETS}
        EXPORT redis++-targets
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
        RUNTIME DESTINATION bin
        INCLUDES DESTINATION include)

set(REDIS_PLUS_PLUS_CMAKE_DESTINATION share/cmake/redis++)

install(EXPORT redis++-targets
        FILE redis++-targets.cmake
        NAMESPACE redis++::
        DESTINATION ${REDIS_PLUS_PLUS_CMAKE_DESTINATION})

# Install headers.
set(HEADER_PATH "sw/redis++")
file(GLOB HEADERS "${REDIS_PLUS_PLUS_SOURCE_DIR}/*.h*" "${TLS_SUB_DIR}/*.h")
install(FILES ${HEADERS} DESTINATION ${CMAKE_INSTALL_PREFIX}/include/${HEADER_PATH})

include(CMakePackageConfigHelpers)

write_basic_package_version_file("${CMAKE_CURRENT_BINARY_DIR}/cmake/redis++-config-version.cmake"
        VERSION ${PROJECT_VERSION}
        COMPATIBILITY AnyNewerVersion)

configure_package_config_file("${CMAKE_CURRENT_SOURCE_DIR}/cmake/redis++-config.cmake.in"
        "${CMAKE_CURRENT_BINARY_DIR}/cmake/redis++-config.cmake"
        INSTALL_DESTINATION ${REDIS_PLUS_PLUS_CMAKE_DESTINATION})

install(FILES "${CMAKE_CURRENT_BINARY_DIR}/cmake/redis++-config.cmake"
        "${CMAKE_CURRENT_BINARY_DIR}/cmake/redis++-config-version.cmake"
        DESTINATION ${REDIS_PLUS_PLUS_CMAKE_DESTINATION})

export(EXPORT redis++-targets
        FILE "${CMAKE_CURRENT_BINARY_DIR}/cmake/redis++-targets.cmake"
        NAMESPACE redis++::)

configure_file("${CMAKE_CURRENT_SOURCE_DIR}/cmake/redis++.pc.in"
        "${CMAKE_CURRENT_BINARY_DIR}/cmake/redis++.pc" @ONLY)

install(FILES "${CMAKE_CURRENT_BINARY_DIR}/cmake/redis++.pc"
        DESTINATION "lib/pkgconfig")
