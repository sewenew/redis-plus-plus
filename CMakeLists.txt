project(redis++)

if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    cmake_minimum_required(VERSION 3.0.0)
else()
    cmake_minimum_required(VERSION 2.8.0)
endif()

# Check C++ compiler support
message(STATUS "Checking compiler support...")

include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG(-std=c++17 HAVE_FLAG_STD_CXX17)
if(HAVE_FLAG_STD_CXX17)
    message(STATUS "Supported C++ standard: C++17")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -DM_CPP17_AVAILABLE")
elseif()
    CHECK_CXX_COMPILER_FLAG(-std=c++1z HAVE_FLAG_STD_CXX1Z)
    if(HAVE_FLAG_STD_CXX1Z)
        message(STATUS "Supported C++ standard: C++17 [C++1z]")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1z -DM_CPP17_AVAILABLE")
   else()
        CHECK_CXX_COMPILER_FLAG(-std=c++11 HAVE_FLAG_STD_CXX11)
        if(HAVE_FLAG_STD_CXX11)
            message(STATUS "Supported C++ standard: C++11")
            set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
        else()
            message(FATAL_ERROR "C++11 is NOT supported!")
        endif()
    endif()
endif()

# Set compiler flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -W -Werror -fPIC")
message(STATUS "CMAKE_CXX_FLAGS: ${CMAKE_CXX_FLAGS}")

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

set(PROJECT_SOURCE_DIR ${PROJECT_SOURCE_DIR}/src/sw/redis++)

file(GLOB PROJECT_SOURCE_FILES "${PROJECT_SOURCE_DIR}/*.cpp")

set(STATIC_LIB static)
set(SHARED_LIB shared)

add_library(${STATIC_LIB} STATIC ${PROJECT_SOURCE_FILES})
add_library(${SHARED_LIB} SHARED ${PROJECT_SOURCE_FILES})

# hiredis dependency
find_path(HIREDIS_HEADER hiredis)
target_include_directories(${STATIC_LIB} PUBLIC ${HIREDIS_HEADER})
target_include_directories(${SHARED_LIB} PUBLIC ${HIREDIS_HEADER})

find_library(HIREDIS_LIB hiredis)
target_link_libraries(${SHARED_LIB} ${HIREDIS_LIB})

set_target_properties(${STATIC_LIB} PROPERTIES OUTPUT_NAME ${PROJECT_NAME})
set_target_properties(${SHARED_LIB} PROPERTIES OUTPUT_NAME ${PROJECT_NAME})

set_target_properties(${STATIC_LIB} PROPERTIES CLEAN_DIRECT_OUTPUT 1)
set_target_properties(${SHARED_LIB} PROPERTIES CLEAN_DIRECT_OUTPUT 1)

add_subdirectory(test)

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
