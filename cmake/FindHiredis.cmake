find_package(hiredis QUIET)
if(hiredis_FOUND)
    list(APPEND REDIS_PLUS_PLUS_HIREDIS_LIBS hiredis::hiredis)

    if(NOT hiredis_INCLUDE_DIRS)
        # This can happen when hiredis is included with FetchContent with OVERRIDE_FIND_PACKAGE
        find_path(
          hiredis_INCLUDE_DIRS
          hiredis.h
          PATHS
            ${CMAKE_BINARY_DIR}/_deps/hiredis
            ${CMAKE_CURRENT_BINARY_DIR}/_deps/hiredis
          NO_CACHE
          REQUIRED
        )

        # Remove the trailing /hiredis from the include path so that we can include hiredis with hiredis/hiredis.h
        get_filename_component(hiredis_INCLUDE_DIRS "${hiredis_INCLUDE_DIRS}" DIRECTORY)
    endif()

    if(REDIS_PLUS_PLUS_USE_TLS)
        find_package(hiredis_ssl REQUIRED)
        list(APPEND REDIS_PLUS_PLUS_HIREDIS_LIBS hiredis::hiredis_ssl)
        find_package(OpenSSL REQUIRED)
        list(APPEND REDIS_PLUS_PLUS_HIREDIS_LIBS ${OPENSSL_LIBRARIES})
    endif()
else()
    find_path(HIREDIS_HEADER hiredis)
    find_library(HIREDIS_LIB hiredis)
    list(APPEND REDIS_PLUS_PLUS_HIREDIS_LIBS ${HIREDIS_LIB})

    if(REDIS_PLUS_PLUS_USE_TLS)
        find_library(HIREDIS_TLS_LIB hiredis_ssl)
        list(APPEND REDIS_PLUS_PLUS_HIREDIS_LIBS ${HIREDIS_TLS_LIB})
        find_package(OpenSSL REQUIRED)
        list(APPEND REDIS_PLUS_PLUS_HIREDIS_LIBS ${OPENSSL_LIBRARIES})
    endif()
endif()