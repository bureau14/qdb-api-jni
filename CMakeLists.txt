cmake_minimum_required (VERSION 2.8.12)
project (qdb-api-jni)

# SWIG
find_package(SWIG REQUIRED)
include(${SWIG_USE_FILE})
set(CMAKE_SWIG_FLAGS -package com.b14.qdb.jni)

# JNI
find_package(JNI REQUIRED)
include_directories(${JNI_INCLUDE_DIRS})

# Quasardb C API
include_directories(qdb/include)
link_directories(qdb/lib)

# CPack
set(CPACK_PACKAGE_VERSION_MAJOR "1")
set(CPACK_PACKAGE_VERSION_MINOR "2")
set(CPACK_PACKAGE_VERSION_PATCH "0")
include(CPack)

# Extra resource files on Windows
if(WIN32)
    set(JAVA_API_RESOURCE
        resource.h
        targetver.h
        qdb_java_api.rc
    )
endif()

# Compilation warnings
if(CMAKE_CXX_COMPILER_ID MATCHES "(GNU|Clang)")
    add_compile_options(
        -Wno-strict-aliasing
        -Wno-sign-compare
    )
endif()

set_source_files_properties(qdb_java.i PROPERTIES CPLUSPLUS ON)
swig_add_module(qdb_java_api java qdb_java.i ${JAVA_API_RESOURCE})
swig_link_libraries(qdb_java_api qdb_api ${JAVA_JVM_LIBRARY})

install(
    DIRECTORY ${SWIG_OUTPUT_DIR}        
    DESTINATION .
    FILES_MATCHING PATTERN "*.java"
)

install(TARGETS qdb_java_api
    RUNTIME DESTINATION bin
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib)
