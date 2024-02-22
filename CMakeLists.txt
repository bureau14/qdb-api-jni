cmake_minimum_required(VERSION 3.15)
project(qdb-api-jni
  DESCRIPTION "java api for the quasardb timeseries database"
  HOMEPAGE_URL "https://www.quasar.ai")

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_OSX_DEPLOYMENT_TARGET "11.0")
if(APPLE)
    # Robustly get the SDK path using xcrun.
    execute_process(
        COMMAND xcrun --sdk macosx${CMAKE_OSX_DEPLOYMENT_TARGET} --show-sdk-path
        OUTPUT_VARIABLE SDK_PATH
    )
    string(STRIP "${SDK_PATH}" CMAKE_OSX_SYSROOT)
endif()

find_package(Java REQUIRED)
find_package(JNI REQUIRED)

include(UseJava)

include(cmake_modules/options.cmake)
include(cmake_modules/libcxx.cmake)
include(cmake_modules/compile_options.cmake)

set(QDB_JNI_CLASSES
    net.quasardb.qdb.jni.qdb
)

set(QDB_JNI_SOURCES
  src/main/c++/net/quasardb/qdb/jni/byte_buffer.cpp
  src/main/c++/net/quasardb/qdb/jni/debug.cpp
  src/main/c++/net/quasardb/qdb/jni/env.cpp
  src/main/c++/net/quasardb/qdb/jni/exception.cpp
  src/main/c++/net/quasardb/qdb/jni/introspect.cpp
  src/main/c++/net/quasardb/qdb/jni/string.cpp
  src/main/c++/net/quasardb/qdb/jni/vm.cpp
  src/main/c++/net/quasardb/qdb/jni/log.cpp
  src/main/c++/net/quasardb/qdb/jni/load.cpp
  src/main/c++/net/quasardb/qdb/jni/adapt/column.cpp
  src/main/c++/net/quasardb/qdb/jni/adapt/local_table.cpp
  src/main/c++/net/quasardb/qdb/jni/adapt/value.cpp
  src/main/c++/net/quasardb/qdb/jni/export/qdb_batch.cpp
  src/main/c++/net/quasardb/qdb/jni/export/qdb_blob.cpp
  src/main/c++/net/quasardb/qdb/jni/export/qdb_client.cpp
  src/main/c++/net/quasardb/qdb/jni/export/qdb_double.cpp
  src/main/c++/net/quasardb/qdb/jni/export/qdb_int.cpp
  src/main/c++/net/quasardb/qdb/jni/export/qdb_tag.cpp
  src/main/c++/net/quasardb/qdb/jni/export/qdb_node.cpp
  src/main/c++/net/quasardb/qdb/jni/export/qdb_perf.cpp
  src/main/c++/net/quasardb/qdb/jni/export/qdb_query.cpp
  src/main/c++/net/quasardb/qdb/jni/export/qdb_string.cpp
  src/main/c++/net/quasardb/qdb/jni/export/qdb_timestamp.cpp
  src/main/c++/net/quasardb/qdb/jni/export/qdb_ts.cpp
  src/main/c++/net/quasardb/qdb/jni/export/ts/point.cpp
  src/main/c++/net/quasardb/qdb/jni/util/helpers.cpp
  src/main/c++/net/quasardb/qdb/jni/util/ts_helpers.cpp
)

add_library(qdb_api_jni SHARED
    ${QDB_JNI_SOURCES}
)

target_include_directories(qdb_api_jni
    PRIVATE
    qdb/include/
    ${JNI_INCLUDE_DIRS}
    target/headers/
    ${CMAKE_BINARY_DIR}

    thirdparty/range-v3/include/
)

find_library(QDB_API
    NAMES qdb_api
    PATHS qdb/lib
)

link_directories(qdb/lib)
target_link_libraries(qdb_api_jni
    PRIVATE
    ${QDB_API}
    ${JAVA_JVM_LIBRARY}

    range-v3
)

include(cmake_modules/native_jar.cmake)

install(TARGETS qdb_api_jni
        DESTINATION lib)

install(FILES ${NATIVE_JAR_FILE}
        DESTINATION lib)

add_subdirectory(thirdparty)
