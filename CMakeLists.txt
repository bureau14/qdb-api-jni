cmake_minimum_required(VERSION 3.4)
project(qdb-api-jni)

find_package(Java REQUIRED)
find_package(JNI REQUIRED)

include(UseJava)
include(compile_options.cmake)

file(GLOB QDB_JAVA_SOURCES
  "*.java"
  "tools/*.java"
  "src/main/net/quasardb/qdb/exceptions/connection/*.java"
  "src/main/net/quasardb/qdb/exceptions/input/*.java"
  "src/main/net/quasardb/qdb/exceptions/operation/*.java"
  "src/main/net/quasardb/qdb/exceptions/protocol/*.java"
  "src/main/net/quasardb/qdb/exceptions/system/*.java"
  "src/main/net/quasardb/qdb/exceptions/system/remote/*.java"
  "src/main/net/quasardb/qdb/exceptions/system/local/*.java"
  "src/main/net/quasardb/qdb/exceptions/*.java"
  "src/main/net/quasardb/qdb/timeseries/*.java"
  "src/main/net/quasardb/qdb/*.java")

set(QDB_JNI_CLASSES
    net.quasardb.qdb.jni.qdb
)

set(QDB_JNI_SOURCES
    helpers.cpp
    ts_helpers.cpp
    qdb_batch.cpp
    qdb_blob.cpp
    qdb_client.cpp
    qdb_deque.cpp
    qdb_hset.cpp
    qdb_int.cpp
    qdb_stream.cpp
    qdb_tag.cpp
    qdb_node.cpp
    qdb_ts.cpp
)

add_jar(jni
  ${QDB_JAVA_SOURCES})

create_javah(TARGET qdb_api_jni_headers
    CLASSES ${QDB_JNI_CLASSES}
    DEPENDS jni
    CLASSPATH jni
)

add_library(qdb_api_jni SHARED
    ${QDB_JNI_SOURCES}
)

add_dependencies(qdb_api_jni
    qdb_api_jni_headers
)

target_include_directories(qdb_api_jni
    PRIVATE
    qdb/include
    ${JNI_INCLUDE_DIRS}
    ${CMAKE_BINARY_DIR}
)

find_library(QDB_API
    NAMES qdb_api
    PATHS qdb/lib
)

link_directories(qdb/lib)
target_link_libraries(qdb_api_jni
    ${QDB_API}
    ${JAVA_JVM_LIBRARY}
)

include(native_jar.cmake)
include(sources_jar.cmake)
