cmake_minimum_required(VERSION 3.4)
project(qdb-api-jni)

find_package(Java REQUIRED)
find_package(JNI REQUIRED)

include(UseJava)
include(compile_options.cmake)

file(GLOB QDB_JAVA_SOURCES
  "tools/*.java"
  "src/main/java/net/quasardb/qdb/exception/connection/*.java"
  "src/main/java/net/quasardb/qdb/exception/input/*.java"
  "src/main/java/net/quasardb/qdb/exception/operation/*.java"
  "src/main/java/net/quasardb/qdb/exception/protocol/*.java"
  "src/main/java/net/quasardb/qdb/exception/system/*.java"
  "src/main/java/net/quasardb/qdb/exception/system/remote/*.java"
  "src/main/java/net/quasardb/qdb/exception/system/local/*.java"
  "src/main/java/net/quasardb/qdb/exception/*.java"
  "src/main/java/net/quasardb/qdb/jni/*.java"
  "src/main/java/net/quasardb/qdb/ts/*.java"
  "src/main/java/net/quasardb/qdb/*.java")

set(QDB_JNI_CLASSES
    net.quasardb.qdb.jni.qdb
)

set(QDB_JNI_SOURCES
  src/main/c++/net/quasardb/qdb/jni/byte_buffer.cpp
  src/main/c++/net/quasardb/qdb/jni/debug.cpp
  src/main/c++/net/quasardb/qdb/jni/env.cpp
  src/main/c++/net/quasardb/qdb/jni/introspect.cpp
  src/main/c++/net/quasardb/qdb/jni/string.cpp
  src/main/c++/net/quasardb/qdb/jni/vm.cpp
  src/main/c++/net/quasardb/qdb/jni/ts/qdb_value.cpp
  src/main/c++/net/quasardb/qdb/jni/util/helpers.cpp
  src/main/c++/net/quasardb/qdb/jni/util/ts_helpers.cpp
  src/main/c++/net/quasardb/qdb/jni/export/qdb_batch.cpp
  src/main/c++/net/quasardb/qdb/jni/export/qdb_blob.cpp
  src/main/c++/net/quasardb/qdb/jni/export/qdb_client.cpp
  src/main/c++/net/quasardb/qdb/jni/export/qdb_deque.cpp
  src/main/c++/net/quasardb/qdb/jni/export/qdb_int.cpp
  src/main/c++/net/quasardb/qdb/jni/export/qdb_stream.cpp
  src/main/c++/net/quasardb/qdb/jni/export/qdb_tag.cpp
  src/main/c++/net/quasardb/qdb/jni/export/qdb_node.cpp
  src/main/c++/net/quasardb/qdb/jni/export/qdb_ts.cpp
  src/main/c++/net/quasardb/qdb/jni/export/qdb_query.cpp
)

add_jar(jni
  SOURCES ${QDB_JAVA_SOURCES}
  OUTPUT_NAME jni-2.8.0-SNAPSHOT)

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
