cmake_minimum_required(VERSION 3.4)
project(qdb-api-jni)

find_package(Java REQUIRED)
find_package(JNI REQUIRED)

include(UseJava)
include(compile_options.cmake)

set(QDB_JAVA_SOURCES
    Reference.java
    qdb.java
    qdb_error.java
    qdb_err_origin.java
    qdb_err_severity.java
    qdb_entry_type.java
    qdb_stream_mode.java
    qdb_timespec.java
    qdb_ts_column_type.java
    qdb_ts_column_info.java
    tools/JarFileHelper.java
    tools/NativeLibraryLoader.java
)

set(QDB_JNI_CLASSES
    net.quasardb.qdb.jni.qdb
)

set(QDB_JNI_SOURCES
    helpers.cpp
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
    ${QDB_JAVA_SOURCES}
)

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
