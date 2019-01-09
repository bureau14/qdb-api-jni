if(CMAKE_SIZEOF_VOID_P EQUAL 8)
    set(ARCH "x86_64")
else()
    set(ARCH "x86_32")
endif()

if(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    set(SYSTEM "osx")
else()
    string(TOLOWER "${CMAKE_SYSTEM_NAME}" SYSTEM)
endif()

set(NATIVE_JAR_FILE "${CMAKE_BINARY_DIR}/jni-3.1.0-${SYSTEM}-${ARCH}.jar")

# Quasardb C API
if (WIN32)
    set(QDB_API_DLL "${CMAKE_SOURCE_DIR}/qdb/bin/qdb_api.dll")
else()
    set(QDB_API_DLL "${CMAKE_SOURCE_DIR}/qdb/lib/libqdb_api${CMAKE_SHARED_LIBRARY_SUFFIX}")
endif()

# JAR: qdb_jni.dll -> windows-x86_64.jar
add_custom_command(
    OUTPUT ${NATIVE_JAR_FILE}
    DEPENDS qdb_api_jni
    COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/native/net/quasardb/qdb/jni/${SYSTEM}/${ARCH}
    COMMAND ${CMAKE_COMMAND} -E copy_if_different $<TARGET_FILE:qdb_api_jni> ${CMAKE_BINARY_DIR}/native/net/quasardb/qdb/jni/${SYSTEM}/${ARCH}
    COMMAND ${CMAKE_COMMAND} -E copy_if_different ${QDB_API_DLL} ${CMAKE_BINARY_DIR}/native/net/quasardb/qdb/jni/${SYSTEM}/${ARCH}
    COMMAND jar cvf ${NATIVE_JAR_FILE} -C ${CMAKE_BINARY_DIR}/native/ .
)

add_custom_target(jni-native
    ALL
    DEPENDS ${NATIVE_JAR_FILE}
)
