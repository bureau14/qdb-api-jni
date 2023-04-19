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

set(NATIVE_JAR_FILE "${CMAKE_SOURCE_DIR}/target/jni-3.15.0-SNAPSHOT-${SYSTEM}-${ARCH}.jar")

# Quasardb C API
if (WIN32)
    set(QDB_API_DLL "${CMAKE_SOURCE_DIR}/qdb/bin/${CMAKE_SHARED_LIBRARY_PREFIX}qdb_api${CMAKE_SHARED_LIBRARY_SUFFIX}")
else()
    set(QDB_API_DLL "${CMAKE_SOURCE_DIR}/qdb/lib/${CMAKE_SHARED_LIBRARY_PREFIX}qdb_api${CMAKE_SHARED_LIBRARY_SUFFIX}")
endif()

# Copy libc++ dependencies from C API into JNI .jar file.
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  find_library(LIBCPP
    NAMES "c++.1" "c++"
    REQUIRED
    HINTS qdb/lib /usr/local/lib /usr/lib /lib /usr/local/lib64 /usr/lib64 /lib64)
  find_library(LIBCPPABI
    NAMES "c++abi.1" "c++abi"
    REQUIRED
    HINTS qdb/lib /usr/local/lib /usr/lib /lib /usr/local/lib64 /usr/lib64 /lib64)
  message(STATUS "libc++: ${LIBCPP}")
  message(STATUS "libc++abi: ${LIBCPPABI}")
endif()

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU")
  find_library(LIBSTDCPP
    NAMES "stdc++" "stdc++.6" "libstdc++.so.6"
    REQUIRED
    HINTS qdb/lib /usr/local/lib /usr/lib /lib /usr/local/lib64 /usr/lib64 /lib64)
endif()

message(STATUS "libstdc++: ${LIBSTDCPP}")

set(NATIVE_DIR "${CMAKE_BINARY_DIR}/native/net/quasardb/qdb/jni/${SYSTEM}/${ARCH}")

# JAR: qdb_jni.dll -> windows-x86_64.jar
add_custom_command(
    OUTPUT  ${NATIVE_JAR_FILE}
    DEPENDS qdb_api_jni
    COMMAND ${CMAKE_COMMAND} -E make_directory    ${NATIVE_DIR}
    COMMAND ${CMAKE_COMMAND} -E copy_if_different $<TARGET_FILE:qdb_api_jni> ${NATIVE_DIR}
    COMMAND ${CMAKE_COMMAND} -E copy_if_different ${QDB_API_DLL}             ${NATIVE_DIR}
)

if(LIBCPP)
    add_custom_command(
        OUTPUT  ${NATIVE_JAR_FILE}
        COMMAND ${CMAKE_COMMAND} -E copy_if_different ${LIBCPP}    ${NATIVE_DIR}
        APPEND
    )
endif()

if(LIBCPPABI)
    add_custom_command(
        OUTPUT  ${NATIVE_JAR_FILE}
        COMMAND ${CMAKE_COMMAND} -E copy_if_different ${LIBCPPABI} ${NATIVE_DIR}
        APPEND
    )
endif()

if(LIBSTDCPP)
    add_custom_command(
        OUTPUT  ${NATIVE_JAR_FILE}
        COMMAND ${CMAKE_COMMAND} -E copy_if_different ${LIBSTDCPP} ${NATIVE_DIR}
        APPEND
    )
endif()

add_custom_command(
    OUTPUT  ${NATIVE_JAR_FILE}
    COMMAND jar cvf ${NATIVE_JAR_FILE} -C ${CMAKE_BINARY_DIR}/native/ .
    APPEND
)

add_custom_target(jni-native
    ALL
    DEPENDS ${NATIVE_JAR_FILE}
)
