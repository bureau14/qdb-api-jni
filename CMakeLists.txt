cmake_minimum_required(VERSION 3.2.0)
project(qdb-api-jni)

set(CLASS_JAR_FILE "${CMAKE_BINARY_DIR}/jni.jar")
set(SOURCE_JAR_FILE "${CMAKE_BINARY_DIR}/jni-sources.jar")

if(CMAKE_SIZEOF_VOID_P EQUAL 8)
    set(ARCH "x64")
else()
    set(ARCH "x86")
endif()

if(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
    set(SYSTEM "macosx")
else()
    string(TOLOWER "${CMAKE_SYSTEM_NAME}" SYSTEM)
endif()

set(DLL_JAR_FILE "${CMAKE_BINARY_DIR}/jni-${SYSTEM}-${ARCH}.jar")

# macros

macro(add_link_options)
    foreach(OPTION ${ARGV})
        set(CMAKE_EXE_LINKER_FLAGS    "${CMAKE_EXE_LINKER_FLAGS} ${OPTION}")
        set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} ${OPTION}")
        set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} ${OPTION}")
    endforeach()
endmacro()

macro(add_link_options_release)
    foreach(OPTION ${ARGV})
        set(CMAKE_EXE_LINKER_FLAGS_RELEASE    "${CMAKE_EXE_LINKER_FLAGS_RELEASE} ${OPTION}")
        set(CMAKE_MODULE_LINKER_FLAGS_RELEASE "${CMAKE_MODULE_LINKER_FLAGS_RELEASE} ${OPTION}")
        set(CMAKE_SHARED_LINKER_FLAGS_RELEASE "${CMAKE_SHARED_LINKER_FLAGS_RELEASE} ${OPTION}")
    endforeach()
endmacro()

macro(add_link_options_relwithdebinfo)
    foreach(OPTION ${ARGV})
        set(CMAKE_EXE_LINKER_FLAGS_RELWITHDEBINFO    "${CMAKE_EXE_LINKER_FLAGS_RELWITHDEBINFO} ${OPTION}")
        set(CMAKE_MODULE_LINKER_FLAGS_RELWITHDEBINFO "${CMAKE_MODULE_LINKER_FLAGS_RELWITHDEBINFO} ${OPTION}")
        set(CMAKE_SHARED_LINKER_FLAGS_RELWITHDEBINFO "${CMAKE_SHARED_LINKER_FLAGS_RELWITHDEBINFO} ${OPTION}")
    endforeach()
endmacro()

# SWIG
find_package(SWIG REQUIRED)
include(${SWIG_USE_FILE})
set(CMAKE_SWIG_FLAGS -package net.quasardb.qdb.jni)
set(CMAKE_SWIG_OUTDIR jni)

# JNI
find_package(JNI REQUIRED)
include_directories(${JNI_INCLUDE_DIRS})

# Quasardb C API
include_directories(qdb/include)
link_directories(qdb/lib)
if (WIN32)
    set(QDB_API_DLL "${CMAKE_SOURCE_DIR}/qdb/bin/qdb_api.dll")
else()
    set(QDB_API_DLL "${CMAKE_SOURCE_DIR}/qdb/lib/libqdb_api${CMAKE_SHARED_LIBRARY_SUFFIX}")
endif()

# Extra resource files on Windows
if(WIN32 AND MSVC)
    set(RESOURCE_FILES
        resource.h
        targetver.h
        qdb_api_jni.rc
    )
endif()

# Compilation flags
IF (MSVC)
    add_compile_options(
        /Gy           # Allows the compiler to package individual functions in the form of packaged functions (COMDATs).
        /Zc:wchar_t   # Parse wchar_t as a built-in type according to the C++ standard
        /EHa          # The exception-handling model that catches both asynchronous (structured) and synchronous (C++) exceptions.
        /GR           # Enable Run-Time Type Information
        /GF           # Eliminate Duplicate Strings

        $<$<OR:$<CONFIG:Debug>,$<CONFIG:RelWithDebInfo>>:/Zi> # Produces a program database (PDB) that contains type information and symbolic debugging information for use with the debugger
        $<$<NOT:$<CONFIG:Debug>>:/Ox>  # selects full optimization.
        $<$<NOT:$<CONFIG:Debug>>:/Ob2> # Expands functions marked as inline or __inline and any other function that the compiler chooses
        $<$<NOT:$<CONFIG:Debug>>:/Oi>  # Replaces some function calls with intrinsic or otherwise special forms of the function that help your application run faster.
        $<$<NOT:$<CONFIG:Debug>>:/Ot>  # Maximizes the speed of EXEs and DLLs by instructing the compiler to favor speed over size.
        $<$<NOT:$<CONFIG:Debug>>:/Oy>  # Suppresses creation of frame pointers on the call stack.
        $<$<NOT:$<CONFIG:Debug>>:/GS->  # Suppresses Buffer Security Check

        $<$<CONFIG:Debug>:/Ob0>        # Disables inline expansion, which is on by default.
        $<$<CONFIG:Debug>:/Od>         # Turns off all optimizations in the program and speeds compilation.
        $<$<CONFIG:Debug>:/RTC1>       # Enable the run-time error checks feature, in conjunction with the runtime_checks pragma.
        /MT$<$<CONFIG:Debug>:d>        # /MT : Causes the application to use the multithread, static version of the run-time library.
                                       # /MTd: Defines _DEBUG and _MT. This option also causes the compiler to place the library name LIBCMTD.lib into the .obj file so that the linker will use LIBCMTD.lib to resolve external symbols.
    )
endif()

if(CMAKE_CXX_COMPILER_ID MATCHES "(GNU|Clang)")
    add_compile_options(
        -fPIC
        -std=c++11
        -Wno-strict-aliasing
        -Wno-sign-compare
    )
endif()

# link flags

if(MSVC)

    add_link_options(
        /NXCOMPAT
        /FUNCTIONPADMIN
        /INCREMENTAL:NO
    )

    add_link_options_release(
        /OPT:REF
        /RELEASE
        /DYNAMICBASE
    )

    add_link_options_relwithdebinfo(
        /DEBUG
        /PROFILE
        /DYNAMICBASE:NO
        /INCREMENTAL:NO
    )
endif()

if(CLANG)

    if(NOT CMAKE_SYSTEM_NAME MATCHES "FreeBSD")
        add_link_options(
            -lc++
            -lc++abi
        )
    endif()
endif()

if(CLANG AND NOT APPLE)
    add_link_options(
        -Qunused-arguments
        -Wl,--gc-sections
    )

    add_link_options_release(
        -Wl,-s # strip symbol because we can't print a stacktrace with Clang
    )
endif()

if(CMAKE_COMPILER_IS_GNUCXX)
    add_link_options(
        -static-libgcc
        -static-libstdc++
        -Wl,-s
        -Wl,--gc-sections # remove dead code
    )
endif()

set(SOURCES
    qdb_api_batch.i
    qdb_api_blob.i
    qdb_api_deque.i
    qdb_api_expiry.i
    qdb_api_hset.i
    qdb_api_integer.i
    qdb_api_iterator.i
    qdb_api_jni.i
    qdb_api_node.i
    qdb_api_stream.i
    qdb_api_tag.i
    qdb_enum.i
    qdb_struct.i
)

set_source_files_properties(${SOURCES} PROPERTIES CPLUSPLUS ON)
swig_add_module(qdb_api_jni java
    qdb_api_jni.i
    ${RESOURCE_FILES}
)
swig_link_libraries(qdb_api_jni qdb_api ${JAVA_JVM_LIBRARY})

add_custom_command(
    OUTPUT ${CLASS_JAR_FILE}
    DEPENDS qdb_api_jni
    COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/class-jar
    COMMAND javac ${CMAKE_BINARY_DIR}/${CMAKE_SWIG_OUTDIR}/*.java -d ${CMAKE_BINARY_DIR}/class-jar
    COMMAND jar cvf ${CLASS_JAR_FILE} -C ${CMAKE_BINARY_DIR}/class-jar .
)

add_custom_command(
    OUTPUT ${DLL_JAR_FILE}
    DEPENDS qdb_api_jni
    COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/dll-jar/net/quasardb/qdb/jni/${SYSTEM}/${ARCH}
    COMMAND ${CMAKE_COMMAND} -E copy_if_different $<TARGET_FILE:qdb_api_jni> ${CMAKE_BINARY_DIR}/dll-jar/net/quasardb/qdb/jni/${SYSTEM}/${ARCH}
    COMMAND ${CMAKE_COMMAND} -E copy_if_different ${QDB_API_DLL} ${CMAKE_BINARY_DIR}/dll-jar/net/quasardb/qdb/jni/${SYSTEM}/${ARCH}
    COMMAND jar cvf ${DLL_JAR_FILE} -C ${CMAKE_BINARY_DIR}/dll-jar/ .
)

add_custom_command(
    OUTPUT ${SOURCE_JAR_FILE}
    COMMAND jar cvf ${SOURCE_JAR_FILE} ${SOURCES}
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

add_custom_target(JAR
    ALL
    DEPENDS ${CLASS_JAR_FILE} ${DLL_JAR_FILE} ${SOURCE_JAR_FILE}
)