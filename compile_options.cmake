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

# Compilation flags
IF (MSVC)
    add_compile_options(
        /Gy           # Allows the compiler to package individual functions in the form of packaged functions (COMDATs).
        /Zc:wchar_t   # Parse wchar_t as a built-in type according to the C++ standard
        /EHa          # The exception-handling model that catches both asynchronous (structured) and synchronous (C++) exceptions.
        /GR           # Enable Run-Time Type Information
        /GF           # Eliminate Duplicate Strings
        /W4

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
        -Wall
        -Werror
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
