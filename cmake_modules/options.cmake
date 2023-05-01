option(QDB_CPU_ARCHITECTURE_CORE2 "On x86, use Core2 microarchitecture (limit optimizations)" OFF)
option(QDB_SHARED_STDLIB "Use dynamically linked standard C++ library." OFF)

if(APPLE AND NOT QDB_SHARED_STDLIB)
    message(
        AUTHOR_WARNING
            "Building on Apple systems (e.g. macOS) requires the use of shared run-time libraries (i.e. it implies QDB_SHARED_STDLIB)"
    )
    set(QDB_SHARED_STDLIB ON)
endif()
