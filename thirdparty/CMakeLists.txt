add_subdirectory(range-v3)

if(APPLE)
    # Apple's libc++ SDK no longer provides the private visibility macros used by meta's forward declarations.
    target_compile_definitions(range-v3-meta INTERFACE META_NO_STD_FORWARD_DECLARATIONS)
endif()
