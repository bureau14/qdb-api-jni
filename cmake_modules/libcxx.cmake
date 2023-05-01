# NOTES(leon)
#
# We explicitly search for the static variants if possible. This is almost always what we want in production, and/or
# what you want if you install your standard libraries in custom directories (e.g. /usr/local/clang16/...
# or /usr/local/gcc9/...)
#
# The trick here is to specify an additional, explicit search path, and instead of linking against `-lc++` or something,
# explicitly link against `-l:libc++.a`. This is mainly to work around an issue with clang not actually doing any static
# linking at all when you provide `-static-libstdc++ -static-libgcc`. Why that is: it's complicated, but just doing a
# quick google search for 'clang static link libc++' should give you a clue that half the c++ developing world has these
# issues.

if (NOT QDB_SHARED_STDLIB)
  if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    message(STATUS "Clang detected, enabling dark magic wizardry to statically link libc++.")
    add_link_options(
      -nostdlib++
      -l:libc++abi.a
      -l:libc++.a

      # NOTE(leon):
      # If we do not link directly with libunwind, clang attemps to link with gcc_s. I think
      # it will still invoke `--as-needed -lgcc -lgcc_s --no-as-needed` in the linker, but if
      # we explicitly link against libunwind.a, the `as-needed` decides it is, in fact, not needed.
      #
      # It isn't a big drama if we end up keeping libgcc_s.so as a runtime dependency, as this is
      # present by freebsd by default, but this strategy brings it in line with `-static-libgcc`
      # that we also use on GCC.
      -l:libunwind.a
    )
  elseif (CMAKE_CXX_COMPILER_ID MATCHES "GNU")
    message(STATUS "GCC detected, which has working -static-libsdtc++, enabling that.")
    add_link_options(
      -static-libgcc
      -static-libstdc++)
  endif()
endif()
