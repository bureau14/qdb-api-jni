# quasardb JNI API

This API is required by the quasardb Java API. It serves as a high-performance bridge between the C API and the Java API.

Binary package can be downloaded from https://download.quasardb.net/quasardb

### Compiling

To compile the JNI API, you will need the C API.
It can either be installed on the machine (e.g. on unix in /usr/lib or /usr/local/lib) or you can unpack the C API archive in `qdb/`.

You will need [CMake](http://www.cmake.org/), [SWIG](http://www.swig.org/) and the Java SDK installed.

On a terminal type the following commands:

    mkdir build
    cd build
    cmake ..
    cmake --build . --config Release

The compiled library location is:

- `build\Release\qdb_api_jni.dll` on Windows
- `build\libqdb_api_jni.so` on Linux and FreeBSD
- `build\libqdb_api_jni.dylib` on Mac OS X
