# quasardb Java JNI API

This API is required by the quasardb Java API. It serves as a high-performance bridge between the C API and the Java API.

## Installation

### quasardb C API

To build the Python API, you will need the C API. It can either be installed on the machine (e.g. on unix in /usr/lib or /usr/local/lib) or you can unpack the C API archive in qdb.

### Building the extension

You will need [CMake](http://www.cmake.org/), [SWIG](http://www.swig.org/) and the Java SDK installed. You can also download a pre-compiled package from our download site.

First, run cmake to create a project directory, for example:

```
    mkdir build
    cd build
    cmake -G "your generator" ..
```

Depending on the generator you chose, you will then either have to run make or open the solution with your editor (e.g. Visual Studio).

For example on UNIX:

```
    mkdir build
    cd build
    cmake -G "Unix Makefiles" ..
    make
```
