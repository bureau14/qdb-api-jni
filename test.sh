#!/usr/bin/env bash

# Generate JNI header files if this is first run
[ ! -d "target/" ] && mvn compile

mkdir build || true && \
    cd build && \
    cmake -G Ninja .. && \
    cmake --build . && \
    cd .. && \
    #mvn  '-Dtest=WriterExtraTablesBenchmarkTest*' test
    # mvn  '-Dtest=TableTest*' test
    mvn '-Dtest=WriterTest#canInsertNullRows' test
