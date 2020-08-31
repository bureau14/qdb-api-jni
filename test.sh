#!/usr/bin/env bash

# This is just a utility file for local development, it is not intended for
# any production/CI usage.

# Generate JNI header files if this is first run
[ ! -d "target/" ] && mvn compile

mkdir build || true && \
    cd build && \
    cmake -G Ninja .. && \
    cmake --build . && \
    cd .. && \
    # mvn -X test
    # mvn  -X '-Dtest=WriterExtraTablesBenchmarkTest*' test
    # mvn   -X  '-Dtest=TableTest*' test
    mvn -X '-Dtest=WriterTest' test
