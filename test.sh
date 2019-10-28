#!/usr/bin/env bash

rm -rf build && \
    mkdir build && \
    cd build && \
    cmake -G Ninja .. && \
    cmake --build . && \
    cd .. && \
    mvn test
