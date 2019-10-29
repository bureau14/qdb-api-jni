#!/usr/bin/env bash

mkdir build || true && \
    cd build && \
    cmake -G Ninja .. && \
    cmake --build . && \
    cd .. && \
    mvn test
