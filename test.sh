#!/usr/bin/env bash

# This is just a utility file for local development, it is not intended for
# any production/CI usage.

# Generate JNI header files if this is first run
[ ! -d "target/" ] && mvn compile

if [ ! -d "build/" ]
then
    echo "Configuring JNI"
    mkdir build && \
        cd build && \
        cmake -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo .. \
        && cd ..
fi

echo "Building JNI"
cd build && \
    cmake --build . --config RelWithDebInfo && \
    cd ..

if [ "$1" == "bench" ]
then
    echo "Installing JNI"
    mvn package -DskipTests -Darch=linux-x86_64

    mvn install:install-file -f pom-jni.xml
    mvn install:install-file -f pom-jni-arch.xml -Darch=linux-x86_64

    cd benchmark && \
        mvn package -DskipTests -Darch=linux-x86_64

    echo "Running benchmark"
    # exec valgrind java -Djava.compiler=NONE -XX:UseSSE=0 -jar target/benchmarks.jar
    exec java -jar target/benchmarks.jar

else
    echo "Running tests"

    TESTPARAM=""

    if [ ! "$2" == "" ]
    then
        TESTPARAM="-Dtest=${2}"
    fi

    exec mvn -X $TESTPARAM test
fi
