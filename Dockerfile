# This Dockerfile is meant for development purposes only, and forms a base
# that demonstrates:
#
# - how to compile the JNI driver from scratch;
# - a base docker image which has the JNI driver preinstalled.

FROM docker.io/amazonlinux:latest

RUN yum update -y \
    && yum install -y java-17-amazon-corretto \
                      java-17-amazon-corretto-devel \
                      ninja-build \
                      gdb \
                      gcc10-c++ \
                      tar \
                      which

# Install fresh cmake, necessary for range-v3
RUN mkdir /download \
    && cd /download \
    && curl -L https://github.com/Kitware/CMake/releases/download/v3.25.0-rc2/cmake-3.25.0-rc2-linux-x86_64.tar.gz | tar -xz \
    && mv cmake-3.25.0-rc2-linux-x86_64/bin/* /usr/local/bin/ \
    && mv cmake-3.25.0-rc2-linux-x86_64/share/* /usr/local/share/ \
    && cmake --version \
    && rm -rf /download

# Install fresh maven, necessary for some modern compiler plugins
RUN mkdir /download \
    && cd /download \
    && curl -L https://dlcdn.apache.org/maven/maven-3/3.8.6/binaries/apache-maven-3.8.6-bin.tar.gz | tar -xz \
    && mv apache-maven-3.8.6/ /usr/local/maven/ \
    && ln -s /usr/local/maven/bin/mvn /usr/local/bin/mvn \
    && mvn -version \
    && rm -rf /download


RUN mkdir /build
WORKDIR /build

ADD CMakeLists.txt /build/
ADD compile_options.cmake /build/
ADD native_jar.cmake /build/
ADD pom.xml /build/
ADD pom-jni.xml /build/
ADD pom-jni-arch.xml /build/
ADD qdb/ /build/qdb/
ADD src/ /build/src/
ADD thirdparty/ /build/thirdparty/

RUN mvn compile

ENV CXX=gcc10-c++
ENV CC=gcc10-cc

RUN mkdir build && \
        cd build && \
        cmake -G Ninja -DCMAKE_BUILD_TYPE=RelWithDebInfo .. && \
        cmake --build . --config RelWithDebInfo && \
        cd ..

RUN mvn package -Dmaven.javadoc.skip=true -DskipTests -Darch=linux-x86_64

RUN mvn install:install-file -f pom-jni.xml \
    && mvn install:install-file -f pom-jni-arch.xml -Darch=linux-x86_64