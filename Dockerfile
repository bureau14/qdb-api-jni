# This Dockerfile is meant for development purposes only, and forms a base
# that demonstrates:
#
# - how to compile the JNI driver from scratch;
# - a base docker image which has the JNI driver preinstalled.


FROM docker.io/amazonlinux:2 AS base

RUN yum update -y && yum install -y curl gzip tar which


# Stage -- download cmake
FROM base AS cmake

# Install fresh cmake, necessary for range-v3
RUN mkdir /download \
    && cd /download \
    && curl -L https://github.com/Kitware/CMake/releases/download/v3.26.4/cmake-3.26.4-linux-x86_64.tar.gz | tar -xz

# Stage -- download maven
FROM base AS maven

# Install fresh maven, necessary for some modern compiler plugins
RUN mkdir /download \
    && cd /download \
    && curl -L https://dlcdn.apache.org/maven/maven-3/3.9.2/binaries/apache-maven-3.9.2-bin.tar.gz | tar -xz


# Stage -- actual JNI container
FROM base AS jni

RUN yum install -y java-17-amazon-corretto \
                   java-17-amazon-corretto-devel \
                   ninja-build \
                   gdb \
                   gcc10 \
                   gcc10-c++ \
                   libstdc++ \
                   libstdc++-static

COPY --from=cmake /download/cmake-3.26.4-linux-x86_64/bin/ /usr/local/bin/
COPY --from=cmake /download/cmake-3.26.4-linux-x86_64/share/ /usr/local/share/

COPY --from=maven /download/apache-maven-3.9.2/ /usr/local/maven/
RUN ln -s /usr/local/maven/bin/mvn /usr/local/bin/mvn

RUN cmake --version
RUN mvn -version

RUN mkdir /build
WORKDIR /build

ADD CMakeLists.txt /build/
ADD cmake_modules/compile_options.cmake /build/cmake_modules/
ADD cmake_modules/native_jar.cmake /build/cmake_modules/
ADD cmake_modules/libcxx.cmake /build/cmake_modules/
ADD cmake_modules/options.cmake /build/cmake_modules/

ADD pom.xml /build/
ADD pom-jni.xml /build/
ADD pom-jni-arch.xml /build/
ADD qdb/ /build/qdb/
ADD src/ /build/src/
ADD thirdparty/ /build/thirdparty/

# mvn compile creates the necessary jni header file
RUN mvn compile

# now compile our JNI native library
RUN mkdir build \
    && cd build \
    && cmake -G Ninja -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_COMPILER=gcc10-c++ -DCMAKE_C_COMPILER=gcc10-gcc .. \
    && cmake --build . --config Debug \
    && cd .. \
    \
# now build a .jar with all our assets (.jar files + native cojmpiled files)
    && mvn package -Dmaven.javadoc.skip=true -DskipTests -Darch=linux-x86_64 \
    \
# now install our library into our local maven repo ~/.m2
    && mvn install:install-file -f pom-jni.xml \
    && mvn install:install-file -f pom-jni-arch.xml -Darch=linux-x86_64 \
    \
# clear the build directory files
    && rm -rf /build \
    && mkdir /build
