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
                      gcc \
                      gcc-c++ \
                      gzip \
                      libstdc++ \
                      libstdc++-static \
                      tar \
                      which

# Install fresh cmake, necessary for range-v3
RUN mkdir /download \
    && cd /download \
    && curl -L https://github.com/Kitware/CMake/releases/download/v3.25.1/cmake-3.25.1-linux-x86_64.tar.gz | tar -xz \
    && mv cmake-3.25.1-linux-x86_64/bin/* /usr/local/bin/ \
    && mv cmake-3.25.1-linux-x86_64/share/* /usr/local/share/ \
    && cmake --version \
    && rm -rf /download

# Install fresh maven, necessary for some modern compiler plugins
RUN mkdir /download \
    && cd /download \
    && curl -L https://dlcdn.apache.org/maven/maven-3/3.8.8/binaries/apache-maven-3.8.8-bin.tar.gz | tar -xz \
    && mv apache-maven-3.8.8/ /usr/local/maven/ \
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

# mvn compile creates the necessary jni header file
RUN mvn compile

# now compile our JNI native library
RUN mkdir build \
    && cd build \
    && cmake -G Ninja -DCMAKE_BUILD_TYPE=Debug .. \
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
