#!/bin/sh
set -eu -o pipefail
IFS=$'\n\t'

if [[ $# -ne 1 ]] ; then
    >&2 echo "Usage: $0 <new_version>"
    exit 1
fi

INPUT_VERSION=$1; shift

MAJOR_VERSION=${INPUT_VERSION%%.*}
WITHOUT_MAJOR_VERSION=${INPUT_VERSION#${MAJOR_VERSION}.}
MINOR_VERSION=${WITHOUT_MAJOR_VERSION%%.*}
WITHOUT_MINOR_VERSION=${INPUT_VERSION#${MAJOR_VERSION}.${MINOR_VERSION}.}
PATCH_VERSION=${WITHOUT_MINOR_VERSION%%.*}

XYZ_VERSION="${MAJOR_VERSION}.${MINOR_VERSION}.${PATCH_VERSION}"

if [[ "${INPUT_VERSION}" == *-* ]] ; then
    TAGS_VERSION="-SNAPSHOT"
else
    TAGS_VERSION=
fi

cd $(dirname -- $0)
cd ${PWD}/../..

# val qdbVersion = "2.8.0-SNAPSHOT"
#sed -i -e 's/val qdbVersion\s*=\s*"[0-9.]\+\(-SNAPSHOT\)\?"\s*$/val qdbVersion = "'"${XYZ_VERSION}${TAGS_VERSION}"'"/' build.sbt

# pom.xml
# <groupId>net.quasardb</groupId>
# <version>2.8.0-SNAPSHOT</version>
# <properties>
#   <groupId>net.quasardb</groupId>
#   <artifactId>jni</artifactId>
#   <version>2.8.0-SNAPSHOT</version>
#   <packaging>jar</packaging>
#   <file>target/jni-2.8.0-SNAPSHOT.jar</file>
# </properties>
# <artifact>
#   <file>${project.basedir}/target/jni-2.8.0-SNAPSHOT-linux-x86_64.jar</file>
# </artifact>

sed -i -e '/<groupId>net.quasardb<\/groupId>/,/<\/\(properties\|dependency\)>/ s/<version>[0-9.]\+[0-9]\(-SNAPSHOT\)\?<\/version>/<version>'"${XYZ_VERSION}${TAGS_VERSION}"'<\/version>/' pom.xml
sed -i -e '/<groupId>net.quasardb<\/groupId>/,/<\/properties>/ s/<file>\([-.${}a-zA-Z_/]*\)[0-9.]\+[0-9]\(-SNAPSHOT\)\?\([-.${}a-zA-Z_]*\)<\/file>/<file>\1'"${XYZ_VERSION}${TAGS_VERSION}"'\3<\/file>/' pom.xml
sed -i -e '/<artifact>/,/<\/artifact>/ s/<file>\([-.${}a-zA-Z_/]*\)[0-9.]\+[0-9]\(-SNAPSHOT\)\?\([-.${}a-zA-Z_0-9]*\)<\/file>/<file>\1'"${XYZ_VERSION}${TAGS_VERSION}"'\3<\/file>/' pom.xml

# set(NATIVE_JAR_FILE "${CMAKE_BINARY_DIR}/jni-2.8.0-SNAPSHOT-${SYSTEM}-${ARCH}.jar")
sed -i -e 's/\(set(NATIVE_JAR_FILE\) *\([-".${}a-zA-Z_/]*\)[0-9.]\+[0-9]\(-SNAPSHOT\)\?\([-.${}a-zA-Z_0-9]*")\)/\1 \2'"${XYZ_VERSION}${TAGS_VERSION}"'\4/' native_jar.cmake

# add_jar(jni
#   SOURCES ${QDB_JAVA_SOURCES}
#   OUTPUT_NAME jni-2.8.0-SNAPSHOT
# )
sed -i -e '/add_jar/,/OUTPUT_NAME/ s/\(OUTPUT_NAME\) *\([-".${}a-zA-Z_/]*\)[0-9.]\+[0-9]\(-SNAPSHOT\)\?\([-.${}a-zA-Z_0-9]*\)/\1 \2'"${XYZ_VERSION}${TAGS_VERSION}"'\4/' CMakeLists.txt