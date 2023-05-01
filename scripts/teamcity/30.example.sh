#!/usr/bin/env bash

set -eux -o pipefail

THIS_SCRIPT_DIR=$(dirname $(realpath ${BASH_SOURCE[0]}))
source "${THIS_SCRIPT_DIR}/common.sh"

echo "Debug, target dir: "
find ${MVN_TARGET_DIR}

tc_block_open 'Local JNI install'
(
    pushd ${PROJECT_ROOT}
    "${MVN}" install:install-file -f pom-jni.xml
    popd
)
tc_block_close 'Local JNI install'

tc_block_open 'Local JNI install (Native)'
(
    pushd ${PROJECT_ROOT}
    "${MVN}" -f pom-jni-arch.xml -Darch=${ARCH_CLASSIFIER} install:install-file
    popd
)
tc_block_close 'Local JNI install (Native)'

tc_block_open 'Package example'
(
    pushd ${EXAMPLES_DIR}
    "${MVN}" -f pom.xml -Darch=${ARCH_CLASSIFIER} package
    popd
)
tc_block_close 'Package example'

tc_block_open 'Run example'
(
    "${JAVA}" -jar ${EXAMPLES_DIR}/target/examples-jar-with-dependencies.jar
)
tc_block_close 'Run example'
