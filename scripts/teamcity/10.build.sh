#!/usr/bin/env bash

set -eux -o pipefail

THIS_SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

source "${THIS_SCRIPT_DIR}/common.sh"

echo "PROJECT_ROOT: ${PROJECT_ROOT}"

tc_block_open 'Clean previous artifacts'
(
    if [[ -d "${MVN_TARGET_DIR}" ]]
    then
        rm -rfv ${MVN_TARGET_DIR}
    fi

    if [[ -d "${CMAKE_BUILD_DIR}" ]]
    then
        rm -rfv ${CMAKE_BUILD_DIR}
    fi
)
tc_block_close 'Clean previous artifacts'

# First round of compilation will be the java files; they should generate the
# necessary `.h` file needed to compile the JNI code.
#
# Note that this does *not* yet create any .jar files or whatever, it's not the
# same at all as packaging.
tc_block_open 'Maven compile'
(
    pushd ${PROJECT_ROOT}
    ${MVN} compile
    popd
)
tc_block_close 'Maven compile'


# Now that we have the header files, we can run cmake
tc_block_open 'CMake configure'
(
    pushd ${PROJECT_ROOT}
    CMAKE_FLAGS=("-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}")

    CMAKE_FLAGS+=("-S ${CMAKE_SOURCE_DIR}")
    CMAKE_FLAGS+=("-B ${CMAKE_BUILD_DIR}")

    if [ ! -z "${CMAKE_GENERATOR}" ]
    then
        CMAKE_FLAGS+=("-G '${CMAKE_GENERATOR}'")
    fi

    if [ ! -z "${CMAKE_C_COMPILER}" ]
    then
        CMAKE_FLAGS+=("-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}")
    fi

    if [ ! -z "${CMAKE_CXX_COMPILER}" ]
    then
        CMAKE_FLAGS+=("-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}")
    fi

    if [ ! -z "${QDB_CPU_ARCHITECTURE_CORE2}" ]
    then
        CMAKE_FLAGS+=("-D 'QDB_CPU_ARCHITECTURE_CORE2=${QDB_CPU_ARCHITECTURE_CORE2}'")
    fi

    if [ ! -z "${WINDOWS_HOST_ARCH}" ]
    then
        echo "Windows host arch: ${WINDOWS_HOST_ARCH}"
        CMAKE_FLAGS+=("-T host=${WINDOWS_HOST_ARCH}")
    fi

    if [ ! -z "${WINDOWS_TARGET_ARCH}" ]
    then
        echo "Windows target arch: ${WINDOWS_HOST_ARCH}"
        CMAKE_FLAGS+=("-A ${WINDOWS_TARGET_ARCH}")
    fi

    mkdir -p ${CMAKE_BUILD_DIR}

    eval ${CMAKE} ${CMAKE_FLAGS[@]}

    popd
)
tc_block_close 'CMake configure'

tc_block_open 'Compile JNI'
(
    CMAKE_FLAGS=(
        "--build ${CMAKE_BUILD_DIR}"
        "--config $CMAKE_BUILD_TYPE"
    )

    eval ${CMAKE} ${CMAKE_FLAGS[@]}
)
tc_block_close 'Compile JNI'

tc_block_open 'Package'
(
    pushd ${PROJECT_ROOT}
    ${MVN} package -DskipTests
    popd
)
tc_block_close 'Package'
