#!/usr/bin/env bash

set -eux -o pipefail

##
# Function definitions

function tc_build_problem {
    echo -n '##'
    echo "teamcity[buildProblem description='$1']"
}

function tc_block_open {
    echo -n '##'
    echo "teamcity[blockOpened name='$1']"
}

function tc_block_close {
    echo -n '##'
    echo "teamcity[blockClosed name='$1']"
}

export -f tc_build_problem
export -f tc_block_open
export -f tc_block_close


##
# Default variable definitions
#
# These variables should never be affected by any environment provided variables

THIS_SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_ROOT="${THIS_SCRIPT_DIR}/../../"
MVN_TARGET_DIR="${PROJECT_ROOT}/target/"
EXAMPLES_DIR="${PROJECT_ROOT}/examples/"

CMAKE_SOURCE_DIR="${PROJECT_ROOT}"
CMAKE_BUILD_DIR="${PROJECT_ROOT}/build/"

# Export
export PROJECT_ROOT
export MVN_TARGET_DIR
export EXAMPLES_DIR
export CMAKE_SOURCE_DIR
export CMAKE_BUILD_DIR


##
# Dynamic variable detection and definitions
#
# These variables can/could/should be customised using environment variables. If not
# provided, we use sane defaults.
#
# This is where the most "custom logic" is consolidated


# Use sane default cmake generator based on environment


WINDOWS_HOST_ARCH=${WINDOWS_HOST_ARCH:-}
WINDOWS_TARGET_ARCH=${WINDOWS_TARGET_ARCH:-}

case $(uname) in
    Linux )
        CMAKE_GENERATOR='Ninja'
        ARCH_CLASSIFIER=${ARCH_CLASSIFIER:-"linux-x86_64"}
        ;;
    FreeBSD )
        CMAKE_GENERATOR='Ninja'
        ARCH_CLASSIFIER=${ARCH_CLASSIFIER:-"freebsd-x86_64"}
        ;;
    Darwin )
        CMAKE_GENERATOR='Ninja'
        ARCH_CLASSIFIER=${ARCH_CLASSIFIER:-"osx-x86_64"}
        ;;
    MINGW* )
        CMAKE_GENERATOR='Visual Studio 17 2022'

        echo "Windows detected, probing target architecture"

        if [[ -z "${WINDOWS_HOST_ARCH}" ]]
        then
            echo "No exlicit host architecture provided, assuming win64"
            WINDOWS_HOST_ARCH="x64"
        fi

        if [[ "${WINDOWS_TARGET_ARCH}" == "win32" ]]
        then
            echo "Targeting win32"
            WINDOWS_TARGET_ARCH="Win32"
            ARCH_CLASSIFIER=${ARCH_CLASSIFIER:-"windows-x86_32"}

        elif [[ "${WINDOWS_TARGET_ARCH}" == "win64" ]]
        then
            echo "Targeting win64"
            WINDOWS_TARGET_ARCH="x64"
            ARCH_CLASSIFIER=${ARCH_CLASSIFIER:-"windows-x86_64"}
        else
            tc_build_problem "Unrecognized windows target architecture: ${WINDOWS_TARGET_ARCH}"
            exit -1
        fi
        ;;
    * )
        echo "Unable to detect environment, using default generator"
        ;;
esac

echo "ARCH_CLASSIFIER: ${ARCH_CLASSIFIER}"

MVN_PATH=${MVN_PATH:-}
CMAKE_PATH=${CMAKE_PATH:-}
JAVA_PATH=${JAVA_PATH:-}

if [ ! -z "${JAVA_PATH}" ]
then
    JAVA="${JAVA_PATH}"
else
    JAVA=java
fi

echo "Detected java: "
"${JAVA}" -version

if [ ! -z "${MVN_PATH}" ]
then
    MVN="${MVN_PATH}"
else
    MVN=mvn
fi

echo "Detected maven: "
"${MVN}" -version

if [ ! -z "${CMAKE_PATH}" ]
then
    CMAKE="${CMAKE_PATH}"
else
    CMAKE=cmake
fi

echo "Detected cmake: "
"${CMAKE}" -version

# Set these variables explicitly to empty values if they're not defined. This avoids
# undefined variable issues because we use set -eux
CMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE:-"RelWithDebInfo"}
CMAKE_C_COMPILER=${CMAKE_C_COMPILER:-}
CMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER:-}
QDB_CPU_ARCHITECTURE_CORE2=${QDB_CPU_ARCHITECTURE_CORE2:-}

export MVN
export CMAKE
export JAVA

export CMAKE_GENERATOR
export CMAKE_BUILD_TYPE
export CMAKE_C_COMPILER
export CMAKE_CXX_COMPILER

export WINDOWS_HOST_ARCH
export WINDOWS_TARGET_ARCH

export QDB_CPU_ARCHITECTURE_CORE2
