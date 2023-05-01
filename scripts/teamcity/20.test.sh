#!/usr/bin/env bash

set -eux -o pipefail

THIS_SCRIPT_DIR=$(dirname $(realpath ${BASH_SOURCE[0]}))

source "${THIS_SCRIPT_DIR}/common.sh"

pushd ${PROJECT_ROOT}
${MVN} surefire:test
popd
