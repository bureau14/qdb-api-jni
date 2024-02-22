#!/usr/bin/env bash

set -eux -o pipefail

THIS_SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

source "${THIS_SCRIPT_DIR}/common.sh"

pushd ${PROJECT_ROOT}
${MVN} surefire:test
popd
