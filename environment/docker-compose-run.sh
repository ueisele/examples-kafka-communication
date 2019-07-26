#!/usr/bin/env bash
set -e
pushd . > /dev/null
cd $(dirname ${BASH_SOURCE[0]})
source ./docker-compose.sh
popd > /dev/null

function run_in_environment () {
    service=${1:?"Missing service name as first parameter!"}
    shift
    command=$@
    #sleep is a ugly fix for https://github.com/docker/compose/issues/4076
    if [ -z "${command}" ]; then
        docker_compose_in_environment run --rm ${service} bash
    else
        docker_compose_in_environment run --rm ${service} bash -c "sleep 0.1; ${command}"
    fi
}

if [ "${BASH_SOURCE[0]}" == "$0" ]; then
    run_in_environment "$@"
fi
