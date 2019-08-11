#!/usr/bin/env bash
set -e
pushd . > /dev/null
cd $(dirname ${BASH_SOURCE[0]})
source ./docker-compose.sh
popd > /dev/null

function main() {
    docker_compose_in_environment stop app-producer app-consumer app-verified-generator
}

main