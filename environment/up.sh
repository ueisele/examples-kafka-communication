#!/usr/bin/env bash
set -e
pushd . > /dev/null
cd $(dirname ${BASH_SOURCE[0]})
source ./docker-compose.sh
popd > /dev/null

function main () {
    start_service "kafka"
    start_service "schema-registry"
    start_once "init-topics"
}

function start_service () {
    docker_compose_in_environment up -d $1
}

function show_logs () {
    docker_compose_in_environment logs -f $1
}

function start_once () {
    docker_compose_in_environment up -d $1
    while [[ $(docker_compose_in_environment ps --filter "status=running" --services) == *"$1"* ]]; do
      docker_compose_in_environment logs -f $1
    done
}

main
