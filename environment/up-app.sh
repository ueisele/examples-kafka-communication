#!/usr/bin/env bash
set -e
pushd . > /dev/null
cd $(dirname ${BASH_SOURCE[0]})
source ./docker-compose.sh
popd > /dev/null

function main () {
    local app=${1:?"Missing the name of the app or 'all'."}
    if [ $app = "all" ]; then
      start_apps app-producer app-consumer app-verified-generator
    else
      start_app "app-$app"
    fi
}

function start_app () {
    docker_compose_in_environment up $1
}

function start_apps () {
    for app in $@; do
      start_app_detached $app
    done
    attach_apps $@
}

function start_app_detached () {
    docker_compose_in_environment up -d $1
}

function attach_apps () {
    docker_compose_in_environment logs -f $@
}

main $@
