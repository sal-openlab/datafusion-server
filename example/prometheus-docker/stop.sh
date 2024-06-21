#!/bin/bash

CONTAINER="prometheus"

ID=$(docker ps -q --filter name=$CONTAINER)

if [[ -n $ID ]]; then
    echo "Stop and removing container $ID ($CONTAINER)"
    docker rm -f "$ID"
else
    echo "Container '$CONTAINER' is not running"
fi
