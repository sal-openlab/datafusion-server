#!/bin/bash

CONTAINER="prometheus"
IMAGE="prom/prometheus"
VERSION="v2.53.0"
ID=$(docker ps -q --filter name=$CONTAINER)

if [[ -z $ID ]]; then
    echo "Executing container '$CONTAINER'"

    docker run -d \
        -p 9090:9090 \
        -v ./data:/prometheus-data \
        -v ./prometheus.yml:/etc/prometheus/prometheus.yml \
        --name $CONTAINER \
        $IMAGE:$VERSION
else
    echo "Container '$CONTAINER' is already running in $ID"
fi
