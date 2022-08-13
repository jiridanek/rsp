#!/bin/bash

sudo podman run --net=host --rm --name envoy -v "$(pwd)/envoy.yaml:/etc/envoy/envoy.yaml" -e ENVOY_UID="$(id -u)" -it docker.io/envoyproxy/envoy:v1.23.0
