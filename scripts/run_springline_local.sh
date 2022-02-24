#!/usr/bin/env bash
set -x
set -eo pipefail

PROJECT_HOME="${SPRINGLINE_HOME:=./springline}"

RESOURCES="$(pwd)/resources"

docker run -d -it \
  -m 64m \
  --rm \
  --name springline \
  --mount type=bind,source="${RESOURCES}",target="/app/resources" \
  --mount type=bind,source="${KUBECONFIG}",target="/secrets/environment.kubeconfig" \
  --mount type=bind,source="${HA_CREDENTIALS}",target="/secrets/credentials.properties" \
  --expose 8000 \
  --network host \
  springline:latest


#todo: enable host networking?? point to host-gateway, other?
#  --add-host host.springline:host-gateway \
#  --mount type=bind,source="${RESOURCES}",target="/app/resources" \
#  --entrypoint /bin/bash \
#  --add-host host.docker.internal:host-gateway \
