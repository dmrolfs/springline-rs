#!/usr/bin/env bash
set -x
set -eo pipefail

PROJECT_HOME="${SPRINGLINE_HOME:=./springline}"
KUBECONFIG_HOME="/Users/rolfs/Documents/dev/here/olp/plm/secrets/admin/plaintext/k8setup/int-1-aws-eu-west-1"
KUBECONFIG="${KUBECONFIG_HOME}/plm-dev-hak.kubeconfig"

RESOURCES="`pwd`/resources"

docker run -d -it \
  --name springline \
  -m 64m \
  --mount type=bind,source="${KUBECONFIG}",target="/secrets/environment.kubeconfig" \
  --mount type=bind,source="${HA_CREDENTIALS}",target="/secrets/credentials.properties" \
  --expose 8000 \
  --network host \
  springline:latest

#  --mount type=bind,source="${RESOURCES}",target="/app/resources" \
#  --entrypoint /bin/bash \
#  --add-host host.docker.internal:host-gateway \
