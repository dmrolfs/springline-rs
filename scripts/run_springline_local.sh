#!/usr/bin/env bash
set -x
set -eo pipefail

if [ -z "${KUBECONFIG}" ]; then
  echo "Error: KUBECONFIG environment variable is not set" >&2
  exit 1
fi

if [ -z "$HA_CREDENTIALS" ]; then
  echo "Error: HA_CREDENTIALS environment variable is not set" >&2
  exit 1
fi

MEM_OPT="64m"
RM_OPT=""

PARAMS=""
while (( "$#" )); do
  case "$1" in
    -m|--memory)
      if [ -n "$2" ] && [ ${2:0:1} != "-" ]; then
        MEM_OPT="$2"
        shift 2
      else
        echo "Error: Argument for $1 is missing" >&2
        exit 1
      fi
      ;;
    --rm)
      RM_OPT="--rm"
      PARAMS="$2"
      shift 1
      ;;
    -*|--*=) # unsupported flags
      echo "Error: Unsupported flag $1" >&2
      exit 1
      ;;
    *) # preserve positional arguments
      PARAMS="$PARAMS $1"
      shift
      ;;
  esac
done

PROJECT_HOME="${SPRINGLINE_HOME:=./springline}"
RESOURCES="$(pwd)/resources"

docker run -d -it \
  -m ${MEM_OPT} \
  ${RM_OPT} \
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
