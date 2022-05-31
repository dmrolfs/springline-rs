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

MEM_OPT="50m"
RM_OPT=""
LOG_OPT='--env RUST_LOG="info,springline::flink=debug,springline::kubernetes=debug,springline::phases::act=debug,springline::model=debug"'

DOCKER_PARAMS=""
SPRINGLINE_PARAMS=""
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
    -e|--env)
      if [ -n "$2" ] && [ ${2:0:1} != "-" ]; then
        ENV="$2"
        shift 2
      else
        echo "Error: Argument for $1 is missing" >&2
        exit 1
      fi
      ;;
    -a|--app-env)
      if [ -n "$2" ] && [ ${2:0:1} != "-" ]; then
        DOCKER_PARAMS="$DOCKER_PARAMS --env-file $2"
        shift 2
      else
        echo "Error: Argument for $1 is missing" >&2
        exit 1
      fi
      ;;
    -s|--search-path)
      if [ -n "$2" ] && [ ${2:0:1} != "-" ]; then
        SPRINGLINE_PARAMS="$SPRINGLINE_PARAMS --search-path=$2"
        shift 2
      else
        echo "Error: Argument for $1 is missing" >&2
        exit 1
      fi
      ;;
    -h|--host)
      if [ -n "$2" ] && [ ${2:0:1} != "-" ]; then
        SPRINGLINE_PARAMS="$SPRINGLINE_PARAMS --host=$2"
        shift 2
      else
        echo "Error: Argument for $1 is missing" >&2
        exit 1
      fi
      ;;
    -p|--port)
      if [ -n "$2" ] && [ ${2:0:1} != "-" ]; then
        SPRINGLINE_PARAMS="$SPRINGLINE_PARAMS --port=$2"
        shift 2
      else
        echo "Error: Argument for $1 is missing" >&2
        exit 1
      fi
      ;;
    --rm)
      RM_OPT="--rm"
      shift 1
      ;;
    -l|--log)
      if [ -n "$2" ] && [ ${2:0:1} != "-" ]; then
        LOG_OPT="--env RUST_LOG=$2"
        shift 2
      fi
      ;;
    -*|--*=) # unsupported flags
      echo "Error: Unsupported flag $1" >&2
      exit 1
      ;;
    *) # preserve positional arguments
      SPRINGLINE_PARAMS="$SPRINGLINE_PARAMS $1"
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
  --env KUBECONFIG="/secrets/environment.kubeconfig" \
  --env HA_CREDENTIALS="/secrets/credentials.properties" \
  ${LOG_OPT} \
  --mount type=bind,source="${KUBECONFIG}",target="/secrets/environment.kubeconfig" \
  --mount type=bind,source="${HA_CREDENTIALS}",target="/secrets/credentials.properties" \
  --expose 8000 \
  --network host \
  ${DOCKER_PARAMS} \
  springline:latest --env "${ENV}" ${SPRINGLINE_PARAMS}


#todo: enable host networking?? point to host-gateway, other?
#  --add-host host.springline:host-gateway \
#  --mount type=bind,source="${RESOURCES}",target="/app/resources" \
#  --entrypoint /bin/bash \
#  --add-host host.docker.internal:host-gateway \
