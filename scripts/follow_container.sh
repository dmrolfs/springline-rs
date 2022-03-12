#!/usr/bin/env bash
set -x
set -eo pipefail

if [ -z "$1" ]; then
    echo "Usage: $0 <container_id>"
    exit 1
fi

CONTAINER_ID=$1

docker logs --follow ${CONTAINER_ID} | bunyan | lnav
