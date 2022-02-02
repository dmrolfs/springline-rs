#!/usr/bin/env bash
set -x
set -eo pipefail

PROJECT_HOME="${SPRINGLINE_HOME:=./springline}"

docker build --tag springline --file "${PROJECT_HOME}/Dockerfile" .
