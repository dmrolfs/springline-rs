set dotenv-load

alias c := full-check
alias u := update
alias db := build-docker
alias drl := run-docker-local
alias df := docker-follow
alias dp := push-docker

default:
  just --list

full-check:
  cargo fmt
  cargo c
  cargo clippy

update:
  cargo upgrade --workspace
  cargo update

build-docker:
  cargo t
  docker build --tag springline --file springline/Dockerfile .

# expects QUAY_TAG to be set in `.env`
push-docker:
  echo "Labeling then pushing springline:latest docker image with tag: $QUAY_TAG"
  docker tag springline:latest $QUAY_TAG
  docker push $QUAY_TAG

# expects KUBECONFIG and HA_CREDENTIALS to be set in `.env`
run-docker-local +ARGS:
  {{justfile_directory()}}/scripts/run_springline_local.sh {{ARGS}}

docker-follow CONTAINER_ID:
  docker logs --follow {{CONTAINER_ID}} | bunyan | lnav
