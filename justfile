set dotenv-load

timestamp := `date '+%s'`
image_default := env_var('IMAGE_TAG')
ns_default := env_var('JOB_NS_PREFIX') + env_var('JOB_ID')

alias c := full-check
alias u := update
alias db := build-docker
alias drl := run-docker-local
alias df := docker-follow
alias dp := push-image

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

# expects QUAY_PATH to be set in `.env`
push-image:
  echo "Labeling then pushing springline:latest docker image with tag: ${QUAY_PATH}:{{timestamp}}"
  docker tag springline:latest ${QUAY_PATH}:{{timestamp}}
  docker push ${QUAY_PATH}:{{timestamp}}

# expects KUBECONFIG and HA_CREDENTIALS to be set in `.env`
run-docker-local +ARGS:
  {{justfile_directory()}}/scripts/run_springline_local.sh {{ARGS}}

docker-follow CONTAINER_ID:
  docker logs --follow {{CONTAINER_ID}} | bunyan | lnav

helm-install env image=image_default namespace=ns_default:
  #!/usr/bin/env bash
  set -euxo pipefail
  echo "installing springline helm chart for image[{{image}}], configuration-env[{{env}}] to k8s namespace: {{namespace}}"
  cd {{justfile_directory()}}/springline/chart
  helm install autoscaler ./springline --debug \
    --values ./springline/env/{{env}}.yaml \
    --set image.tag={{image}} \
    --set global.pipeline.jobId=$JOB_ID \
    --namespace {{namespace}}

helm-uninstall namespace=ns_default:
  #!/usr/bin/env bash
  set -euxo pipefail
  helm uninstall -n {{namespace}} autoscaler

