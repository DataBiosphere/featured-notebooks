#!/bin/bash
set -euo pipefail

# Resolve the location of this file and set BDCAT_NOTEBOOKS_HOME to the root
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ] ; do SOURCE="$(readlink "$SOURCE")"; done
export BDCAT_NOTEBOOKS_HOME="$(cd -P "$(dirname "$SOURCE")" && cd .. && pwd)"

IMAGE_NAME="us.gcr.io/broad-dsp-gcr-public/terra-jupyter-python:0.0.12"
CONTAINER_NAME="leo-container"
CONTAINER_REPO_ROOT="$(basename ${BDCAT_NOTEBOOKS_HOME})"
wid=$(docker ps -a --latest  --filter "status=running" --filter "name=${CONTAINER_NAME}" --format="{{.ID}}")
if [[ -z $wid ]]; then
    # start new container
    docker pull ${IMAGE_NAME} 1>&2
    wid=$(docker run \
          --mount type=bind,source=${BDCAT_NOTEBOOKS_HOME},target=/home/jupyter-user/${CONTAINER_REPO_ROOT} \
          -v ~/.config:/home/jupyter-user/.config \
          --name "${CONTAINER_NAME}" \
          -it -d \
          ${IMAGE_NAME})
else
    # use existing container
    :
fi
echo -n ${wid}
