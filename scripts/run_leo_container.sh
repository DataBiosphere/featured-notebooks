#!/bin/bash
function usage() {
    echo 'Given a Docker container name, kill and remove existing named'
    echo 'container and launch a new one.'
}
if [[ $# != 1 ]]; then
    usage
    exit 1
fi

set -euo pipefail

CONTAINER_NAME=$1
docker kill $1 1>&2 || :
docker rm $1 1>&2 || :
docker pull ${LEO_IMAGE} 1>&2
wid=$(docker run \
  -v ${BDCAT_NOTEBOOKS_HOME}:${LEO_REPO_DIR} \
  -v ~/.config:/home/jupyter-user/.config \
  --name "${CONTAINER_NAME}" \
  -it -d \
  ${LEO_IMAGE})
echo -n ${wid}
