#!/bin/bash
function usage() {
    echo 'Given a notebook name, launch the Docker image configered in'
    echo '`notebooks/${NOTEBOOK_NAME}/leo_config`, install requirements,'
    echo 'and execute the notebook. '
}
if [[ $# != 1 ]]; then
    usage
    exit 1
fi

set -euo pipefail

NOTEBOOK=$1
CONTAINER=${NOTEBOOK}

if [[ -e ${BDCAT_NOTEBOOKS_HOME}/notebooks/${NOTEBOOK}/leo_config ]]; then
    source ${BDCAT_NOTEBOOKS_HOME}/notebooks/${NOTEBOOK}/leo_config
fi

docker kill $1 1>&2 || :
docker rm $1 1>&2 || :
docker pull ${LEO_IMAGE} 1>&2
wid=$(docker run \
  -v ${BDCAT_NOTEBOOKS_HOME}:${LEO_REPO_DIR} \
  -v ~/.config:/home/jupyter-user/.config \
  --name "${CONTAINER}" \
  -it -d \
  ${LEO_IMAGE})
echo -n ${wid}

docker exec ${CONTAINER} bash -c "${LEO_PIP} install --upgrade -r ${LEO_REPO_DIR}/notebooks/${NOTEBOOK}/requirements.txt"
docker exec ${CONTAINER} ${LEO_PYTHON} "${LEO_REPO_DIR}/notebooks/${NOTEBOOK}/main.py"
