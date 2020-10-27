#!/bin/bash

set -euo pipefail

function usage() {
    echo 'Given a source herzog script and a Google Storage url, generate the notebook'
    echo 'and copy it into the GS url.'
}

if [[ ! $(command -v gsutil) ]]; then
    echo "Please install gsutil by following instructions at 'https://cloud.google.com/storage/docs/gsutil_install'"
    exit 1
fi

if [[ $# != 2 ]]; then
    usage
    exit 1
fi

herzog_script=$1
gs_dest=$2

if [[ ! -f ${herzog_script} ]]; then
    echo "${herzog_script}: No such file"
    exit 1
fi

herzog "${herzog_script}" > notebook.ipynb
gsutil cp notebook.ipynb "${gs_dest}" || echo "Unable to publish to ${gs_dest}"
