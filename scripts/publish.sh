#!/bin/bash

set -euo pipefail

function usage() {
    echo 'Given a source notebook file and a publish directive file copy the generated'
    echo 'notebook to destination specified in the publish directive. The publish'
    echo 'directive file should contain lines specifying Google Storage destination of'
    echo 'the notebook .ipynb file: gs://bucket_name/pfx/notebook_name.ipynb'
    echo 'The GS url may contain spaces. Text after "#" in any line is ignored.'
}

if [[ $# != 2 ]]; then
    usage
    exit 1
fi

notebook_ipynb=$1
publish_directive=$2

if [[ ! -f ${notebook_ipynb} ]]; then
    echo "${notebook_ipynb}: No such file"
    exit 1
fi

if [[ ! -f ${publish_directive} ]]; then
    echo "${publish_directive}: No such file"
    exit 1

fi

while read line; do
    gs_dest=$(echo -n ${line} | cut -d'#' -f1)
    if [[ ! -z "${gs_dest}" ]]; then
        gsutil cp "${notebook_ipynb}" "${gs_dest}" || echo "Unable to publish to ${gs_dest}"
    fi
done < ${publish_directive}
