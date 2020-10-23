#!/bin/bash

set -euo pipefail

function usage() {
    echo 'Given a source callysto script and a Google Storage url, generate the notebook'
    echo 'and copy it into the GS url.'
}

if [[ $# != 2 ]]; then
    usage
    exit 1
fi

callysto_script=$1
gs_dest=$2

if [[ ! -f ${callysto_script} ]]; then
    echo "${callysto_script}: No such file"
    exit 1
fi

callysto "${callysto_script}" > notebook.ipynb
gsutil cp notebook.ipynb "${gs_dest}" || echo "Unable to publish to ${gs_dest}"
