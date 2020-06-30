#!/bin/bash

set -euo pipefail

function usage() {
    echo 'Given a source callysto script and a generated Jupyter notebook'
    echo 'copy the generated notebook to destination specified in the publish'
    echo 'directive. The publish directive is the first line of the source callysto script'
    echo 'with the format: `# publish to gs://my_bucket/my_key`'
}

if [[ $# != 2 ]]; then
    usage
    exit 1
fi

source_callysto_notebook=$1
generated_jupyter_notebook=$2

echo $source_callysto_notebook
publish_directive=$(head -n 1 $source_callysto_notebook)
dest=$(echo -n ${publish_directive} | cut -d ' ' -f4)
if [[ ${dest} != *gs://* ]]; then
    usage
    echo "Malformatted publish directive: ${publish_directive}"
    echo ${dest}
    exit 1
fi

if [[ -f $generated_jupyter_notebook ]]; then
    gsutil cp ${generated_jupyter_notebook} ${dest}
else
    usage
    echo "File does not exist: ${generated_jupyter_notebook}"
    exit 1
fi
