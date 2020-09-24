#!/bin/bash

set -euo pipefail

function usage() {
    echo 'Given a source callysto script and a generated Jupyter notebook'
    echo 'copy the generated notebook to destination specified in the publish'
    echo 'directive. The publish directive is the first line of the source callysto script'
    echo 'with the format: `# publish to: "my workspace name" "my notebook name"`.'
}

if [[ $# != 2 ]]; then
    usage
    exit 1
fi

source_callysto_notebook=$1
generated_jupyter_notebook=$2

tnu=$(which tnu || find / -iname "tnu" -executable 2>/dev/null || :)
publish_directive=$(head -n 1 $source_callysto_notebook)
workspace=$(echo -n ${publish_directive} | cut -d '"' -f2)
notebook_name=$(echo -n ${publish_directive} | cut -d '"' -f3- | sed 's/"//g')
bucket=$(${tnu} workspace get-bucket --workspace "${workspace}")
dest="gs://${bucket}/notebooks/${notebook_name}.ipynb"
if [[ -f $generated_jupyter_notebook ]]; then
	echo ${generated_jupyter_notebook}
	echo ${dest}
    gsutil cp "${generated_jupyter_notebook}" "${dest}"
else
    usage
    echo "File does not exist: ${generated_jupyter_notebook}"
    exit 1
fi
