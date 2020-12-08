#!/bin/bash
# This script generates .gitlab-ci.yml
set -euo pipefail

function usage() {
    echo 'Given an output filename, generate a GitLab CI/CD yml file'
    echo 'If the output file exists, it will be overwritten.'
}

if [[ $# != 1 ]]; then
    usage
    exit 1
fi

out=$1

cat << 'EOF' > $1
# This file is generated and should not be modified directly.
# Edit 'scripts/generate_gitlab_yaml.sh' instead

image: us.gcr.io/broad-dsp-gcr-public/terra-jupyter-python:0.0.17

stages:
  - verify-gitlab-yml
  - test

before_script:
  - date && date -u

verify-gitlab-yml:
  stage: verify-gitlab-yml
  script: 
    - source environment
    - make verify-gitlab-yml

.notebook-test:
  stage: test
  before_script:
    - mkdir -p ~/.config/gcloud
    - cp $TEST_MULE_CREDENTIALS ~/.config/gcloud/application_default_credentials.json
    - source environment
    - $LEO_PIP install --upgrade -r requirements-dev.txt
  script:
    - make lint/$NOTEBOOK
    - make mypy/$NOTEBOOK
    - make cicd_test/$NOTEBOOK
EOF

for nb in $(find notebooks -mindepth 1 -maxdepth 1 -type d -print0 | sort -z | xargs -r0); do
    echo "" >> ${out}
    echo "$(basename ${nb}):" >> ${out}
    echo "  extends: .notebook-test" >> ${out}
    echo "  variables:" >> ${out}
    echo "    NOTEBOOK: \"$(basename ${nb})\"" >> ${out}
done
