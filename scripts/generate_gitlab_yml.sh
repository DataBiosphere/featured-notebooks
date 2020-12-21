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

# Escaped variables, e.g. \$TEST_MULE_CREDENTIALS, are not expanded in the heredoc defined below
# during this script's execution.
cat << EOF > $1
# This file is generated and should not be modified directly.
# Edit 'scripts/generate_gitlab_yaml.sh' instead

stages:
  - verify-gitlab-yml
  - test

before_script:
  - date && date -u

verify-gitlab-yml:
  stage: verify-gitlab-yml
  image: ${LEO_IMAGE}
  script: 
    - source environment
    - make verify-gitlab-yml

.notebook-test:
  stage: test
  image: \$IMAGE
  before_script:
    - mkdir -p ~/.config/gcloud
    - cp \$TEST_MULE_CREDENTIALS ~/.config/gcloud/application_default_credentials.json
    - source environment
    - if [[ -e notebooks/\$NOTEBOOK/leo_config ]]; then source notebooks/\$NOTEBOOK/leo_config; fi
    - \$LEO_PIP install --upgrade -r requirements-dev.txt
    - \$LEO_PIP install --upgrade -r notebooks/\$NOTEBOOK/requirements.txt
  script:
    - make lint/\$NOTEBOOK
    - make mypy/\$NOTEBOOK
    - make cicd_test/\$NOTEBOOK
EOF

export LC_ALL='en_US.UTF-8'  # Needed to ensure `sort` works as expected on Ubuntu (and perhaps other systems)
if [ ${USER} = ash ]; then
  PRINT="-print"
  SORT="sort -f"
  XARGS="xargs"
else
  PRINT="-print0"
  SORT="sort -f -z"
  XARGS="xargs -r0"
fi
for nb in $(find notebooks -mindepth 1 -maxdepth 1 -type d ${PRINT} | ${SORT} | ${XARGS}); do
    source environment
    if [[ -e "${nb}/leo_config" ]]; then
        source "${nb}/leo_config"
    fi
    echo "" >> ${out}
    echo "$(basename ${nb}):" >> ${out}
    echo "  extends: .notebook-test" >> ${out}
    echo "  variables:" >> ${out}
    echo "    NOTEBOOK: \"$(basename ${nb})\"" >> ${out}
    echo "    IMAGE: ${LEO_IMAGE}" >> ${out}
done
