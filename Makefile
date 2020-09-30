include common.mk

MODULES=notebooks
CONTAINER=$(shell ./scripts/run_leo_container.sh)
LOCAL_ROOT_DIR:=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))
CONTAINER_HOME_DIR=/home/jupyter-user
CONTAINER_REPO_DIR=$(CONTAINER_HOME_DIR)/$(shell basename ${LOCAL_ROOT_DIR})
LEO_PYTHON=$(CONTAINER_HOME_DIR)/miniconda/bin/python
LEO_PIP=$(CONTAINER_HOME_DIR)/miniconda/bin/pip
CALLYSTO=callysto

all: test

test: lint mypy

lint:
	flake8 $(MODULES)

mypy:
	mypy --ignore-missing-imports --no-strict-optional $(MODULES)

notebooks:=$(wildcard notebooks/*.py)
$(notebooks): clean_notebooks lint mypy
	docker exec $(CONTAINER) bash -c "$(LEO_PIP) install --upgrade -r $(CONTAINER_REPO_DIR)/requirements-notebooks.txt"
	docker exec -it $(CONTAINER) $(LEO_PYTHON) $(CONTAINER_REPO_DIR)/$@
	$(CALLYSTO) $(LOCAL_ROOT_DIR)/$@ > $(LOCAL_ROOT_DIR)/$(@:.py=.ipynb)
	docker exec -it $(CONTAINER) $(CONTAINER_REPO_DIR)/scripts/publish.sh $(CONTAINER_REPO_DIR)/$@ $(CONTAINER_REPO_DIR)/$(@:.py=.ipynb)

clean_notebooks:
	git clean -dfx notebooks

clean:
	git clean -dfx

.PHONY: lint mypy clean clean_notebooks $(notebooks)
