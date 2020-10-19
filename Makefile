MODULES=notebooks
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
	scripts/run_leo_container.sh $(@:notebooks/%.py=%)
	docker exec $(@:notebooks/%.py=%) bash -c "$(LEO_PIP) install --upgrade -r $(CONTAINER_REPO_DIR)/requirements-notebooks.txt"
	docker exec -it $(@:notebooks/%.py=%) $(LEO_PYTHON) $(CONTAINER_REPO_DIR)/$@
	$(CALLYSTO) $(LOCAL_ROOT_DIR)/$@ > $(LOCAL_ROOT_DIR)/$(@:.py=.ipynb)
	docker exec -it $(@:notebooks/%.py=%) $(CONTAINER_REPO_DIR)/scripts/publish.sh $(CONTAINER_REPO_DIR)/$@ $(CONTAINER_REPO_DIR)/$(@:.py=.ipynb)

# create make targets with pattern: cicd_notebooks/*.py (i.e. "make cicd_notebooks/byod.py")
cicd_notebooks:=$(notebooks:%=cicd_%)
$(cicd_notebooks):
	$(LEO_PIP) install --upgrade -r requirements-notebooks.txt
	$(LEO_PYTHON) $(@:cicd_%.py=%.py)
	$(LEO_PYTHON) /home/jupyter-user/.local/bin/callysto $(LOCAL_ROOT_DIR)/$(@:cicd_%.py=%.py) > $(LOCAL_ROOT_DIR)/$(@:cicd_%.py=%.ipynb)

clean_notebooks:
	git clean -dfX notebooks

clean:
	git clean -dfx

.PHONY: lint mypy clean clean_notebooks $(notebooks) $(cicd_notebooks)
