include common.mk

MODULES=notebooks
CONTAINER=$(shell ./scripts/run_leo_container.sh)
CONTAINER_ROOT_DIR=/home/jupyter-user
LOCAL_ROOT_DIR:=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))
LEO_PYTHON=$(CONTAINER_ROOT_DIR)/miniconda/bin/python
LEO_PIP=$(CONTAINER_ROOT_DIR)/miniconda/bin/pip
CALLYSTO=callysto

all: test

test: lint mypy
	@echo $(CALLYSTO)

lint:
	flake8 $(MODULES)

mypy:
	mypy --ignore-missing-imports --no-strict-optional $(MODULES)

notebooks:=$(wildcard notebooks/*.py)
PIP_REQS:=$(shell cat $(LOCAL_ROOT_DIR)/requirements.txt | tr "\n" " ")
$(notebooks): clean lint mypy
	docker exec $(CONTAINER) bash -c "$(LEO_PIP) install --upgrade $(PIP_REQS)"
	docker exec $(CONTAINER) mkdir -p notebooks
	docker cp $(LOCAL_ROOT_DIR)/$@ $(CONTAINER):$(CONTAINER_ROOT_DIR)/$@
	docker exec $(CONTAINER) $(LEO_PYTHON) $(CONTAINER_ROOT_DIR)/$@
	$(CALLYSTO) $(LOCAL_ROOT_DIR)/$@ > $(LOCAL_ROOT_DIR)/$(@:.py=.ipynb)
	scripts/publish.sh $(LOCAL_ROOT_DIR)/$@ $(LOCAL_ROOT_DIR)/$(@:.py=.ipynb)

clean:
	git clean -dfx

.PHONY: lint mypy clean $(notebooks)
