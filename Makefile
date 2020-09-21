include common.mk

MODULES=notebooks
CONTAINER=$(shell ./scripts/run_leo_container.sh)
LEO_PYTHON=/home/jupyter-user/miniconda/bin/python
LEO_PIP=/home/jupyter-user/miniconda/bin/pip
CALLYSTO=/home/jupyter-user/.local/bin/callysto
CONTAINER_REPO_ROOT=/$(shell basename $(PWD))

all: test

test: lint mypy
	@echo $(CALLYSTO)

lint:
	flake8 $(MODULES)

mypy:
	mypy --ignore-missing-imports --no-strict-optional $(MODULES)

notebooks:=$(wildcard notebooks/*.py)
$(notebooks): clean lint mypy
	docker exec -it $(CONTAINER) bash -c "$(LEO_PIP) install --upgrade -r $(CONTAINER_REPO_ROOT)/requirements.txt"
	docker exec -it $(CONTAINER) bash -c "$(LEO_PYTHON) $(CONTAINER_REPO_ROOT)/$@"
	docker exec -it $(CONTAINER) bash -c "$(CALLYSTO) $(CONTAINER_REPO_ROOT)/$@ > $(CONTAINER_REPO_ROOT)/$(@:.py=.ipynb)"
	scripts/publish.sh $@ $(@:.py=.ipynb)

clean:
	git clean -dfx

.PHONY: lint mypy clean $(notebooks)
