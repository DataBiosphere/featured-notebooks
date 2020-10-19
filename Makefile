MODULES=notebooks

all: test

test: lint mypy

lint:
	flake8 $(MODULES)

mypy:
	mypy --ignore-missing-imports --no-strict-optional $(MODULES)

notebooks:=$(wildcard notebooks/*.py)
$(notebooks): clean_notebooks lint mypy
	scripts/run_leo_container.sh $(@:notebooks/%.py=%)
	docker exec $(@:notebooks/%.py=%) bash -c "$(LEO_PIP) install --upgrade -r $(LEO_REPO_DIR)/requirements-notebooks.txt"
	docker exec -it $(@:notebooks/%.py=%) $(LEO_PYTHON) $(LEO_REPO_DIR)/$@
	callysto $@ > $(@:.py=.ipynb)
	docker exec -it $(@:notebooks/%.py=%) $(LEO_REPO_DIR)/scripts/publish.sh $(LEO_REPO_DIR)/$@ $(LEO_REPO_DIR)/$(@:.py=.ipynb)

# create make targets with pattern: cicd_notebooks/*.py (i.e. "make cicd_notebooks/byod.py")
cicd_notebooks:=$(notebooks:%=cicd_%)
$(cicd_notebooks):
	$(LEO_PIP) install --upgrade -r requirements-notebooks.txt
	$(LEO_PYTHON) $(@:cicd_%.py=%.py)
	$(LEO_PYTHON) /home/jupyter-user/.local/bin/callysto $(@:cicd_%.py=%.py) > $(@:cicd_%.py=%.ipynb)

clean_notebooks:
	git clean -dfX notebooks

clean:
	git clean -dfx

.PHONY: lint mypy clean clean_notebooks $(notebooks) $(cicd_notebooks)
