MODULES=notebooks

all: test

test: lint mypy

lint:
	flake8 $(MODULES)

mypy:
	mypy --ignore-missing-imports --no-strict-optional $(MODULES)

notebooks:=$(wildcard notebooks/*.py)
$(notebooks): lint mypy
	python $@
	callysto $@ > $(@:.py=.ipynb)
	scripts/publish.sh $@ $(@:.py=.ipynb)

clean:
	git clean -dfx

.PHONY: lint mypy clean $(notebooks)
