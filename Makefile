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
	gsutil cp $(@:.py=.ipynb) gs://fc-9169fcd1-92ce-4d60-9d2d-d19fd326ff10/notebooks

clean:
	git clean -dfx

.PHONY: lint mypy clean $(notebooks)
