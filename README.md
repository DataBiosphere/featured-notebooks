# bdcat_notebooks
Example Jupyter notebook source files for the BioData Catalyst consortium. These notebooks are published to the
[Terra notebook environment](https://support.terra.bio/hc/en-us/articles/360027237871-Terra-s-Jupyter-Notebooks-environment-Part-I-Key-components).

Jupyter notebook `.ipynb` files are generated from source files with the
[callysto](https://github.com/xbrianh/callysto) library. 

## Source File Schema
The first line of each notebook source file contains the publish directive, a string with the following format:
```
# publish to "{my workspace name}" "{my notebook name}"
```
where `{my workspace name}` should be replaced with the name of the workspace, and `{my notebook name}` should be
replaced with the name of the notebook.

Notebook source files are executable Python scripts. The contents of cells are denoted with the callysto Python
context managers:
```
with callysto.Cell("{mode}"):
	...
```
where `{mode}` is `python` or `markdown`. Python statements outside of callysto contexts will not be rendered into
cells. Test code, or code for preparing mock environments, should not be placed in callysto contexts.

## Publishing Notebooks
Notebooks are published with make commands, e.g.
```
make notebooks/prepare_igv_viewer_input.py
```

This command passes the source script through the [flake8](https://flake8.pycqa.org/en/latest/) linter and
[mypy](https://mypy.readthedocs.io/en/stable/) static analysis tool, and executes with the local Python interpreter.
If there are no errors, callysto is used to generate the source script into an `.ipynb`, which is copied into the Terra
workspace bucket.

Publishing requires Google client authorization, as well as Terra access to the target workspace. Google authorization
is obtained with the command
```
gcloud auth login
```

## Authorization for Testing and Publishing

Google user credentials are required to publish notebooks to Terra workspaces. Additinally, notebook execution may
require Google application default credentials. Both sets can be obtained by executing the commmads
```
gcloud auth login
gcloud auth application-default login
```

Credentials are injected into the local Docker container when testing and publishing notebooks, and are expected to be
in the standard location `~/.config`.

## Local Development Environment

Notebook source scripts should be developed using a Python 3.7 virtual environment, which matches the typical Python version
in the Terra notebook runtime, and requirements should be installed.

On MacOS or similar systems, this can be done by issuing the commands
```
/usr/local/bin/python3.7 -m venv {vpath}
source {vpath}/bin/activate
pip install -r requirements.txt
```

where `{vpath}` should be replaced with the desired location of the virtual environment data. It is common developer
pattern to maintain a clean Python environment by frequently re-creating virtual environments.

## Links
Project home page [GitHub](https://github.com/DataBiosphere/bdcat_notebooks)  

### Bugs
Please report bugs, issues, feature requests, etc. on [GitHub](https://github.com/DataBiosphere/bdcat_notebooks).
