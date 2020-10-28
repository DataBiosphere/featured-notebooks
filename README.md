# bdcat_notebooks
Example Jupyter notebook source files for the BioData Catalyst consortium. These notebooks are published to the
[Terra notebook environment](https://support.terra.bio/hc/en-us/articles/360027237871-Terra-s-Jupyter-Notebooks-environment-Part-I-Key-components).

Jupyter notebook `.ipynb` files are generated from source files with the
[callysto](https://github.com/xbrianh/callysto) library. 

### Notebook Source Schema
A notebook is defined by creating a directory under ${REPO_ROOT}/notebooks containing
  - `main.py`, the source script for the notebook in [callysto](https://github.com/xbrianh/callysto) format.
  - `requirements.txt`, pip-installable requirements needed for notebook execution.
  - `publish.txt`, a list of destination Google Storage URLs where the notebook will be published.

### The Callysto Format
Notebook source files, `main.py`, are executable Python scripts. Contents of cells are denoted with the callysto
Python context managers:
```
with callysto.Cell("{mode}"):
  ...
```
where `{mode}` is `python` or `markdown`. Python statements outside of callysto contexts will not be rendered into
cells. Test code or mock environments should not be placed outside callysto contexts.

## Publishing & Testing

### Authorization for Testing and Publishing

Google user credentials are required to publish notebooks to Terra workspaces. Additionally, notebook execution may
require Google application default credentials. Both sets can be obtained by executing the commands
```
gcloud auth login
gcloud auth application-default login
```

Credentials are injected into the local Docker container when testing and publishing notebooks, and are expected to be
in the standard location under `~/.config`.

### Local Development Environment

Notebook source scripts should be developed using a Python 3.7 virtual environment, which matches the typical Python version
in the Terra notebook runtime, and `requirement-dev.txt` should be installed.

**If you do not already have Python 3.7, you can use the commands below to install it.**
```
brew install pyenv
pyenv install 3.7.3
echo -e 'if command -v pyenv 1>/dev/null 2>&1; then\n  eval "$(pyenv init -)"\nfi' >> ~/.zshrc
pyenv local 3.7.3
```
Then activate a virtual environment with:
```
python3 -m venv {vpath}
source {vpath}/bin/activate
```
where `{vpath}` should be replaced with the desired location of the virtual environment.


**Alternatively, if you have Python 3.7 installed by another method, you can activate your virtual environment using the following commands:**
```
/usr/local/bin/python3.7 -m venv {vpath}
source {vpath}/bin/activate
```
where `{vpath}` should be replaced with the desired location of the virtual environment.

**Once in a virtual environment, install the requirements:**
```
pip install -r requirements-dev.txt
```
### Publishing Notebooks
Notebooks are published with make commands, e.g.
```
make publish/byod
```
Before publishing, make sure that the Google Bucket in the publish.txt file matches that of the destination workspace. 

### Testing Notebooks
Tests are performed in Docker containers of the expected notebook deployment platform, e.g. Terra, with the command
```
make test
```

Tests can also be performed on individual notebooks
```
make test/byod
```

These recipes pass the source script through the [flake8](https://flake8.pycqa.org/en/latest/) linter and
[mypy](https://mypy.readthedocs.io/en/stable/) static analysis tool, and executes with a Docker container that is
typical of Terra notebook runtime environments for Python. If there are no errors,
[callysto](https://github.com/xbrianh/callysto) is used to generate the source script into an `.ipynb`, which is copied
into the Terra workspace bucket.

## Other
### Links
Project home page [GitHub](https://github.com/DataBiosphere/bdcat_notebooks)  

### Bugs
Please report bugs, issues, feature requests, etc. on [GitHub](https://github.com/DataBiosphere/bdcat_notebooks).
