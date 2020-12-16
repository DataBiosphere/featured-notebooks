# bdcat_notebooks
Example Jupyter notebook source files for the BioData Catalyst consortium. These notebooks are published to the
[Terra notebook environment](https://support.terra.bio/hc/en-us/articles/360027237871-Terra-s-Jupyter-Notebooks-environment-Part-I-Key-components).

Jupyter notebook `.ipynb` files are generated from source files with the
[herzog](https://github.com/xbrianh/herzog) library. 

## Notebooks Represented
* BYOD -- Tutorial for bringing your own data to the Terra platform
* GWAS Blood Pressure -- Two part series teaching how to use Gen3 data to preform a GWAS; [mirror of template workspace](https://app.gitbook.com/@bdcatalyst/s/biodata-catalyst-tutorials/tutorials/terra-tutorials/genome-wide-association-study-tutorial)
* Prepare IGV Viewer Input -- Prepares data for IGV within Terra
* VCF Merge/Subsample Tutorial -- Merging and subsampling for jointly called VCFs
* Workflow Cost Estimator -- Estimate costs of workflows, even on billing projects that obscure this information
* xvcfmerge Array Input -- Demonstrate Terra's data table structure being converted to inputs for xvcfmerge workflows

### Notebook Source Schema
A notebook is defined by creating a directory under ${REPO_ROOT}/notebooks containing
  - `main.py`, the source script for the notebook in [herzog](https://github.com/xbrianh/herzog) format.
  - `requirements.txt`, pip-installable requirements needed for notebook execution.
  - `publish.txt`, a list of destination Google Storage URLs where the notebook will be published.

### The Herzog Format
Notebook source files, `main.py`, are executable Python scripts. Contents of cells are denoted with the herzog
Python context managers:
```
with herzog.Cell("{mode}"):
	...
```
where `{mode}` is `python` or `markdown`. Python statements outside of herzog contexts will not be rendered into
cells. Test code or mock environments should not be placed outside herzog contexts.

## Publishing & Testing

### Publishing Notebooks
Notebooks are published with make commands, e.g.
```
make publish/byod
```
Before publishing, make sure that the Google Bucket in the publish.txt file matches that of the destination workspace. 

### ad-hoc publication
A convenience script is provided to generate herzog scripts into .ipynb files and copy them into Google Storage
locations.
```
scrips/generate_to_gs.sh notebooks/byod/main.py gs://my-bucket/my-notebook-location
```

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
[herzog](https://github.com/xbrianh/herzog) is used to generate the source script into an `.ipynb`, which is copied
into the Terra workspace bucket.

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

1. Notebook source scripts should be developed using a Python 3.7 virtual environment, which matches the typical Python version
   in the Terra notebook runtime, and `requirement-dev.txt` should be installed.

   On MacOS or similar systems, this can be done by issuing the commands
   ```
   /usr/local/bin/python3.7 -m venv {vpath}
   source {vpath}/bin/activate
   pip install -r requirements-dev.txt
   ```
   where `{vpath}` should be replaced with the desired location of the virtual environment.

2. The Google Storage utility "gsutil" should be installed according to the instructions found
   [here](https://cloud.google.com/storage/docs/gsutil_install).

## Other
### Links
Project home page [GitHub](https://github.com/DataBiosphere/bdcat_notebooks)  

### Bugs
Please report bugs, issues, feature requests, etc. on [GitHub](https://github.com/DataBiosphere/bdcat_notebooks).
