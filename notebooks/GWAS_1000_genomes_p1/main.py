#!/usr/bin/env python
# coding: utf-8

# Notebook author: Beth Sheets
# Herzog version: Ash O'Farrell

import os
import herzog

# Mock the environment
os.environ['WORKSPACE_NAME'] = "CICD TESTER - PUBLIC DATA ONLY - BDC GWAS 1000G Tutorial"
os.environ['WORKSPACE_BUCKET'] = "gs://fc-7d1224a1-1770-4db9-9904-137efcd412c5"
os.environ['GOOGLE_PROJECT'] = "anvil-stage-demo"

with herzog.Cell("markdown"):
    """
    # Notebook Overview

    This notebook consolidates the phentotypic data in tables imported to Terra from Gen3 into one consolidated table of metadata with familiar subject IDs for downstream analysis. It uses functions defined in the terra-util notebook, nd can be adapted to consolidate different metadata. I

    # Set up your notebook
    ----

    ## Set runtime values
    If you are opening this notebook for the first time, and you did not edit your runtime settings before starting it, you will now need to change your settings. Click on the gear icon in the upper right to edit your Notebook Runtime. Set the values as specified below:
    | Option                    | Values                                        |
    |---------------------------|-----------------------------------------------|
    | Environment               | Default (GATK 4.1.4.1, Python 3.7.7, R 4.0.3) |
    | CPUs                      | 4                                             |
    | Memory (GB)               | 15                                            |
    | Startup script            | (leave blank)                                 |
    | Compute type              | Standard VM                                   |
    | Persistent disk size (GB) | 50                                            |

    Click the "Replace" button when you are done, and Terra will begin to create a new runtime with your settings. When it is finished, it will pop up asking you to apply the new settings.

    ## Check kernel type

    A kernel is a _computational engine_ that executes the code in the notebook. You can think of it as defining the programming language. For this notebook, we'll use a `Python 3` kernel. In the upper right corner of the notebook, just under the Notebook Runtime, it should say `Python 3`. If it doesn't, you can switch it by navigating to the Kernel menu and selecting `Change kernel`.

    ## Install packages
    """

with herzog.Cell("python"):
    #%pip install tenacity
    pass
with herzog.Cell("markdown"):
    """
    Make sure to restart the kernel whenever you pip install things in a Jupyter notebook.

    ## Import all the packages this notebook will use
    """

with herzog.Cell("python"):
    from firecloud import fiss
    import tenacity
    import pandas as pd
    pd.set_option('display.max_row', 10)
    import os
    import io
    import numpy as np
    import seaborn as sns
with herzog.Cell("markdown"):
    """
    ## Define filepaths and environmental variables
    """

with herzog.Cell("python"):
    PROJECT = os.environ['GOOGLE_PROJECT']
    PROJECT
with herzog.Cell("python"):
    WORKSPACE = os.path.basename(os.path.dirname(os.getcwd()))
    WORKSPACE
with herzog.Cell("python"):
    bucket = os.environ['WORKSPACE_BUCKET']
    bucket = bucket + '/'
    bucket
with herzog.Cell("markdown"):
    """
    Consolidate the Gen3 clinical entities into a single Terra data model using functions in the "terra_data_table_util" python notebook
    """

with herzog.Cell("python"):
    # Run the companion notebook. Note: it must be in the same workspace you're currently working in.
    #%run terra_data_table_util.ipynb
    pass
with herzog.Cell("python"):
    # Take a look at all the entities (tables) in the workspace
    ent_types = fiss.fapi.list_entity_types(PROJECT, WORKSPACE).json()
    for t in ent_types.keys():
        print(t, "count:", ent_types[t]['count'])  #type: ignore  # noqa
with herzog.Cell("python"):
    # Set the name for your consolidated table
    consolidated_table_name = "consolidated_metadata"
with herzog.Cell("markdown"):
    """
    The consolidate_gen3_pheno_tables function:
    * Joins all clinical data tables into a single consolidated_metadata table in the Terra data section
    * This join forces all entities (individuals) to be present in every clinical table, so some individuals may be removed. Consider how this affects your dataset.
    * Renames attribute fields to have a prefix of the original entity type (for example: "demographic_annotated_sex", where demographic is the original entity type, annotated_sex is the attribute field)
    """

with herzog.Cell("python"):
    # Consolidate the phenotypic data using the function defined in the terra-util notebook
    consolidate_gen3_pheno_tables(PROJECT, WORKSPACE, consolidated_table_name)  #type: ignore  # noqa
