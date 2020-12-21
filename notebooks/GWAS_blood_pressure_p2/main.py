#!/usr/bin/env python
# coding: utf-8

# Title: 2-GWAS-preliminary-analysis

# Notebook author: Beth Sheets
# Herzogification: Ash O'Farrell

import os
from unittest import mock

import herzog


# Heavyweight dependencies (e.g. hail, Spark) make it challenging to create robust automated testing for this notebook.
# In order to get it to pass Python execution, many objects are mocked with unittest.mock.MagicMock. These have been
# marked with `# test fixture`. This will at least catch some syntax errors.
#
# In the meantime, this notebook should be tested manually in a Terra notebook environment.

consolidate_gen3_pheno_tables = mock.MagicMock()  # test fixture
get_terra_table_to_df = mock.MagicMock()  # test fixture
get_ipython = mock.MagicMock()  # test fixture
os.environ['GOOGLE_PROJECT'] = "foo"  # test fixture
os.environ['WORKSPACE_BUCKET'] = "bar"  # test fixture

with herzog.Cell("markdown"):
    """
    # Data disclaimer

    All data in this notebook is restricted access. You are responsible for the protection and proper use of any data or files downloaded from this workspace.

    # Introduction

    This first phenotype analysis notebook demonstrates typical initial steps in a genetic association analysis: exploring phenotype distributions and selection potential covariates.



    1. First, we discover all of the metadata available from our Gen3 export. We then use a set of functions created to merge and reformat the metadata to create a consolidated data table. We reformat a bit of the Gen3 graph language to be more familiar TOPMed nomenclature.

    2. We then import the metadata from the data table to the notebook compute environment using the FISS API.

    3. We explore and process the phenotypic data to understand their underlying structure. To do this, we subset the specific phenotype data that we are interested in and generate plots to examine the distribution and structure of these phenotypes.

    4. We define an outcome and a set of covariates to use when modeling genotype-phenotype associations.

    5. Finally, we save a new csv file with our phenotypes of interest that we will compare to genotypic data in the next notebook. 
    
   
    # Set up your notebook

    ## Set your runtime configuration

    <table style="float:left">
        <thead>
            <tr><th>Option</th><th>Value</th></tr>
        </thead>
        <tbody>
            <tr><td> Application configuration</td><td>Default (GATK 4.1.4.1, Python 3.7.7, R 4.0.3) </tr></td>
            <tr><td> CPUs</td><td>4</tr></td>
            <tr><td> Memory (GB)</td><td>15</tr></td>
            <tr><td> Startup script</td><td>(leave blank)</tr></td>
            <tr><td> Compute type</td><td>Standard VM</tr></td>
            <tr><td> Persistent disk size (GB)</td><td>500</tr></td>
        </tbody>
    </table>
    """
with herzog.Cell("markdown"):
    """
    # Load useful packages

    * **FISS** - a toolkit for using Firecloud/Terra APIs through Python
    * **Pandas & Numpy** - packages for data analysis
    * **Pprint** - for pretty printing
    * **Matplotlib & Seaborn** - for plotting

    ## Install and update packages
    """
with herzog.Cell("python"):
    #%pip install tenacity
    pass
with herzog.Cell("markdown"):
    """
    **Restart the kernel after every pip install. You can do this using the kernel selection in the toolbar.**

    ## Import all the packages this notebook will use

    """
with herzog.Cell("python"):
    #%%capture
    from firecloud import fiss
    import pandas as pd
    pd.set_option('display.max_row', 10)
    import os
    import io
    import numpy as np
    from pprint import pprint
    import matplotlib.pyplot as plt
    import seaborn as sns
    #%matplotlib inline
    import time
    from datetime import timedelta
    import tenacity

fiss = mock.MagicMock()  # noqa # test fixture
sns = mock.MagicMock()  # noqa test fixture
pd = mock.MagicMock()  # noqa test fixture
with herzog.Cell("markdown"):
    """
    # Load workspace data

    Phenotypic data for each individual in the study are stored in the workspace data table. To analyze inside this notebook, we have to explicitly load the data in our notebook environment. To do this, we'll need some information about the Terra Workspace. We use the Fiss API to access environmental variables.

    The billing project, workspace, and bucket filepaths are neccessary to define in every python Jupyter notebook you run in Terra.

    ## Define filepaths and environmental variables
    """
with herzog.Cell("python"):
    start_notebook_time = time.time()
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
    Here, we define some output files we will generate in this notebook.
    """
with herzog.Cell("python"):
    phenotype_out = 'bp-phenotypes.csv'
    notebook_out = 'bp-phenotypes.ipynb'
    html_out = 'bp-phenotypes.html'
    samples_out = 'samples_traits.csv'
with herzog.Cell("python"):
    # Take a look at all of the entities in of our workspace
    ent_types = fiss.fapi.list_entity_types(PROJECT, WORKSPACE).json()
    for t in ent_types.keys():
        print(t, "count:", ent_types[t]['count'])
with herzog.Cell("markdown"):
    """
    Note: If you receive an error here about the Firecloud model. Try restarting the notebook kernel and reruning the prior lines of code.

    # Consolidate the Gen3 clinical entities into a single Terra data model using functions in the "terra_data_util" python notebook

    The terra_data_util notebook was created to help researchers manipulate TOPMed data imported from Gen3. The notebook contains several functions for consolidating, renaming, and deleting entities in Terra. It is available as a notebook so that a researcher can easily edit functions for their specific use cases. Review the Gen3 data dictionary to understand how entities are related in the graph structure.
    """
with herzog.Cell("python"):
    #Run the companion notebook. Note: it must be in the same workspace you are currently working in.
    #%run terra_data_table_util.ipynb
    pass
with herzog.Cell("markdown"):
    """
    The consolidate_gen3_pheno_tables function:
    * Joins all clinical data tables into a single consolidated_metadata table in the Terra data section
    * This join forces all entities (individuals) to be present in every clinical table, so some individuals may be removed. Consider how this affects your dataset.
    * Renames attribute fields to have a prefix of the original entity type (for example: "demographic_annotated_sex", where demographic is the original entity type, annotated_sex is the attribute field)
    """
with herzog.Cell("python"):
    consolidated_table_name = "consolidated_metadata"
with herzog.Cell("python"):
    consolidate_gen3_pheno_tables(PROJECT, WORKSPACE, consolidated_table_name)

with herzog.Cell("markdown"):
    """
    ## Read data from the workspace data model

    Here, we use another function in the terra_data_util notebook to use Terra's fiss API to load the consolidate metadata into a pandas dataframe:
    """
with herzog.Cell("python"):
    samples = get_terra_table_to_df(PROJECT, WORKSPACE, consolidated_table_name)
    samples
with herzog.Cell("python"):
    # We modify the first column of the dataframe to be relevant to TOPMed nomenclature
    samples.rename(columns={'entity:consolidated_metadata_id': 'subject_id'}, inplace=True)
with herzog.Cell("python"):
    # Verify the data
    samples
with herzog.Cell("markdown"):
    """
    # Examine sample phenotypes

    Now let's take a look at the phenotype distributions. In a GWAS - and statistical genetics more generally - we should always be on the lookout for correlations within our dataset. Correlations between phenotypic values can confound our analysis, leading to results that may not represent true genetic associations with our traits. Exploring these relationships may help in choosing a reasonable set of covariates to model.

    When generating plots, try to think about what we would expect a trait distribution to look like. Should it be uniform? skewed? normal? What about the distribution of two traits? What would this look like if the traits are correlated? uncorrelated? What axes of variation might confound a clear conclusion about a trait? More plotting functions for exploring phenotype data is available in [this featured workspace](https://app.terra.bio/#workspaces/fc-product-demo/2019_ASHG_Reproducible_GWAS).

    Let's take a look at the distribution of an outcome, diastolic blood pressure values, across other categorical phenotypic variables that may covary with this phenotype.

    ## Plot diastolic blood pressure by gender in this cohort

    Depending on the project you imported and your phenotypes of interest, you will need to update the inputs for x, y, and hue.
    """
with herzog.Cell("python"):
    plt.rcParams["figure.figsize"] = [12, 9]
    ax = sns.boxplot(x="demographic_annotated_sex", y="blood_pressure_test_bp_diastolic", hue="medication_antihypertensive_meds", data=samples, palette=sns.xkcd_palette(["windows blue", "amber"]))
with herzog.Cell("markdown"):
    """
    ## Plot systolic blood pressure distribution by sex and antihypertensive medications
    """
with herzog.Cell("python"):
    # Let's also look at systolic blood pressure by gender in this cohort
    ax = sns.boxplot(x="demographic_annotated_sex", y="blood_pressure_test_bp_systolic", hue="medication_antihypertensive_meds", data=samples, palette=sns.xkcd_palette(["windows blue", "amber"]))
with herzog.Cell("markdown"):
    """
    ## Plot age at the time of the bp reading by gender
    """
with herzog.Cell("python"):
    ax = sns.boxplot(x="demographic_annotated_sex", y="blood_pressure_test_age_at_bp_systolic", data=samples, palette=sns.xkcd_palette(["windows blue"]))
with herzog.Cell("markdown"):
    """
    # Subset the dataframe to include only the metadata we are interested in for this analysis
    """
with herzog.Cell("python"):
    #Select the metadata we want to use and check our output
    samples_traits_for_analysis = samples[["subject_id", "sample_submitter_id", "demographic_annotated_sex", "blood_pressure_test_age_at_bp_systolic", "medication_antihypertensive_meds", "blood_pressure_test_bp_diastolic", "blood_pressure_test_bp_systolic"]]
    samples_traits_for_analysis.head()
with herzog.Cell("python"):
    #Rename the second column to TOPMed nomenclature
    samples_traits_for_analysis.rename(columns={'sample_submitter_id': 'nwd_id'}, inplace=True)
    samples_traits_for_analysis.head()
with herzog.Cell("python"):
    #Reformat some of the data for downstream analyses
    samples_traits_for_analysis.medication_antihypertensive_meds = samples_traits_for_analysis.medication_antihypertensive_meds.replace(['Not taking antihypertensive medication', "Taking antihypertensive medication"], [0, 1])
    samples_traits_for_analysis.demographic_annotated_sex = samples_traits_for_analysis.demographic_annotated_sex.replace(['male', 'female'], ['M', 'F'])
with herzog.Cell("python"):
    #Check that formatting is correct
    samples_traits_for_analysis
with herzog.Cell("python"):
    #Sum missing data for each column in the dataframe
    samples_traits_for_analysis.isnull().sum()
with herzog.Cell("python"):
    #Remove rows with missing data
    samples_traits_for_analysis = samples_traits_for_analysis.dropna()
with herzog.Cell("python"):
    #Check the dataset for duplicates of the TOPMed NWD_ID entity. Duplication should not be the case, but can occur.
    #samples_traits_for_analysis['submitter_id'].duplicated()
    len(samples_traits_for_analysis['subject_id'].unique())
with herzog.Cell("python"):
    #Drop duplicates
    samples_traits_for_analysis = samples_traits_for_analysis.drop_duplicates(subset='subject_id')
with herzog.Cell("python"):
    #Check the data
    samples_traits_for_analysis
with herzog.Cell("python"):
    #Check out the distributions of the phenotypic data
    samples_traits_for_analysis.describe()



with herzog.Cell("markdown"):
    """
    # Save sample metadata and update data table
    Now that we've explored the phenotypes, the next step is to save and push our results back to the workspace data model. Once we populate the data model, we can use it for downstream analyses.
    """



with herzog.Cell("python"):
    # Change the column names of our data
    # You can see what column headers are required for the Genesis workflows (for example, sex)
    col_map = {'s': 'nwd_id',
               'pheno.subject_id': 'subject_id',
               'pheno.blood_pressure_test_age_at_bp_systolic': 'age_at_bp_systolic',
               'pheno.demographic_annotated_sex': 'sex',
               'pheno.medication_antihypertensive_meds': 'antihypertensive_meds',
               'pheno.blood_pressure_test_bp_diastolic': 'bp_diastolic',
               'pheno.blood_pressure_test_bp_systolic': 'bp_systolic',
               }
    samples_traits_for_analysis.rename(columns=col_map, inplace=True)
with herzog.Cell("python"):
    # Check that this work
    # Note that because we used the nwd_id as the key to match the phenotypic to genotypic data,
    # nwd_id is now the firt column
    samples_traits_for_analysis.head()
with herzog.Cell("markdown"):
    """
    ## Write the phenotype data to a new file
    """
with herzog.Cell("python"):
    samples_traits_for_analysis.to_csv(phenotype_out, index=False)
with herzog.Cell("markdown"):
    """
    ## Move the phenotype files to the workspace bucket
    """
with herzog.Cell("python"):
    #!gsutil cp {phenotype_out} {bucket + phenotype_out}
    pass





with herzog.Cell("markdown"):
    """
    ## Save the notebook and an HTML rendering to the workspace bucket
    """
with herzog.Cell("python"):
    #%notebook {notebook_out}
    #!jupyter nbconvert --to html {notebook_out}
    #!gsutil cp {notebook_out} {bucket + notebook_out}
    #!gsutil cp {html_out} {bucket + html_out}
    pass
with herzog.Cell("python"):
    elapsed_notebook_time = time.time() - start_notebook_time
    print(timedelta(seconds=elapsed_notebook_time))
