# Notebook author: Beth Sheets
# Herzog version: Ash O'Farrell

# Notebook title: 2-GWAS-initial-analysis

import os
import herzog

from unittest import mock
# Certain tasks (e.g. making a boxplot) make it challenging to create robust automated testing for this notebook.
# In order to get it to pass Python execution, many objects are mocked with unittest.mock.MagicMock. These have been
# marked with `# test fixture`. This will at least catch some syntax errors.
#
# In the meantime, this notebook should be tested manually in a Terra notebook environment.

get_terra_table_to_df = mock.MagicMock()  # test fixture

import seaborn as sns
sns.jointplot = mock.MagicMock()  # test fixture
# boxPlot is mocked later in the notebook

# Mock the environment
os.environ['WORKSPACE_NAME'] = "cicd-tester-1000genomes-gwas"
os.environ['WORKSPACE_BUCKET'] = "gs://fc-eb68164b-bae8-4892-83b8-637c1385b09a"
os.environ['GOOGLE_PROJECT'] = "firecloud-cgl"

with herzog.Cell("markdown"):
    """
    # GWAS Initial Analysis
    *version: 2.0.5*

    # Introduction
    ----

    This notebook demonstrates typical initial steps in a genetic association analysis: exploring phenotype distributions, filtering and PD-pruning, and Principal Component analysis. It has been written to be interactive, allowing you to make choices as you go.
        You should treat this notebook as a real analysis, where you will need to carefully consider aspects of the dataset to accurately model genotype-phenotype relationships. In particular, you will want to identify covariation of traits and population structure. These two aspects are among the most common causes of confounding within a GWAS.

    By the end of this notebook, you should understand:
        1. How to import phenotypic data from a Terra data table to a notebook compute environment
        2. Exploring and processing these data to understand their underlying structure
        3. Defining an outcome and a set of covariates to use when modeling genotype-phenotype associations
        4. Importing, exploring, and performing quality control on genotype data
        5. Understanding population structure within the 1000 Genomes sample
        6. How to prepare a full set of input parameters and data for a genomewide association analysis pipeline
        7. How to configure the Terra data model to easily run the association pipeline

    ## Data disclaimer

    All data in this notebook (and this workspace) are publicly available thanks to the effort of many dedicated individuals:
    * Genotype and some phenotypic data were produced by the [1000 Genomes Project (phase 3)](https://www.internationalgenome.org/)
    * Individual phenotypes were modeling using the [GCTA software](cnsgenomics.com/software/gcta) and variant-level summary statistics from [MAGIC](https://www.magicinvestigators.org/), the [GIANT Consortium](https://portals.broadinstitute.org/collaboration/giant/index.php/Main_Page), the [UK Biobank](https://www.ukbiobank.ac.uk/), and the [MVP](https://www.research.va.gov/mvp/)

    Phenotypes were modeled to reflect the actual genetic architecture of these complex traits as closely as possible. Most single variant association results should correspond well to published GWAS, but others may not. **Results produced from these data should not be taken as representing real, replicable genetic associations. These data are provided for demonstration and training purposes only.**

    # Set up your notebook
    ----

    ## Recommended runtime values
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
    Click the "Replace" button when you are done, and Terra will begin to create a new runtime with your settings. When it is finished, it will pop up asking you to apply the new settings.

    ### A note on runtime settings
    We're catering this VM to a relatively small dataset. When using your own data, you need to consider the file size of your VCFs, which will affect not just disk size but also memory. Additionally, more workers will help the analysis go quicker.

    ## Check kernel type
    A kernel is a _computational engine_ that executes the code in the notebook. You can think of it as defining the programming language. For this notebook, we'll use a `Python 3` kernel. In the upper right corner of the notebook, just under the Notebook Runtime, it should say `Python 3`. If it doesn't, you can switch it by navigating to the Kernel menu and selecting `Change kernel`.

    ## Install packages
    """

with herzog.Cell("python"):
    #%pip install tenacity
    pass

with herzog.Cell("markdown"):
    """
    Make sure to restart the kernal after pip installing anything.
    ## Load Python packages
    ----

    * **FISS** - a toolkit for using Terra APIs through Python
    * **Pandas & Numpy** - packages for data analysis
    * **Matplotlib & Seaborn** - for plotting
    To see the entire list of Python packages, click the purple arrow to the right below.
    """

with herzog.Cell("python"):
    #%%capture
    from firecloud import fiss
    import os
    import io
    import pandas as pd
    import numpy as np
    import matplotlib.pyplot as plt
    import seaborn as sns

with herzog.Cell("markdown"):
    """
    # Load phenotypes

    Phenotypic data for each individual in the study are stored in the workspace data table. To analyze inside this notebook, we have to explicitly load the data in our notebook environment. To do this, we'll need some information about the Terra Workspace. This can be access programmatically using some environmental variables.

    ## Goal of this section

    Use the FISS package to import data referenced in the workspace data table into the notebook environment

    ## Set environment variables
    """

with herzog.Cell("python"):
    # Get the Google billing project name and workspace name
    PROJECT = os.environ['GOOGLE_PROJECT']
    WORKSPACE = os.environ['WORKSPACE_NAME']
    bucket = os.environ['WORKSPACE_BUCKET'] + "/"

    # Verify that we've captured the environment variables
    print("Billing project: " + PROJECT)
    print("Workspace: " + WORKSPACE)
    print("Workspace storage bucket: " + bucket)

with herzog.Cell("python"):
    second_notebook_pheno_data = "nb2pheno.csv"

with herzog.Cell("markdown"):
    """
    ## Load phenotype data

    We'll use <font color='red'>read_csv</font> to load the phenotypes from the workspace data table. The columns correspond to:
    * **sample:** a unique label for each individual sample in our dataset
    * **age:** numerical age of the individual at the time of each phenotype measure
    * **bmi:** body mass index
    * **fg:** fasting glucose
    * **fi:** fasting insulin
    * **hdl:** high density lipoprotein
    * **height:** standing height
    * **ldl:** low density lipoprotein
    * **population:** population of each sample, see [1000 Genomes description](https://www.internationalgenome.org/category/population/)
    * **sex:** biological sex
    * **tc:** total cholesteral
    * **tg:** total triglycerides
    """

with herzog.Cell("python"):
    # Run the companion notebook to define useful functions.
    # Note: it must be in the same workspace you are currently working in.
    #%run terra_data_table_util.ipynb
    consolidated_table_name = "consolidated_metadata"

with herzog.Cell("python"):
    # Pull the phenotypic data from the consolidated table into a pandas dataframe
    samples = get_terra_table_to_df(PROJECT, WORKSPACE, consolidated_table_name)

    # Print out the top few rows (notice the number of columns)
    samples.head()

with herzog.Cell("markdown"):
    """
    ### If you get an error when running the code above

    The command above uses the FISS API. Sometimes the Terra Firecloud Service gets an error that requests you retry after 30 seconds. If you get an error in the 500 range, we suggest that you restart the notebook kernel. If that doesn't work, you can restart you notebook runtime (but remember to save your work using gsutil whenever you do this option).
    """

with herzog.Cell("python"):
    # We modify the first column of the dataframe to be relevant to TOPMed nomenclature
    samples.rename(columns={'entity:consolidated_metadata_id': 'subject_id'}, inplace=True)

with herzog.Cell("markdown"):
    """
    # Examine phenotype data
    ----

    Now let's take a look at the phenotype distributions. In a GWAS - and statistical genetics more generally - we should always be on the lookout for correlations within our dataset. Correlations between phenotypic values can confound our analysis, leading to results that may not represent true genetic associations with our traits. Exploring these relationships may help in choosing a reasonable set of covariates to model.

    When generating plots, try to think about what we would expect a trait distribution to look like. Should it be uniform? skewed? normal? What about the distribution of two traits? What would this look like if the traits are correlated? uncorrelated? What axes of variation might confound a clear conclusion about a trait?

    We've included a number of plotting functions below to make this as easy as possible. Feel free to modify - or write your own functions - as you explore the data.
    ## Goals of this section

    1. Visualize the distribution of phenotype values
            - Within each continuous trait
                - using the kdplot function
            - Within each continuous trait, organized by dichotomous data
                - ex: the distribution of BMI in each ancestry group
                - using the boxPlot function
            - Between two continuous traits
                - with the bivariateDistributionPlot function
    2. Determine whether trait distributions follow patterns we might expect
    3. Choose an outcome and covariates to model in the second part of this workshop
    ## Code for plotting functions

    The next code block defines the plotting functions. Because running the plotting functions doesn't require understanding the syntax of the code, we have collapsed this block. Feel free to uncollapse (click the purple arrow at the right) if you're interested in all the details!
    """

with herzog.Cell("python"):
    # Define functions to easily plot phenotypes
    plt.rcParams["figure.figsize"] = [6, 4]

    # Visualize distribution with each continuous trait
    def kdPlot(data, var):
        sns.set_style("whitegrid")
        sns.set_context("poster",
                        font_scale=0.9,
                        rc={"grid.linewidth": 0.6, 'lines.linewidth': 1.6})
        sns.distplot(data[(var)])

    # Visualize the distribution between two continuous traits
    def bivariateDistributionPlot(data, var1, var2, kind="scatter"):
        with sns.axes_style("whitegrid"):
            jplot = sns.jointplot(x=data[var1], y=data[var2], kind=kind, color="k", s=1)
            jplot.set_axis_labels(var1, var2)

    # Visualize within each continuous trait, organized by dichotomous data
    def boxPlot(data, catagorical_var, continuous_var, color_by=None, force_x=False, force_color=False):  # type: ignore
        make_plot = True
        if len(data[catagorical_var].unique().tolist()) > 10 and force_x is not True:
            make_plot = False
            print("catagorical_var must be catagorical. If you insist on using these x values, set force_x=True.")
        if color_by is not None:
            if len(data[color_by].unique()) > 5 and force_color is not True:
                make_plot = False
                print("color_by column must be catagorical. If you insist on using these values, set force_color=True.")

        if (make_plot is True):
            sns.set_style("whitegrid")
            sns.set_context("poster",
                            font_scale=0.7,
                            rc={"grid.linewidth": 0.6, 'lines.linewidth': 1.6})
            sns.boxplot(x=catagorical_var,
                        y=continuous_var,
                        hue=color_by,
                        data=data,
                        palette=["#275F9A", "#A2C353"],
                        saturation=1)
            plt.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)

with herzog.Cell("markdown"):
    """
    ## Generating distribution plots

    <img src="https://raw.githubusercontent.com/tmajaria/ashg_2019_workshop/master/ldl_kdplot.png" align="left" width="20%">

    ***Univariate distributions*** are easily visualized in histograms or density plots. We provide a function (<font color='red'>kdplot</font>) that will generate both types of plots, overlayed in a single figure. A continuously-valued variable corresponding to a column in the phenotype dataframe should be used as input, *ldl* in this example. The function is called with the following syntax:

    ```python
    kdPlot(samples, var="ldl")
    ```

    <img src="https://raw.githubusercontent.com/tmajaria/ashg_2019_workshop/master/whr_hdl_bivariateDistributionPlot.png" align="left" width="20%">

    ***Bivariate distributions*** can be visualized using a scatterplot. Use the function <font color='red'>bivariateDistributionPlot</font> to visualize two continuously values variables. The *type* argument determines the type of plot generated and can be one of: "scatter", "reg", "resid", "kde", and "hex".

    ```python
    bivariateDistributionPlot(samples, var1="hdl", var2="whr", kind="scatter")
    ```

    <img src="https://raw.githubusercontent.com/tmajaria/ashg_2019_workshop/master/height_ancestry_boxPlot.png" align="left" width="20%">

    ***Boxplots*** can be used to further interogate relationships between continuous and categorical variables, using the <font color='red'>boxPlot</font> function. Distributions can be further subset by sex (by setting *color_by*="sex").

    ```python
    boxPlot(samples, catagorical_var="ancestry", continuous_var="height", color_by="sex")
    ```

    ### Exercise: Univariate distributions

    Use the code cells below to plot the distribution of single variables of your choice (such as ldl or bmi). You may need to refer to section 3.2 above for the list of variables and to section 4.1 for the plotting syntax.
    """

with herzog.Cell("python"):
    kdPlot(samples, var="lab_result_ldl")

with herzog.Cell("python"):
    kdPlot(samples, var="demographic_bmi_baseline")

with herzog.Cell("markdown"):
    """
    ### Exercise: Bivariate distributions

    Generate scatter plots with different combinations of variables. Think about what you would expect versus what you see in the plot. You may need to refer to 3.2 for the list of variables and to section 4.1 for the plotting syntax.
    """

with herzog.Cell("python"):
    bivariateDistributionPlot(samples, var1="demographic_bmi_baseline", var2="demographic_height_baseline", kind="scatter")

with herzog.Cell("python"):
    bivariateDistributionPlot(samples, var1="lab_result_hdl", var2="demographic_height_baseline", kind="scatter")

with herzog.Cell("markdown"):
    """
    ### Exercise: Boxplots

    Boxplots show relationships between continuous and categorical variables. Use the code cell below to generate a boxplot showing the relationship between height and ancestry. Try adding sex as another delineator with the `color_by` argument. You may need to refer to 3.2 for the list of variables and to section 4.1 for the plotting syntax.

    In the next code block, try a boxplot with variables of your choice.
    """
#
# the boxplot is a real pain for our CICD pipeline
# #%boxPlot isn't going to work
# so, we redefine it to a mock right before we use it
# this redefinition is outside a cell so it will not occur once in iPython form
#
sns.boxPlot = mock.MagicMock()  # test fixture
boxPlot = mock.MagicMock()  # test fixture # noqa
with herzog.Cell("python"):
    # Increase plot size to avoid overcrowding
    plt.rcParams["figure.figsize"] = [30, 10]
    boxPlot(samples, catagorical_var="demographic_population", continuous_var="demographic_height_baseline", color_by="demographic_annotated_sex", force_x=True)

with herzog.Cell("python"):
    # Select the metadata we want to use and check our output
    samples = samples[["subject_id", "demographic_age_at_index", "demographic_population", "demographic_bmi_baseline", "demographic_annotated_sex"]]
    samples.head()

    # Uncomment the code below to use all available phenotypes instead
    #samples = samples[["subject_id", "lab_result_age_at_ldl", "demographic_population", "demographic_bmi_baseline", "lab_result_glucos1c", "lab_result_inslnr1t", "lab_result_hdl", "demographic_height_baseline", "lab_result_ldl", "demographic_annotated_sex", "lab_result_total_cholesterol", "lab_result_triglycerides"]]

with herzog.Cell("python"):
    # Replace "male" and "female" with "M" and "F"
    samples.demographic_annotated_sex = samples.demographic_annotated_sex.replace(['male', 'female'], ['M', 'F'])
    samples.head()

with herzog.Cell("markdown"):
    """
    # Save sample metadata and update data table
    Now that we've explored the phenotypes, the next step is to save to the workspace bucket. Once we populate the data model, we can use it for downstream analyses.
    ## Write the phenotype data to a new file
    """

with herzog.Cell("python"):
    samples.to_csv(second_notebook_pheno_data, index=False)

with herzog.Cell("markdown"):
    """
    ## Save the phenotype files to the workspace bucket
    We'll need those files later on in the next notebook in this workspace. By saving it to the bucket, we ensure that we can still access it.
    """

with herzog.Cell("python"):
    #!gsutil cp {second_notebook_pheno_data} {bucket + second_notebook_pheno_data}
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
