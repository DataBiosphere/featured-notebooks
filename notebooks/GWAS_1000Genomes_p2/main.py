# Notebook author: Beth Sheets
# Herzog version: Ash O'Farrell

# Notebook title: 2-GWAS-initial-analysis

import os
import herzog

from unittest import mock
# Heavyweight dependencies (e.g. hail, Spark) make it challenging to create robust automated testing for this notebook.
# In order to get it to pass Python execution, many objects are mocked with unittest.mock.MagicMock. These have been
# marked with `# test fixture`. This will at least catch some syntax errors.
#
# In the meantime, this notebook should be tested manually in a Terra notebook environment.

get_terra_table_to_df = mock.MagicMock()  # test fixture
get_ipython = mock.MagicMock()  # test fixture

import seaborn as sns
sns.jointplot = mock.MagicMock()  # test fixture
# boxPlot, hail, and bokeh are mocked further down the line

# Mock the environment
os.environ['WORKSPACE_NAME'] = "cicd-tester-1000genomes-gwas"
os.environ['WORKSPACE_BUCKET'] = "gs://fc-eb68164b-bae8-4892-83b8-637c1385b09a"
os.environ['GOOGLE_PROJECT'] = "firecloud-cgl"

with herzog.Cell("markdown"):
    """
    # GWAS Initial Analysis
    *version: 2.0.3*

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

    ## Set runtime values
    If you are opening this notebook for the first time, and you did not edit your runtime settings before starting it, you will now need to change your settings. Click on the gear icon in the upper right to edit your Notebook Runtime. Set the values as specified below:
    <table style="float:left">
        <thead>
            <tr><th>Option</th><th>Value</th></tr>
        </thead>
        <tbody>
             <tr><td>Application Configuration</td><td>Hail: (Python 3.7.9, Spark 2.4.5, hail 0.2.61)</td></tr>
                          <tr><td>CPUs</td><td>8</td></tr>
                          <tr><td>Memory (GB)</td><td>30</td></tr>
                          <tr><td>Disk size (GB)</td><td>100</td></tr>
                          <tr><td>Startup script</td><td>(leave blank)</td></tr>
                          <tr><td>Compute Type</td><td>Spark cluster</td></tr>
             <tr><td>Workers</td><td>4</td></tr>
                          <tr><td>Preemptible</td><td>0</td></tr>
                          <tr><td>Workers CPUs</td><td>4</td></tr>
                          <tr><td>Workers Memory (GB)</td><td>15</td></tr>
             <tr><td>Workers Disk size (GB)</td><td>50</td></tr>
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
    * **Pprint** - for pretty printing
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
    from pprint import pprint
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
# the boxplot is a real pain for our pipeline
# #%boxPlot isn't going to work
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
    # Working with genotype data using Hail
    ----

    Now that we have a handle on the phenotype data, we can begin to work with the genotype data. We will make use of the [Hail](hail.is) software, an "open-source, general-purpose, Python-based data analysis tool with additional data types and methods for working with genomic data." Hail utilizes distributed computing with Apache Spark for efficient genome-wide analysis.
    ## Goals of this section
    1. Load genotype data from variant call format (VCF) files into the compute environment
    2. Understand how to access genotype and variant information
    3. Generate variant quality control metrics for the 1000 Genomes data

    ### A short Hail primer

    Hail uses distributed computing -- a way of parallelizing tasks that can greatly decrease the **real** time it takes to complete an analysis. While distributed computing decreases **real** time, the time that you or I experience, it does not decrease **computational** time, the product of *# machines* and *time each machine was running*. Distributed computing relies on an operation called *partitioning*, dividing a object into many, many pieces. The number of partitions determines, in part, the number of operations that can run in parallel. In the code below, you will see syntax like `min_partitions = 200`. In English, this is saying "at the minimum, divide my data into 200 pieces". This parameter can vary and should be determined based on the data and the particular operations that you want to do -- more partitions is not always better.

    It is important to note that Hail expressions are **lazy** -- they are not evalutated until absolutely needed. For example, using Python for the expression `x = 2+2` would immediately store the value of `x` as `4`. However, calling the same expression using Hail would instead store `x` as `2+2` until the variable `x` was explicitly used. When using some of the code below, you will notice that some code blocks will run astoundingly quickly. This does not mean that the operation is complete, rather that the operation's result was not yet needed. When a results is needed, the operation will run. This should become more clear when working through this notebook.
    For more information, checkout the [Hail documentation](http://www.nealelab.is/tools-and-software) and this [helpful video](https://youtu.be/0RTgBYL5x_E).

    ## Query workspace storage for VCF files
    To find the correct VCF files for the analysis, use [Gsutil](https://cloud.google.com/storage/docs/gsutil), a command line program for accessing data from Google Cloud Storage, directly within a notebook. The <font color='red'>!</font> character has special meaning within this Jupyter notebook. It can be used to call command line functions directly and often is referred to as a [magic command](https://ipython.readthedocs.io/en/stable/interactive/magics.html).

    For this notebook, we've hard-coded the VCF paths. **If you wanted to use different data, you would need to change this**.
    """

with herzog.Cell("python"):
    vcf_base = "gs://terra-featured-workspaces/GWAS/1kg-genotypes/subset/*.vcf.bgz"

    # Use gsutil to assign the list of the files to a variable
    vcf_paths = get_ipython().getoutput('gsutil ls {vcf_base}')

    # Print a few of the paths to verify
    pprint(vcf_paths[1:3])

with herzog.Cell("markdown"):
    """
    ##### Import packages and start a Hail session

    * **Hail** - an open-source, general-purpose, Python-based data analysis tool with additional data types and methods for working with genomic data
    * **Bokeh** - an interactive visualization library
    """

with herzog.Cell("python"):
    # Import some packages we will use, and set some parameters so plots render nicely
    import hail as hl
    import bokeh.io as bokeh_io
    from bokeh.resources import INLINE
    bokeh_io.output_notebook(INLINE)
    get_ipython().run_line_magic('matplotlib', 'inline')

bokeh_io = mock.MagicMock()  # noqa # test fixture
hl = mock.MagicMock()  # noqa # test fixture
with herzog.Cell("python"):
    # After importing, start a Hail session
    hl.init(default_reference="GRCh37", log='tutorial-analysis.log')

with herzog.Cell("markdown"):
    """
    ## Load VCF data and perform variant QC

    ### Load 1000 Genomes data
    To load genotype data from VCF files, use the <font color='red'>import_vcf</font> function. This will convert the VCF files into a **matrix table** (**mt**). A matrix table is composed of 3 parts: sample annotations (columns), variant annotations (rows), and entries (genotypes). They are optimized to allow for fast access and computation by storing small pieces of each file independently. Use the following syntax to load the 1000 Genomes data from the variable `vcf_paths` we created earlier. `vcf_paths` lists the links to all the genotype files.
    """

with herzog.Cell("python"):
    mt = hl.import_vcf(vcf_paths)
mt = mock.MagicMock()
with herzog.Cell("markdown"):
    """
    ### View matrix table structure
    Use the <font color='red'>describe</font> function to view the structure of the matrix table:
    """

with herzog.Cell("python"):
    mt.describe()

with herzog.Cell("markdown"):
    """
    **Count columns and rows**: How many variants and samples are there in your matrix table? Note that the `count` function can take a few minutes. It's a big dataset!
    """

with herzog.Cell("python"):
    mt.count()

with herzog.Cell("markdown"):
    """
    ### Merge phenotype and VCF data

    You can merge phenotypes with the VCF data by matching sample IDs between objects. Recall that the *sample* column of the phenotype data held unique IDs for each sample. These same sample IDs are stored in the VCF files and index the matrix table columns. <font color='red'>annotate_cols</font> can be used to add the phenotypes to the matrix table columns. `samples[mt.s]` matches samples IDs between objects and <font color='red'>annotate_cols</font> merges into the VCF:
    """

with herzog.Cell("python"):
    # First convert the phenotypes to a Hail table
    samples = hl.Table.from_pandas(samples, key='subject_id')

with herzog.Cell("python"):
    # Then annotate the matrix table by matching the sample IDs
    mt = mt.annotate_cols(pheno=samples[mt.s])

with herzog.Cell("python"):
    # Take a look at the first few rows of the matrix table
    mt.cols().show(5)

with herzog.Cell("markdown"):
    """
    ### Generate variant level summary statistics

    To generate variant level summary statistics, use <font color='red'>variant_qc</font>. This will compute useful metrics like allele frequencies, call rate, and homozygote counts, among many others. Run variant_qc and take a look at how the matrix table structure changes.
    """

with herzog.Cell("python"):
    mt = hl.variant_qc(mt)

with herzog.Cell("markdown"):
    """
    Then take a look at how the matrix table structure changes: use <fibt color="red">describe</font> and you should see a new set of annotations added to the table (under variant_qc). Then use `mt.rows().show(5)` to see the first few variants and their annotations (scroll down to the end).
    """

with herzog.Cell("python"):
    mt.describe()

with herzog.Cell("python"):
    mt.rows().show(5)

with herzog.Cell("markdown"):
    """
    # Understanding population structure within our sample
    ----

    Many of our statistical tests are built on the assumption that data points are unrelated and require adjustment to account for population structure. There are various ways to quantify population structure, but most start by generating a set of markers (variants) that are nearly independent of one another. For this, we will use an operation called *Linkage Disequalibrium pruning* to extract a set of variants that we can use for calculating relatedness. See [this resource](https://en.wikipedia.org/wiki/Linkage_disequilibrium) for more information on LD.

    Next, we will use principal component analysis (PCA) to transform the genetic data into a space that will aid in modeling by allowing us to more easily visualize genetic distance between individuals.

    ## Goals of this section

    1. Generate a list of variants for calculated relatedness by filtering and LD-pruning
    2. Calculate principal components using Hail
    3. Visualize individuals within PC space

    ## Variant filtering

    To visualize population structure within our sample, we'll first generate a set of common, independent (relative to linkage disequalibrium) variants. This will ensure good representation across samples, and eliminate redundant information that may bias our results.

    ### Exercise: Filter variants
    First, to find where allele frequency is stored, use the <font color=red>describe</font> function again. (<font color='blue'>Hint:</font> There are two values for allele frequency, one each for the reference and alternate alleles. In our biallelic dataset, the reference frequency is the first value and the alternate frequency second.)
    """

with herzog.Cell("python"):
    mt.describe()

with herzog.Cell("markdown"):
    """
    From this output, we see a nested structure called `variant_qc`. The field we are interested in is `AF`, which stands for allele frequency. `AF`'s type is an array. This array contains reference allele frequencies (at index 0) and alternate allele frequencies (at index 1).

    Variants can be filtered using <font color='red'>filter_rows</font> and conditioning on allele frequency. <font color='red'>filter_rows</font> takes an expression. We want to filter to variants with alternate frequency > 5% using the expressions below.
    """

with herzog.Cell("python"):
    alleleFreq = mt.variant_qc.AF[1]

# Sneaky workaround to avoid TypeError: '>' not supported between instances of 'MagicMock' and 'float'
alleleFreq = 0

with herzog.Cell("python"):
    mt = mt.filter_rows(alleleFreq > 0.05)

with herzog.Cell("markdown"):
    """
    **Check filtering results**: To take a look at how many variants remain in your dataset after filtering, use the <font color="red">count</font> function.
    """

with herzog.Cell("python"):
    mt.count()

with herzog.Cell("markdown"):
    """
    ## LD-pruning

    ### Exercise: Run LD pruning

    We'll want to only include variants that are (nearly) independent of each other. We'll accomplish this using linkage disequalibrium pruning with the <font color='red'>ld_prune</font> function. Inputs are the genotypes, an r<sup>2</sup> threshold, and a window size. The r<sup>2</sup> threshold and window size control how strict we are in our definition of independence. The final parameter, *block_size*, relates to parallelization and should not be changed. More information can be found in the  [Hail documentation](https://www.hail.is).

    Note that this command takes some time to run.
    """

with herzog.Cell("python"):
    pruned_variants = hl.ld_prune(mt.GT, r2=0.2, bp_window_size=100000, block_size=1024)

with herzog.Cell("markdown"):
    """
    **Check pruning**: Take a look at how many variants LD pruning retains with the <font color="red">count</font> function. (<font color="blue">Hint</font>: <font color="red">describe</font> the table first so you know what the count is counting)
    """

with herzog.Cell("python"):
    pruned_variants.describe()

with herzog.Cell("python"):
    pruned_variants.count()

with herzog.Cell("markdown"):
    """
    Since this is a count of how many variants we are pruning down to, the amount we are retaining is 25409.

    ### Exercise: Filter the matrix table
    The last step is to filter the matrix table again by the pruned variants list. For this, <font color='red'>is_defined</font> is useful:
    """

with herzog.Cell("python"):
    mt = mt.filter_rows(hl.is_defined(pruned_variants[mt.row_key]))

with herzog.Cell("markdown"):
    """
    **Sanity check**:
    How many variants are left after pruning? Use the count function to see the effect of pruning on the size of the matrix table:
    """

with herzog.Cell("python"):
    mt.count()

with herzog.Cell("markdown"):
    """
    ## Principal Component Analysis

    In this next section, we'll cover a method for easily visualizing and adjusting for population structure in an association analysis: Principal Component Analysis (PCA).

    ### Exercise: Run PCA

    You run PCA using the function <font color='red'>hwe_normalized_pca</font>. For this analysis, we are mainly interested in the scores, and can disregard the eigenvalues and loadings. The `k` parameter determines the number of PCs to return -- as `k` grows, so does the computation time.
    """
mt.GT = [1, 2, 3]
with herzog.Cell("python"):
    _, pcs, _ = hl.hwe_normalized_pca(mt.GT, k=5)

with herzog.Cell("markdown"):
    """
    ### Add PCA values to matrix table
    The PC Scores can then be added to the matrix table in much the same way as phenotypes. Use <font color='red'>annotate_cols</font>. (<font color='blue'>Hint:</font> the pcs object has multiple fields, use <font color='red'>describe</font> to find the field that you want to keep. Don't forget to match over the sample IDs, which is the row field `s`.)
    """

with herzog.Cell("python"):
    # Use describe to find the PC fields you want to keep
    pcs.describe()

with herzog.Cell("python"):
    # Add to the matrix table
    mt = mt.annotate_cols(scores=pcs[mt.s].scores)

with herzog.Cell("markdown"):
    """
    ### Visualize samples in PCA space

    Finally, let's visualize the results. Hail has some efficient built-in plotting functions -- let's use <font color='red'>plot.scatter</font>. Choose which PCs you'd like to plot and a categorical phenotype value to color the points.
    """

with herzog.Cell("python"):
    p = hl.plot.scatter(mt.scores[0],
                        mt.scores[1],
                        label=mt.pheno.demographic_population,
                        title='PCA', xlabel='PC1', ylabel='PC2')
    bokeh_io.show(p)

with herzog.Cell("markdown"):
    """
    # Generate a genetic relatedness matrix (GRM)

    Hail has built-in functions for generating a GRM. A GRM can also account for population stratification and cryptic genetic relatedness in our cohort.
    """

with herzog.Cell("python"):
    # Calculate the GRM
    grm = hl.genetic_relatedness_matrix(mt.GT).to_numpy()

with herzog.Cell("python"):
    # Get the right sample order
    ind_order = mt.s.collect()

with herzog.Cell("python"):
    # GRM to data frame
    rel_pd = pd.DataFrame(data=grm,
                          index=ind_order,
                          columns=ind_order)

with herzog.Cell("python"):
    # Export GRM
    rel_pd.to_csv("kinship.csv")

with herzog.Cell("markdown"):
    """
    # Save outputs and update the data table

    Now that you've explored the phenotypes and a measure of population stratification, the next step is to save the results and push them back to the workspace data table. Once it's in the data table, you can use the data for downstream analyses.

    ## Goals of this section

    1. Create a final phenotype file that includes only the outcomes and covariates desired for modeling
    2. Generate a sample set for the workspace data table
    3. Push all results generated in this notebook back to the workspace storage bucket, and update the data table

    ## Convert the phenotype data to the correct format

    Now that the hard computation work is done, we need to choose the data you'd like to keep, and export to a format that can be read downstream. Earlier in this notebook, you should have been thinking about which outcome and covariates you would like to model. You'll use these in a call to <font color='red'>select</font> to generate a single phenotype file with only the data you want.

    ### Extract sample metadata
    First, extract the sample metadata from the matrix table using <font color='red'>cols</font>:
    """

with herzog.Cell("python"):
    samples = mt.cols()

with herzog.Cell("markdown"):
    """
    ### Convert data format
    The *samples* table generated in the last bit of code needs a little post processing. These steps are just to get the data into a format that is easier to work with, and their explanation is beyond the scope of this workshop.

    **Note:** If you saved the phenotypes or PCA scores to different variable names, you will need to adjust the code below. <font color='red'>describe</font> may help in finding the right column names.
    """

with herzog.Cell("python"):
    samples = samples.key_by().select('s', 'pheno', 'scores')

with herzog.Cell("markdown"):
    """
    ### Flatten the PC array
    If you would like to use PCs in downstream modeling, you will need to *flatten* the PC array (<font color='red'>describe</font> the table and see the type of the *scores* attribute). You can assign data to new columns in the table by using <font color='red'>annotate</font>:
    """

with herzog.Cell("python"):
    samples = samples.annotate(PC1=samples.scores[0], PC2=samples.scores[1])

with herzog.Cell("markdown"):
    """
    ### Convert the phenotype table to a Pandas data frame

    Hail tables are a bit difficult to write out to a regular format. You'll also want to drop the original PC scores as you made new columns for them above. It is much easier using Pandas. Convert the phenotypes into a data frame using:
    """

with herzog.Cell("python"):
    samples = samples.drop('scores').to_pandas()

with herzog.Cell("markdown"):
    """
    ### Convert data and check results
    Finally, convert the data to local dataframe and checkout the results in much the same way as with the GRM above. Write the data out and push it back to the workspace storage. (<font color='blue'>Hint:</font> The column names in the resulting data frame will reflect the nested structure of the table.)
    """

with herzog.Cell("python"):
    col_map = {'s': 'subject_id',
               'pheno.lab_result_age_at_ldl': 'age',
               'pheno.demographic_population': 'population',
               'pheno.demographic_bmi_baseline': 'bmi',
               #'pheno.lab_result_glucos1c': 'glucose',
               #'pheno.lab_result_inslnr1t': 'insulin',
               #'pheno.lab_result_hdl': 'hdl',
               #'pheno.demographic_height_baseline': 'height',
               #'pheno.lab_result_ldl': 'ldl',
               'pheno.demographic_annotated_sex': 'sex'
               #'pheno.lab_result_total_cholestrol': 'total_cholestrol',
               #'pheno.lab_result_triglycerides': 'triglycerides'
               }
    samples.rename(columns=col_map, inplace=True)

with herzog.Cell("python"):
    samples.head()

with herzog.Cell("markdown"):
    """
    ### Write derived data out to files and upload to the workspace storage

    Use *gsutil* to move the new genotype and phenotype files to the workspace storage. Name each file something meaningful to your analysis.
    """

with herzog.Cell("python"):
    # Save to your kinship matrix to workspace bucket
    #!gsutil cp kinship.csv {bucket}
    pass

with herzog.Cell("python"):
    # Write Hail matrix as a VCF to your notebook VM
    mt = mt.repartition(25)
    hl.export_vcf(mt, bucket + 'MyProject_MAFgt0.05.vcf.bgz', parallel='header_per_shard')

    # Use gsutil to move the file to the workspace bucket, since workflows cannot access data
    # stored in the notebook runtime.
    #!gsutil ls bucket + 'MyProject_MAFgt0.05.vcf.bgz/*'

with herzog.Cell("markdown"):
    """
    Here, we create an array pointing to all of the shards that make up the common variants for our dataset. We use this array in the last section of this notebook called "Generate a new Terra data model called "sample_set" and add all of our derived data files to this entity (table).
    """

with herzog.Cell("python"):
    vcf_filtered_base = bucket + 'MyProject_MAFgt0.05.vcf.bgz/*.bgz'
    vcf_filtered_path = get_ipython().getoutput('gsutil ls {vcf_filtered_base}')
    vcf_filtered_array = vcf_filtered_path
    vcf_filtered_array

with herzog.Cell("python"):
    # Write phenotypes and PC scores to a csv file
    samples.to_csv("my_phenotypes.csv", index=False)

    # Use gsutil to move the file to the workspace bucket, since workflows cannot access data
    # stored in the notebook runtime.
    #!gsutil cp my_phenotypes.csv {bucket}

with herzog.Cell("markdown"):
    """
    ## Add to the workspace data model

    The last step in the analysis is to update **your** workspace data table with the data generated in this notebook. For downstream analysis, you'll want to set up all the needed parameters and input data directly in the data table. As you will see, analysis workflows look to the data table for configuration and inputs.

    Recall that each *sample* in the data table corresponded to a single individual. To run a GWAS analysis requires a set of samples, or in Terra terms, a **sample_set**. This entity should include everything you might need to run a workflow for genotype-phenotype modeling: a list of samples to include, phenotype data, a list of covariates, an outcome, a genetic relatedness matrix, and a label. The particular workflow that you will soon run looks for inputs in specific columns of the data table.

    Below is a helpful function for generating a sample set in the correct format so you will have success in the next part this workshop. To look at the full code, click the arrow at the top left of the cell.
    """

with herzog.Cell("python"):
    def makeSampleSet(samples, PROJECT, WORKSPACE, label, phenotype_file, sample_id_column="sample_id", outcome="bmi", covariates=None):
        sample_set = {
            'entity:sample_set_id': label,
            'phenotypes': phenotype_file,
            'outcome_name': outcome,
            'covariates': covariates,
            'grm': grm,
            'sample_id_column': sample_id_column}

        if type(covariates) == list:
            sample_set['covariates'] = ", ".join(covariates)
        elif covariates is not None:
            sample_set['covariates'] = covariates.replace(" ", ", ")
        else:
            del sample_set['covariates']

        entity = '\n'.join(['\t'.join(sample_set.keys()), '\t'.join(sample_set.values())])
        fiss.fapi.upload_entities(PROJECT, WORKSPACE, entity)

        membership = 'membership:sample_set_id\tsample\n'
        for i in range(0, samples.shape[0]):
            membership += label + '\t' + samples.iloc[i, 0] + '\n'
        fiss.fapi.upload_entities(PROJECT, WORKSPACE, membership)

with herzog.Cell("markdown"):
    """
    ### Exercise: Create a sample set and update the data table

    To use the <font color='red'>makeSampleSet</font> function, you will first need to gather a few things:

    1. a label, or some short discriptor, for the sample set
    2. the phenotype filepath in the workspace storage (this should start with *gs://*)
    3. the column name in the phenotype file that corresponds to the sample IDs (if you followed the hint above, this should be *sample_id*)
    4. the outcome you would like to model (this must be a column name in the phenotype file)
    5. a list of covariates to include in the model (these should be column names in the phenotype file separated by commons)

    You can <font color='red'>makeSampleSet</font> with the following syntax. If you'd like to track different covariates and outcomes, you can change those values
    """

with herzog.Cell("python"):
    label = "tutorial-analysis-vcfupdate"
    phenotype_file = bucket + "my_phenotypes.csv"
    sample_id_column = "subject_id"
    outcome = "bmi"
    covariates = "age,sex,population"
    grm = bucket + "kinship.csv"
    makeSampleSet(samples, PROJECT, WORKSPACE, label, phenotype_file, sample_id_column, outcome, covariates)

with herzog.Cell("python"):
    vcf_attribute = [fiss.fapi._attr_set("vcfs", vcf_filtered_array)]
    fiss.fapi.update_entity(PROJECT, WORKSPACE, 'sample_set', 'tutorial-analysis-vcfupdate', vcf_attribute)

with herzog.Cell("markdown"):
    """
    # Next steps
    Congratulations on completing the first steps in your GWAS analysis! You've loaded and explored the phenotype data to understand how the data are distributed, loaded the VCF data and performed variant QC, filtered variants and done LD-pruning to filer the matrix table, run a PCA, and added PCA values to the matrix table. You've saved the sample data and updated it to your workspace data table.
    The next step is to use the data to do a mixed-model association test in a workflow. Save and close this notebook and proceed to the dashboard for instructions of how to do this.

    ## Citations:
        1. A global reference for human genetic variation, The 1000 Genomes Project Consortium, Nature 526, 68-74 (01 October 2015) doi:10.1038/nature15393
        2. Yengo L, Sidorenko J, Kemper KE, Zheng Z, Wood AR, Weedon MN, Frayling TM, Hirschhorn J, Yang J, Visscher PM, GIANT Consortium. (2018). Meta-analysis of genome-wide association studies for height and body mass index in ~700, 000 individuals of European ancestry. Biorxiv
        3. Scott RA et al. Large-scale association analyses identify new loci influencing glycemic traits and provide insight into the underlying biological pathways. Nat. Genet. 2012;44;9;991-1005
        4. Klarin D, et al. Genetics of blood lipids among ~300, 000 multi-ethnic participants of the Million Veteran Program. Nat. Genet. 2018;50:1514–1523. doi: 10.1038/s41588-018-0222-9.
        5. Hail Team. Hail 0.2.13-81ab564db2b4. https://github.com/hail-is/hail/releases/tag/0.2.13.

    ## Authorship
    Much of the source code and tutorial was developed by Tim Majarian in the Manning Lab at the Broad Institute. The notebooks were developed to work in the BioData Catalyst ecosystem by Beth Sheets (UCSC) and Ash O'Farrell (UCSC) as part of the NHLBI BioData Catalyst grant.

    ## License
    Copyright Broad Institute, 2019 | BSD-3
    All code provided in this workspace is released under the WDL open source code license (BSD-3) (full license text at https://github.com/openwdl/wdl/blob/master/LICENSE). Note however that the programs called by the scripts may be subject to different licenses. Users are responsible for checking that they are authorized to run all programs before running these tools.

    ## Exercise hints

    ### Section 4
    ```python
    4.1.1 kdPlot(samples, var="lab_result_ldl")
    4.1.2 bivariateDistributionPlot(samples, var1="lab_result_hdl", var2="lab_result_ldl", kind="scatter")
    4.1.3 boxPlot(samples, catagorical_var="demographic_population", continuous_var="demographic_height_baseline", color_by="demographic_annotated_sex")
    ```

    ### Section 5
    ```python
    5.3.1 mt = hl.import_vcf(vcf_paths)
    5.3.2 mt.describe()
          mt.count()
    5.3.3 samples = hl.Table.from_pandas(samples, key='sample')
          mt = mt.annotate_cols(pheno=samples[mt.s])
          mt.rows().show(5)
    5.3.4 mt = hl.variant_qc(mt)
          mt.describe()
          mt.rows().show(5)
    ```

    ### Section 6
    ```python
    6.1.1 mt.describe()
          mt = mt.filter_rows(mt.variant_qc.AF[1] > 0.05)
          mt.count()

    6.2.1 pruned_variants = hl.ld_prune(mt.GT, r2=0.2, bp_window_size=100000, block_size=1024)
          pruned_variants.describe()
          pruned_variants.count()
    6.2.2 mt = mt.filter_rows(hl.is_defined(pruned_variants[mt.row_key]))
          mt.count()

    6.3.1 _, pcs, _ = hl.hwe_normalized_pca(mt.GT, k=5)
        6.3.2 pcs.describe()
          mt = mt.annotate_cols(scores=pcs[mt.s].scores)
              6.3.3 p = hl.plot.scatter(mt.scores[0],
                                  mt.scores[1],
                                  label = mt.pheno.ancestry,
                                  title = 'PCA', xlabel = 'PC1', ylabel = 'PC2')
                  show(p)
    ```
    """
