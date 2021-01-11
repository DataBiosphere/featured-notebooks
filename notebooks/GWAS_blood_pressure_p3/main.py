# Title: 3-GWAS-genomic-data-preparation

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

    This notebook demonstrates typical initial steps in a genetic association analysis: filtering, LD-pruning, and Principal Component analysis.

    This notebook hopes to help you understand the following steps in performing an association test in BioData Catalyst:

    1. We will import the phenotype.csv generated in the "2-GWAS-phenotypic-data-preparation" notebook.

    2. Next, we import, explore, and perform quality control on genotypic data.

    3. Once we're satisfied that everything looks reasonable, we process the genetic data to better adjust for relatedness within our set of samples. Relatedness can easily confound GWAS results and we must take care to account for it in our analysis.

    4. We write our results to prepare a set of input parameters and data for a genomewide association analysis pipeline.

    5. The final cells of the notebook demonstrate how to generate a new Terra data model that links all of the data generated in the notebook and allows for easy import into the workflows that perform the association tests.


    # Set up your notebook

    ## Select your Jupyter environment

    In the gear wheel at the top right of this notebook you can select the docker image or "Environment" to use. We suggest using the environment with the most update to date Hail package (for example: Python 3.7.6, Hail 0.2.30). Configure your notebook runtime using the gear wheel at the top right to use a spark cluster for parallel processing. You can learn more about Spark in the Hail section below. You can learn more about Terra's [runtime environment](https://support.terra.bio/hc/en-us/articles/360027237871) and how to [control cloud costs](https://support.terra.bio/hc/en-us/articles/360029772212).

    ## Set your runtime configuration based on your cost and time needs

    Your compute needs will be based on the size of your input and your cost concerns.
    * If you are new to scaling costs with hail, it is recommended that you consider starting by running on just one chromosome and then scaling up.
    * The first disk size variable (not the worker one) *must* be larger than the size of your VCF(s).
    * Clusters can be resized dynamically when running hail in some circumstances, but this is not currently available on Terra. For that reason we have made this notebook as focused on hail operations as possible so as to save money and compute time.
    * It is always recommended to use at least two workers when doing anything with hail.
    * That being said, hail is made to work with data larger than available memory, so that does not need to be larger than your VCF(s).
    * Note that preemptibles are counted in addition to non-preemptibles. For example, if you select 100 workers and 50 preemptibles, you are requesting a total of 150 nodes, **not** 100 nodes of which 50 are preemptible.
    * Using preemptibles is not recommended for analyses that take longer than 6 hours. For large-scale GWAS data preparation, this may be the case depending on the number of workers you request for parallel processing of your variant matrix. 
    * Preemptible nodes are generally not assigned more than 100 GB of disk space, so it is not worth making them larger than that. However, besides that, they have the same resources (ie memory) as your nonpreemptible workers.
    * A suggested custom configuration using a 100GB VCF file is below. For larger files, consider using more workers.
    * **It is not recommended to just copy these values down for every possible job. You will likely need to tailor them to what you know about your data.** For instance, a dataset with more closely related individuals (such as the Amish data set) may have a different execution time than a similarly sized dataset derived from a more diverse cohort.
    <table style="float:left">
        <thead>
            <tr><th>Attributes</th><th>Value</th></tr>
        </thead>
        <tbody>
            <tr><td> Application configuration</td><td>Hail: (Python 3.7.9, Spark 2.4.5, hail 0.2.57) </tr></td>
            <tr><td> CPUs</td><td>8 </tr></td>
            <tr><td> Memory (GB)</td><td>30 </tr></td>
            <tr><td> Disk size (GB)</td><td>500 </tr></td>
            <tr><td> Startup script</td><td>(leave blank) </tr></td>
            <tr><td> Compute type</td><td>Spark cluster </tr></td>
            <tr><td> Workers</td><td>120 </tr></td>
            <tr><td> Preemptible</td><td>50 </tr></td>
            <tr><td> Workers-CPUs</td><td>8 </tr></td>
            <tr><td> Workers-Memory (GB)</td><td>30 </tr></td>
            <tr><td> Workers-Disk size (GB)</td><td>100 </tr></td>
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
    * **Hail** - an open-source, general-purpose, Python-based data analysis tool with additional data types and methods for working with genomic data

    ## Install and update packages
    """
with herzog.Cell("python"):
    #%pip install tenacity
    #%pip install hail
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
    The billing project, workspace, and bucket filepaths are neccessary to define in most python Jupyter notebooks you run in Terra.

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

with herzog.Cell("python"):
    # Load phenotypic data from previous notebook
    samples_traits_for_analysis = pd.read_csv(bucket + 'bp-phenotypes.csv')

with herzog.Cell("markdown"):
    """
    Here, we define some output files we will generate in this notebook.
    """
with herzog.Cell("python"):
    phenotype_out = 'bp-phenotypes-hail-update.csv'
    kinship_out = 'bp-kinship.csv'
    notebook_out = 'bp-hail.ipynb'
    html_out = 'bp-hail.html'
    samples_out = 'samples_traits-updated.csv'

with herzog.Cell("python"):
    #Check out the distributions of the phenotypic data
    samples_traits_for_analysis.describe()
with herzog.Cell("markdown"):
    """
    # Work with genotype data using Hail

    Now that we have a handle on the phenotype data, we can begin to work with the genotype data. We will make use of the [Hail](hail.is) software package, an "open-source, general-purpose, Python-based data analysis tool with additional data types and methods for working with genomic data." Hail utilizes distributed computing with Apache Spark for efficient genome-wide analysis.

    ## Goals of this section
    1. Load genotype data from variant call format (VCF) files into the compute environment
    2. Understand how to access genotype and variant information
    3. Generate variant quality control metrics
    4. Keep only those samples with phenotype data


    ### A short primer on Hail:
    Hail uses distributed computing -- a way of parallelizing tasks that can greatly decrease the **real** time it takes to complete an analysis. While distributed computing decreases **real** time, the time that you or I experience, it does not decrease **computational** time, the product of *# machines* and *time each machine was running*. Distributed computing relies on an operation called *partitioning*, dividing a object into many, many pieces. The number of partitions determines, in part, the number of operations that can run in parallel. In the code below, you will see syntax like `min_partitions = 200`. In English, this is saying "at the minimum, divide my data into 200 pieces". This parameter can vary and should be determined based on the data and the particular operations that you want to do -- more partitions is not always better.

    It is important to note that Hail expressions are **lazy** -- they are not evalutated until absolutely needed. For example, using Python for the expression `x = 2+2` would immediately store the value of `x` as `4`. However, calling the same expression using Hail would instead store `x` as `2+2` until the variable `x` was explicitly used. When using some of the code below, you will notice that some code blocks will run astoundingly quickly. This does not mean that the operation is complete, rather that the operation's result was not yet needed. When a results is needed, the operation will run. This should become more clear when working through this notebook.

    For more information, checkout the [Hail documentation](http://www.nealelab.is/tools-and-software) and this [helpful video](https://youtu.be/0RTgBYL5x_E).

    ## Import VCF from workspace

    In the notebook 1-copy-vcf-to-workspace, you copied your VCF of interest to your Terra workspace bucket and extracted the contents of the tar file. After extraction, you should see 23 VCFs (that represent a single VCF per chromsoome) in the "Files" section of the data tab of your workspace (or the variable 'bucket' below). Next, you will create an array to these files in the bucket and interact with them using Hail.
    """
with herzog.Cell("python"):
    vcf_base = bucket + 'ph*/ph*/*.vcf.gz'
    vcf_paths = get_ipython().getoutput('gsutil ls {vcf_base}')
    vcf_paths = vcf_paths
    vcf_paths
with herzog.Cell("markdown"):
    """
    Note, if you downloaded multiple VCFs to your workspace using the 1-copy-vcf-to-workspace notebook, or if you brought a VCF into the workspace with a different method, you will need to update the path in the vcf_base variable.

    ## Import packages and start a Hail session

    * **Hail** - an open-source, general-purpose, Python-based data analysis tool with additional data types and methods for working with genomic data
    * **Bokeh** - an interactive visualization library
    """
with herzog.Cell("python"):
    import hail as hl
    import bokeh.io as bokeh_io
    from bokeh.resources import INLINE

bokeh_io = mock.MagicMock()  # noqa # test fixture
hl = mock.MagicMock()  # noqa # test fixture
with herzog.Cell("python"):
    # Start Hail context using Spark
    bokeh_io.output_notebook(INLINE)
    hl.init(default_reference="GRCh38", log='population-genetics.log')
with herzog.Cell("markdown"):
    """
    ## Load VCF data and perform variant QC

    To load genotype data from VCF files, use the <font color='red'>import_vcf</font> function. This will convert the VCF files into a distributed data type, a **matrix table** (**mt**), storing small pieces of each file independently to allow for fast access and computation. A matrix table is composed of 3 parts: sample annotations (columns), variant annotations (rows), and entries (genotypes). Use the following syntax to load the 1000 Genomes data:

    ```python
    mt = hl.import_vcf(vcf_paths)
    ```

    The structure of the matrix table can be viewed by <font color='red'>describing</font> the object:

    ```python
    mt.describe()
    ```

    The number variants and samples (rows and columns) can be seen by calling <font color='red'>count</font>:

    ```python
    mt.count()
    ```

    You can merge phenotypes with the VCF data by matching sample IDs between objects. Recall that the *sample* column of the phenotype data held unique IDs for each sample. These same sample IDs are stored in the VCF files and index the matrix table columns. <font color='red'>annotate_cols</font> can be used to add the phenotypes to the matrix table columns. `samples[mt.s]` matches samples IDs between objects and <font color='red'>annotate_cols</font> merges into the VCF:

    ```python
    samples = hl.Table.from_pandas(samples, key = 'sample')
    mt = mt.annotate_cols(pheno = samples[mt.s])
    ```

    To generate variant level summary statistics, use <font color='red'>variant_qc</font>. This will compute useful metrics like allele frequencies, call rate, and homozygote counts, among many others. Run variant_qc and take a look at how the matrix table structure changes.

    ```python
    mt = hl.variant_qc(mt)
    ```

    ### Load genotype data
    Use <font color='red'>import_vcf</font> with the syntax described above to define the matrix table, `mt`:
    """
with herzog.Cell("python"):
    # If you get an error here, double check sure your application configuration is set to Hail,
    # not the default GATK/python/R setup -- that default will not work here!
    # See the application configuration at the top of this notebook for more details
    mt = (
        hl
        .import_vcf(
            vcf_paths, force_bgz=True, min_partitions=200
        )
    )
with herzog.Cell("markdown"):
    """
    ### View matrix table structure
    Use the <font color='red'>describe</font> function to view the structure of the matrix table:
    """
with herzog.Cell("python"):
    mt.describe()
with herzog.Cell("markdown"):
    """
    **Count rows and columns**: How many variants and samples are there in your matrix table? Note that the `count` function can take a long time with a big dataset.
    """
with herzog.Cell("python"):
    mt.count()
with herzog.Cell("markdown"):
    """
    ### Merge phenotype and VCF data
    Follow the syntax above to match the phenotypes with the correct genotypes.

    First convert the phenotypes pandas dataframe (samples_traits_for_analysis) to a Hail table:
    """
with herzog.Cell("python"):
    samples_traits_for_analysis = (
        hl
        .Table.from_pandas(
            samples_traits_for_analysis,
            key='nwd_id'
        )
    )
with herzog.Cell("markdown"):
    """
    Then annotate the matrix table by matching the NWD IDs:
    """
with herzog.Cell("python"):
    mt = (
        mt.annotate_cols(pheno=samples_traits_for_analysis[mt.s])
    )
with herzog.Cell("markdown"):
    """
    See the first few rows of the matrix table:
    """
with herzog.Cell("python"):
    mt.rows().show(5)
with herzog.Cell("markdown"):
    """
    ### Generate variant level summary statistics

    Run <font color='red'>variant_qc</font> first:
    """
with herzog.Cell("python"):
    mt = hl.variant_qc(mt)
with herzog.Cell("markdown"):
    """
    Next take a look at how the matrix table structure changes: use <fibt color="red">describe</font> and you should see a new set of annotations added to the table (under variant_qc). Then use `mt.rows().show(5)` to see the first few variants and their annotations (scroll down to the end).
    """
with herzog.Cell("python"):
    mt.describe()
with herzog.Cell("python"):
    mt.rows().show(5)
with herzog.Cell("markdown"):
    """
    ## Variant filtering

    Variants can be filtered using <font color='red'>filter_rows</font> and conditioning on allele frequency. <font color='red'>filter_rows</font> takes an expression, something like:

    ```python
    mt = mt.filter_rows(mt.info.rsid == ".", keep = False)
    ```

    To find where allele frequency is stored, use the <font color='red'>describe</font> function again.  Allele frequency is stored as a tuple. In our biallelic dataset, the reference frequency is the first value and the alternate frequency the second.)

    ### Filter for only common variants
    Because this tutorial uses few individuals, we have poor statistical power for identifying rare variant associaitons. We filter to only common variants, those with a minor allele frequency greater than 0.01.
    """

mt.variant_qc.AF = [1, 2, 3]  # test fixture
with herzog.Cell("python"):
    mt = (
        mt
        .filter_rows(
            mt.variant_qc.AF[1] > 0.01
        )
    )
with herzog.Cell("markdown"):
    """
    ### Filter to only those samples in the phenotype file with "bp_systolic" information
    """
with herzog.Cell("python"):
    mt = (
        mt
        .filter_cols(hl.is_defined(mt.pheno['blood_pressure_test_bp_systolic']), keep=True)
    )
with herzog.Cell("markdown"):
    """
    ### Write the Hail matrix to the workspace bucket to save your work

    For very large analyses, we recommend that you save your work along the way. Due to the interactive nature of notebooks, you may lose your work if it is in the Notebook's RAM and not saved to the Workspace bucket. This may take a several minutes -- for comparison, saving the results when running on chromosome 1 of a 1111 member TOPMed study took just over four minutes.
    """
with herzog.Cell("python"):
    start_matrix_write_time = time.time()
    mt.write(bucket + 'MyProject_MAFgt0.01.mt', overwrite=True)
    elapsed_write_time = time.time() - start_matrix_write_time
with herzog.Cell("python"):
    print(timedelta(seconds=elapsed_write_time))
with herzog.Cell("python"):
    # Read the Hail matrix back in.
    mt = hl.read_matrix_table(bucket + 'MyProject_MAFgt0.01.mt')
with herzog.Cell("python"):
    #Visualize variants
    hl.summarize_variants(mt)
with herzog.Cell("markdown"):
    """
    # Convert the common variant genotype matrix back to a vcf and save in the workspace bucket for use in workflows

    This step computes a new field with allele count information and adds as a row to the matrix
    """
with herzog.Cell("python"):
    mt = mt.annotate_rows(info=mt.info.annotate(AC=mt.variant_qc.AC))
with herzog.Cell("markdown"):
    """
    We export the VCF as several files (shards) to speed up the process.
    """
with herzog.Cell("python"):
    start_vcf_write_time = time.time()
with herzog.Cell("python"):
    mt = mt.repartition(25)
    hl.export_vcf(mt, bucket + 'MyProject_MAFgt0.01.vcf.bgz', parallel='header_per_shard')
with herzog.Cell("python"):
    elapsed_vcf_write_time = time.time() - start_vcf_write_time
with herzog.Cell("python"):
    print(timedelta(seconds=elapsed_vcf_write_time))
with herzog.Cell("markdown"):
    """
    Check that these files were successfully loaded to the bucket:
    """
with herzog.Cell("python"):
    get_ipython().system(" gsutil ls {bucket + 'MyProject_MAFgt0.01.vcf.bgz/*'}")
with herzog.Cell("markdown"):
    """
    Here, we create an array pointing to all of the shards that make up the common variants for our dataset. We use this array in the last section of this notebook called "Generate a new Terra data model called "sample_set" and add all of our derived data files to this entity."
    """
with herzog.Cell("python"):
    vcf_filtered_base = bucket + 'MyProject_MAFgt0.01.vcf.bgz/*.bgz'
    vcf_filtered_path = get_ipython().getoutput('gsutil ls {vcf_filtered_base}')
    vcf_filtered_array = vcf_filtered_path
    vcf_filtered_array
with herzog.Cell("markdown"):
    """
    # Understanding population structure within our sample
    ----

    Many of our statistical tests are built on the assumption of unrelated data points and require adjustment to account for population structure. There are various ways to quantify population structure, but most start by generating a set of markers (variants) that are nearly independent of one another. For this, we will use an operation called *Linkage Disequalibrium pruning* to extract a set of variants that we can use for calculating relatedness. See [this resource](https://en.wikipedia.org/wiki/Linkage_disequilibrium) for more information on LD.

    Next, we will use principal component analysis (PCA) to transform the genetic data into a space that will aid in modeling by allowing us to more easily visualize genetic distance between individuals.

    ## Goals of this section
    1. Generate a list of variants for calculated relatedness by filtering and LD-pruning
    2. Calculate principal components using Hail
    3. Visualize individuals within PC space

    ## LD-pruning

    We'll want to only include variants that are (nearly) independent of each other. We'll accomplish this using linkage disequalibrium pruning with the <font color='red'>ld_prune</font> function. Inputs are the genotypes, an r^2 threshold, and a window size. These last two parameters control how strict we are in our definition of independence. The final parameter *block_size* relates to parallelization and should not be changed. More information can be found in the [Hail documentation](https://www.hail.is).

    ```python
    pruned_variants = hl.ld_prune(mt.GT, r2 = 0.2, bp_window_size = 100000, block_size = 1024)
    ```

    The last step is to filter the matrix table again by the pruned variants list. For this, <font color='red'>is_defined</font> is useful:

    ```python
    mt = mt.filter_rows(hl.is_defined(pruned_variants[mt.row_key]))
    ```

    Be sure to take a look at how pruning changes the number of variants in your dataset using the <font color='red'>count</font> function.
    """

# When testing on Amish chromosome one, this took 38 minutes
with herzog.Cell("python"):
    #We added code to help you monitor the time it takes for pruning. We currently estimate over an hour.
    start_prune_write_time = time.time()
    pruned_variant_table = hl.ld_prune(mt.GT, r2=0.2, bp_window_size=500000, block_size=1024)
    elapsed_prune_write_time = time.time() - start_prune_write_time
    print(timedelta(seconds=elapsed_prune_write_time))
with herzog.Cell("python"):
    mt = mt.filter_rows(hl.is_defined(pruned_variant_table[mt.row_key]))
with herzog.Cell("markdown"):
    """
    ## Principal Component Analysis

    In this next section, we'll cover a method for easily visualizing and adjusting for population structure in an association analysis: Principal Component Analysis (PCA).

    You run PCA using the function <font color='red'>hwe_normalized_pca</font>. For this analysis, we are mainly interested in the scores, and can disregard the eigenvalues and loadings. The `k` parameter determines the number of PCs to return -- as `k` grows, so does the computation time.

    ```python
    _, pcs, _ = hl.hwe_normalized_pca(mt.GT, k=5)
    ```

    The PCs can then be added to the matrix table in much the same way as phenotypes. Use <font color='red'>annotate_cols</font>. (<font color='blue'>Hint:</font> the pcs object has multiple fields, use <font color='red'>describe</font> to find the field that you want to keep. Don't forget to match over the sample IDs.)

    Finally, let's visualize the results. Hail has some efficient built-in plotting functions -- let's use <font color='red'>plot.scatter</font>. Choose which PCs you'd like to plot and a categorical phenotype value to color the points.

    ```python
    p = hl.plot.scatter(mt.scores[0],
                        mt.scores[1],
                        label = mt.pheno.ancestry,
                        title = 'PCA', xlabel = 'PC1', ylabel = 'PC2')
    bokeh_io.show(p)
    ```


    ### Run the PCA
    """
hl.hwe_normalized_pca.return_value = [mock.MagicMock() for _ in range(3)]  # test fixture
with herzog.Cell("python"):
    _, pcs, _ = hl.hwe_normalized_pca(mt.GT, k=5)
with herzog.Cell("markdown"):
    """
    ### Visualize the PCA
    """
with herzog.Cell("python"):
    p = hl.plot.scatter(pcs.scores[0],
                        pcs.scores[1],
                        xlabel='PC1', ylabel='PC2')
    bokeh_io.show(p)
with herzog.Cell("markdown"):
    """
    ### Decide whether genetic stratification should be included in your association tests

    If your project shows distinct clusters of samples in your PCA, you need to account for this genetic stratification by including the first principal components as covariates in your analysis. This can be done using the annotate_cols function.
    """
with herzog.Cell("python"):
    # Use describe to find the PC fields you want to keep
    pcs.describe()
with herzog.Cell("python"):
    # Add to the matrix table
    mt = mt.annotate_cols(scores=pcs[mt.s].scores)
with herzog.Cell("markdown"):
    """
    # Generate a genetic relatedness matrix (GRM)
    Hail has built-in functions for generating a GRM. A GRM can also account for population stratification and cryptic genetic relatedness in our cohort.
    """
# On chr 1 from Amish this only took 46 seconds but on all Amish chromosomes it seemed to have hung
with herzog.Cell("python"):
    # Calculate the GRM
    # WARNING: This can take a very long time to complete!
    start_grm_time = time.time()

    grm = hl.genetic_relatedness_matrix(mt.GT).to_numpy()

    elapsed_grm_time = time.time() - start_grm_time
    print(timedelta(seconds=elapsed_grm_time))
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
    rel_pd.to_csv(kinship_out)
with herzog.Cell("markdown"):
    """
    # Save sample metadata and update data table
    Now that we've explored the phenotypes and generated relatedness measures, the next step is to save and push our results back to the workspace data model. Once we populate the data model, we can use it for downstream analyses.

    ## Convert the phenotype data to the correct format
    """
with herzog.Cell("python"):
    # Gather the sample metadata
    samples_traits_for_analysis = (
        mt.cols()
    )
with herzog.Cell("python"):
    # Select only the columns we want to keep
    samples_traits_for_analysis = (
        samples_traits_for_analysis
        .key_by()
        .select('s', 'pheno')
    )
with herzog.Cell("python"):
    # Convert the Hail matrix back to a pandas dataframe
    samples_traits_for_analysis = (
        samples_traits_for_analysis
        .to_pandas()
    )
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
    # Check that this worked
    # Note that because we used the nwd_id as the key to match the phenotypic to genotypic data,
    # nwd_id is now the firt column
    samples_traits_for_analysis.head()

with herzog.Cell("markdown"):
    """
    ## Write the phenotype data to a new file
    Write the phenotype data exported from the outputs of Hail to a new file for use in the GENESIS workflows. Hail may have removed individuals if they did not have genotype data associated with them.
    """

with herzog.Cell("python"):
    samples_traits_for_analysis.to_csv(phenotype_out, index=False)

with herzog.Cell("markdown"):
    """
    ## Move the phenotype and GRM files to the workspace bucket
    """

with herzog.Cell("python"):
    #!gsutil cp {phenotype_out} {bucket + phenotype_out}
    #!gsutil cp {kinship_out} {bucket + kinship_out}
    pass

with herzog.Cell("markdown"):
    """
    ## Generate a new Terra data model called "sample_set" and add all of our derived data files to this entity.

    The last step in the analysis is to update **your** workspace data table with the data generated in this notebook. For downstream analysis, you'll want to set up all the needed parameters and input data directly in the data table. As you will see, analysis workflows look to the data table for configuration and inputs.

    Recall that each *sample* in the data table corresponded to a single individual. To run a GWAS requires a set of samples, or in Terra terms, a **sample_set**. This entity should include everything you might need to run a workflow for genotype-phenotype modeling: a list of samples to include, phenotype data, a list of covariates, an outcome, a genetic relatedness matrix, and a label. The particular workflow that you will soon run looks for inputs in specific columns of the data table.

    Below is a helpful function for generating a sample set in the correct format so you will have success in the next part this template. To look at the full code, click the arrow at the top left of the cell.

    In a GWAS, you may iterate through an analysis multiple times by filtering your data differently or including/excluding covariates. For each separate run, you can cange the values in the columns, specifically the value in the sample_set_id column to reflect a new analysis. You will see that you can save each notebook you used for each iteration as well.
    """

with herzog.Cell("python"):
    # Make a new "sample set" entity type and create an entity within called "systloicbp"

    #column headers
    cols = ['entity:sample_set_id',
            'phenotypes',
            'phenotypes_notebook',
            'phenotypes_html',
            'hail_notebook',
            'hail_html',
            'outcome_name',
            'outcome_type',
            'covariates',
            'sample_id_column',
            'grm']

    #values for each column
    vals = ['systolicbp',
            bucket + phenotype_out,
            bucket + "bp-phenotypes.iypnb",  # second notebook in this workspace
            bucket + "bp-phenotypes.html",
            bucket + notebook_out,  # third notebook (this one) in this workspace
            bucket + html_out,
            'bp_systolic',
            'Continuous',
            'age_at_bp_systolic,sex,antihypertensive_meds,bp_diastolic',
            'nwd_id',
            bucket + kinship_out]

    #create the entity and upload it using the API
    entity = '\n'.join(['\t'.join(cols), '\t'.join(vals)])
    fiss.fapi.upload_entities(PROJECT, WORKSPACE, entity)
with herzog.Cell("markdown"):
    """
    ## Update your sample_set entity with the array of paths to your filtered vcf files

    In this example, our sample set that we created above is titled "systolicbp" and we are adding a set of filtered common varaints to the
    """
with herzog.Cell("python"):
    common_variants_attribute = [fiss.fapi._attr_set("common_variants", vcf_filtered_array)]
    fiss.fapi.update_entity(PROJECT, WORKSPACE, 'sample_set', 'systolicbp', common_variants_attribute)
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
