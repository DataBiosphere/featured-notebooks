# 3-finalPreparation

# Notebook author: Beth Sheets
# Herzog version: Ash O'Farrell

# Notebook title: 3-GWAS-genomic-data-preparation

import os
import herzog

from unittest import mock
# Heavyweight dependencies (e.g. hail, Spark) make it challenging to create robust automated testing for this notebook.
# In order to get it to pass Python execution, many objects are mocked with unittest.mock.MagicMock. These have been
# marked with `# test fixture`. This will at least catch some syntax errors.
#
# In the meantime, this notebook should be tested manually in a Terra notebook environment.

get_ipython = mock.MagicMock()  # test fixture
# hail and bokeh are are mocked further down the line

# Mock the environment
os.environ['WORKSPACE_NAME'] = "cicd-tester-1000genomes-gwas"
os.environ['GOOGLE_PROJECT'] = "foo"  # test fixture
os.environ['WORKSPACE_BUCKET'] = "bar"  # test fixture

with herzog.Cell("markdown"):
    """
    # GWAS Genomic Analysis
    *version: 2.0.1*

    ## Introduction
    In this notebook, you will be working with Hail to make some final adjustments to your data.

    """

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

    ## Set runtime values
    The settings for a notebook utilizing Hail differ from the previous two notebooks in this workspace. Click on the gear icon in the upper right to edit your Notebook Runtime. Set the values as specified below:
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

    Once you select these options, Terra may ask you about the persistent disk. Essentially, when you run a Jupyter notebook, it creates a VM whose contents can be shared across other notebooks. But the VM of a Hail notebook like this one is not compatiable with the setup that you used for the previous two notebooks. This generally means files created in the VM you used earlier cannot be accessed by the one you're making right now. But, in the previous notebook, we saved our phenotypic data to the workspace bucket, which exists outside of the VM. For that reason, it doesn't matter too much if you keep your old persistent disk or not. If you'd like to return to the previous notebooks and manipulate the data without rerunning them in their entirity, you may want to keep the persistent disk, but doing so will incurr a cost over time. Terra will give you an estimate of that cost to help inform your decision.

    """

with herzog.Cell("python"):
    #%%capture
    import os
    from firecloud import fiss
    import pandas as pd
    from pprint import pprint  # for pretty printing

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
    # If this fails with the following error...
    # Error summary: IOException: No FileSystem for scheme: gs
    # Make sure your notebook is a hail compute. See the top of this notebook under "Set runtime values" for details.
    mt = hl.import_vcf(vcf_paths)

mt = mock.MagicMock()  # noqa # test fixture

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
    # Load phenotypic data from previous notebook
    samples = pd.read_csv(bucket + 'nb2pheno.csv')

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
# In ipynb form, the TypeError does not occur. So, do NOT put this workaround into a Python cell!
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

hl.hwe_normalized_pca.return_value = [mock.MagicMock() for _ in range(3)]  # test fixture

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
               'pheno.demographic_age_at_index': 'age',
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

# Another workaround
vcf_attribute = ['foo', 'bar']

with herzog.Cell("python"):
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

with herzog.Cell("markdown"):
    """
    ### Info
    Authors: Beth Sheets (UCSC), Ash O'Farrell (UCSC)

    The authorship and updating of this notebook was performed under the BioData Catalyst grant.
    """
