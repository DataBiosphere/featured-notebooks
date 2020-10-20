# publish to: "terra-notebook-utils-tests" "VCF Merge and Subsample Tutorial"
import os
import callysto

with callysto.Cell("markdown"):
    """
    # Cohort VCF Merge and Subsample

    The TopMED data indexed by BioData Catalyst has been jointly called for each Freeze. TOPMed researchers not part of the TOPMed Constorium receive jointly called VCFs subsetted by project and consent code. If users have access to multiple projects and consent codes, they may wish to re-combine these VCFs. This notebook demonstrates the merge and subsamle tooling for jointly called VCFs.

    ## Aquire the Cohort VCFs to be merged and subsamled

    The VCFs should be stored in directories in your workspace bucket. During this tutorial, we will use the placeholder structure
    ```
    gs://my-bucket/vcfsa/chr1.vcf.gz
    gs://my-bucket/vcfsa/chr2.vcf.gz
    ...
    gs://my-bucket/vcfsb/chr1.vcf.gz
    gs://my-bucket/vcfsb/chr2.vcf.gz
    ...
    ```

    Note: Freeze5b cohort VCFs in Gen3 are currently stored as tar arhcives exactly as they are represented in dbGAP. Before running this notebook, two or more archives must be extracted using the utilities found in the [unarchive-tar-files-to-workspace notebook](terra.biodatacatalyst.nhlbi.nih.gov/#workspaces/biodata-catalyst/BioData%20Catalyst%20Collection/notebooks/launch/unarchive-tar-files-to-workspace.ipynb).

    ## Workflows

    Import the [Dockstore](https://dockstore.org) workflows into your workspace using the "NHLBI Biodata Catalyst" launch button:
     - [xvcfmerge](https://dockstore.org/workflows/github.com/DataBiosphere/xvcfmerge:master?tab=info)
     - [xvcfsubsample](https://dockstore.org/workflows/github.com/DataBiosphere/xvcfsubsample/xvcfsubsample:master?tab=info)
     - Somehow get cohort VCFs into your workspace bucket
    """

os.environ['WORKSPACE_NAME'] = "terra-notebook-utils-tests"
os.environ['WORKSPACE_BUCKET'] = "gs://fc-9169fcd1-92ce-4d60-9d2d-d19fd326ff10"
os.environ['GOOGLE_PROJECT'] = "firecloud-cgl"

with callysto.Cell("python"):
    # Install the [terra-notebook-utils](https://github.com/DataBiosphere/terra-notebook-utils) package and import libraries
    #%pip install --upgrade --no-cache-dir terra-notebook-utils
    import os
    from firecloud import fiss
    import terra_notebook_utils as tnu

with callysto.Cell("python"):
    # Create a useful function to upload a tsv to a Terra Data Table
    def upload_data_table(tsv):
        billing_project = os.environ['GOOGLE_PROJECT']
        workspace = os.environ['WORKSPACE_NAME']
        resp = fiss.fapi.upload_entities(billing_project, workspace, tsv, model="flexible")
        resp.raise_for_status()

with callysto.Cell("python"):
    # List the VCFs to be merged
    #!gsutil ls $WORKSPACE_BUCKET/vcfsa
    #!echo
    #!gsutil ls $WORKSPACE_BUCKET/vcfsb
    pass

with callysto.Cell("python"):
    # Prepare the merge workflow input data table.
    # There should be one row per chromosome VCF
    bucket = os.environ['WORKSPACE_BUCKET']
    tsv_data = "\t".join(["merge_input_id", "inputs", "output"])
    tsv_data += os.linesep + "\t".join(["1",
                                        f"{bucket}/vcfsa/chr1.vcf.gz,"
                                        + f"{bucket}/vcfsb/chr1.vcf.gz",
                                        f"{bucket}/merged/chr1.vcf.gz"])
    tsv_data += os.linesep + "\t".join(["2",
                                        f"{bucket}/vcfsa/chr2.vcf.gz,"
                                        + f"{bucket}/vcfsb/chr2.vcf.gz",
                                        f"{bucket}/merged/chr2.vcf.gz"])
    upload_data_table(tsv_data)

with callysto.Cell("python"):
    # List the merged VCFs
    #!gsutil ls $WORKSPACE_BUCKET/merged
    pass

with callysto.Cell("python"):
    # Use the terra_notebook_utils package to list the samples in the merged VCFs
    #!~/.local/bin/tnu vcf samples gs://fc-f4cc20e1-26ef-4eb9-9c55-aa8deb2d794b/merged/chr21.vcf.gz
    pass

with callysto.Cell("python"):
    # Prepare the subsample workflow input data table
    # There should be one row per chromosome VCF
    tsv_data = "\t".join(["subsample_input_id", "input", "output", "samples"])
    tsv_data += os.linesep + "\t".join(["1",
                                        f"{bucket}/merged/chr21.vcf.gz",
                                        f"{bucket}/subsampled/chr21.vcf.gz",
                                        "NWD348918,NWD357834,NWD810020,NWD894075"])
    tsv_data += os.linesep + "\t".join(["2",
                                        f"{bucket}/merged/chr22.vcf.gz",
                                        f"{bucket}/subsampled/chr22.vcf.gz",
                                        "NWD954598,NWD848492,NWD312654"])
    upload_data_table(tsv_data)
