# publish to: "terra-notebook-utils-tests" "test"
import os
import herzog
from firecloud import fiss

with herzog.Cell("markdown"):
    """
    # VCF Subsample Workflow Input Preparation

    This notebook demonstrates the creation of Terra data tables as input to xvcfsubsample workflows, referencing input
    and output VCFs with DRS URIs or Google Storage URLs. It should be used with xvcfsubsample version
    [0.0.3](https://dockstore.org/workflows/github.com/DataBiosphere/xvcfsubsample/xvcfsubsample:v0.0.3?tab=info)

    ### Workflows

    Import the [Dockstore](https://dockstore.org) workflows into your workspace using the "NHLBI Biodata Catalyst" launch button:
     - [xvcfsubsample](https://dockstore.org/workflows/github.com/DataBiosphere/xvcfsubsample/xvcfsubsample:v0.0.3?tab=info)

    ### Input File Requirements

    - Input files _must_ be referenced with either DRS URIs or Google bucket URLs.
    - Output files _must_ be Google bucket URLs. The target bucket will usually be your workspace bucket, but it can be
      any bucket for which you have write access.
    """

os.environ['WORKSPACE_NAME'] = "terra-notebook-utils-tests"
os.environ['WORKSPACE_BUCKET'] = "gs://fc-9169fcd1-92ce-4d60-9d2d-d19fd326ff10"
os.environ['GOOGLE_PROJECT'] = "firecloud-cgl"

with herzog.Cell("python"):
    #%pip install --upgrade --no-cache-dir terra-notebook-utils
    pass

with herzog.Cell("python"):
    import os
    from terra_notebook_utils import table

    workspace = os.environ['WORKSPACE_NAME']
    workspace_namespace = os.environ['GOOGLE_PROJECT']
    workspace_bucket = os.environ['WORKSPACE_BUCKET']

with herzog.Cell("markdown"):
    """
    ## Option A: VCF subsample workflow input for DRS URIs
    This is a typical workflow preparation for subsampling TOPMed VCFs _without_ downloading them to your workspace.
    Results will be placed in your workspace bucket.
    """

table.delete("vcf-subsample-input-drs")
with herzog.Cell("python"):
    table_name = "vcf-subsample-input-drs"
    table.put_row(table_name, dict(workspace=workspace,
                                   billing_project=workspace_namespace,
                                   input="drs://dg.4503/b2871873-8dcb-4a3e-a926-a17ab4a19f0a",
                                   output=f"{workspace_bucket}/subsampled/drs_subsampled_a.vcf.gz",
                                   samples=["NWD999037", "NWD996859"]))
    table.put_row(table_name, dict(workspace=workspace,
                                   billing_project=workspace_namespace,
                                   input="drs://dg.4503/06dc6204-a426-11ea-b7de-179adfdbfdb4",
                                   output=f"{workspace_bucket}/subsampled/drs_subsampled_b.vcf.gz",
                                   samples=["NWD927369", "NWD934675", "NWD952492"]))

    # Samples may also be loaded from a file. If your samples file is stored in your workspace bucket,
    # it can be made available to the notebook using the `gsutil` command:
    # !gsutil cp "gs:${WORKSPACE_BUCKET}//my/samples/file" "samples"
    #
    # Now it may be opened for reading or used in other subroutines.
    # with open("samples") as fh:
    #     pass
    #
    # Parsing sample ids from the file is left as an exercise for the reader.

with herzog.Cell("markdown"):
    """
    ## Option B: VCF subsample workflow input for Google bucket URLs
    This workflow preparation uses VCFs that are available in a Google bucket. Results will be placed in your
    workspace bucket.

    Google bucket URLs should have the following format:
    - `gs://[bucket-name]/key/to/my/a-chr1.vcf.gz`
    - `gs://[bucket-name]/key/to/my/a-chr2.vcf.gz`
    - `..`
    - `gs://[bucket-name]/key/to/my/b-chr1.vcf.gz`
    - `gs://[bucket-name]/key/to/my/b-chr2.vcf.gz`
    """

table.delete("vcf-subsample-input-bucket")
with herzog.Cell("python"):
    table_name = "vcf-subsample-input-bucket"
    table.put_row(table_name, dict(workspace=workspace,
                                   billing_project=workspace_namespace,
                                   input=f"{workspace_bucket}/vcfsa/chr1.vcf.gz",
                                   output=f"{workspace_bucket}/subsampled/chr1.vcf.gz",
                                   samples=["NWD957804"]))
    table.put_row(table_name, dict(workspace=workspace,
                                   billing_project=workspace_namespace,
                                   input=f"{workspace_bucket}/vcfsa/chr2.vcf.gz",
                                   output=f"{workspace_bucket}/subsampled/chr2.vcf.gz",
                                   samples=["NWD860709", "NWD496635", "NWD637453", "NWD994242"]))

    # Samples may also be loaded from a file. If your samples file is stored in your workspace bucket,
    # it can be made available to the notebook using the `gsutil` command:
    # !gsutil cp "gs:${WORKSPACE_BUCKET}//my/samples/file" "samples"
    #
    # Now it may be opened for reading or used in other subroutines.
    # with open("samples") as fh:
    #     pass
    #
    # Parsing sample ids from the file is left as an exercise for the reader.
