# publish to: "terra-notebook-utils-tests" "test"
import os
import herzog
from firecloud import fiss

with herzog.Cell("markdown"):
    """
    # VCF Merge Workflow Input Preparation

    This notebook demonstrates the creation of Terra data tables as input to xvcfmerge workflows, referencing input
    VCFs with an `Array[String]` of DRS URIs or Google Storage URLs, and is meant to be used with the xvcfmerge
    version [xbrianh-input-format](https://dockstore.org/workflows/github.com/DataBiosphere/xvcfmerge:v0.1.0?tab=info).

    This version of the xvcfmerge changes input format from a comma separated `String` of Google Storage URLs (gs://) to an
    `Array[String]` of either DRS URIs (drs://) or Google Storage URLs (gs://). It is currently in beta.

    ## Cohort VCF Merge

    TopMED multi-sample VCF files indexed by BioData Catalyst have been jointly called for each "Freeze", or release of
    genomic data. These files are accessible in the "Reference File" node of the Gen3 graph. TOPMed researchers not
    part of the TOPMed Consortium receive jointly called VCFs subsetted by project and consent code. If users have
    access to multiple projects and consent codes, they may wish to re-combine these VCFs to form a synthetic cohort.
    The Merge VCFs notebook demonstrates tooling to merge jointly called VCFs.

    Note: Cohort multi-sample VCFs are currently available in multiple formats in Gen3 with the goal of eventually
    hosting files in actionable formats (i.e. not tar compressed). Currently, Freeze 8 multi-sample VCFs are available
    as 23 files that are blocked zipped (*.vcf.gz). Some Freeze5b cohort VCFs in Gen3 may be stored as a single tar
    bundle. If you are interested in Freeze 5b files that are tar.gz archived, you will need to extract the contents of
    the tar.gz to your workspace using the utilities found in the
    [unarchive-tar-files-to-workspace notebook](https://app.terra.bio/terra.biodatacatalyst.nhlbi.nih.gov/#workspaces/biodata-catalyst/BioData%20Catalyst%20Collection/notebooks/launch/unarchive-tar-files-to-workspace.ipynb).

    ### Workflows

    Import the [Dockstore](https://dockstore.org) workflows into your workspace using the "NHLBI Biodata Catalyst" launch button:
     - [xvcfmerge](https://dockstore.org/workflows/github.com/DataBiosphere/xvcfmerge:v0.1.0?tab=info)

    Runtime and cost can vary greatly depending on the size and number of intput files. Sample runtime and cost
    information is available for previous merge workflows in
    [NHLBI BioData Catalyst / Utilities](https://dockstore.org/organizations/bdcatalyst/collections/Utilities) for

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
    ## Option A: VCF merge workflow input for DRS URIs
    This is a typical workflow preparation for merging TOPMed VCFs _without_ downloading them to your workspace.
    Results will be placed in your workspace bucket.
    """

table.delete("vcf-merge-input-drs")
with herzog.Cell("python"):
    table_name = "vcf-merge-input-drs"
    table.put_row(table_name, dict(workspace=workspace,
                                   billing_project=workspace_namespace,
                                   inputs=["drs://dg.4503/697f611b-aa8a-4bd7-a80b-946276273833",
                                           "drs://dg.4503/ce212b62-e796-4b32-becb-361f272cead0"],
                                   output=f"{workspace_bucket}/merged/drs_combined_a.vcf.gz"))
    table.put_row(table_name, dict(workspace=workspace,
                                   billing_project=workspace_namespace,
                                   inputs=["drs://dg.4503/93286e47-3d09-47e6-ac87-4c2975ef0c3f",
                                           "drs://dg.4503/aba6b011-2ab4-4739-beb4-c1eeaee60c74"],
                                   output=f"{workspace_bucket}/merged/drs_combined_b.vcf.gz"))

with herzog.Cell("markdown"):
    """
    ## Option B: VCF merge workflow input for Google bucket URLs
    This workflow preparation uses VCFs that are available in a Google bucket. Results will be placed in your
    workspace bucket.

    Google bucket URLs should have the following format:
    - `gs://[bucket-name]/key/to/my/a-chr1.vcf.gz`
    - `gs://[bucket-name]/key/to/my/a-chr2.vcf.gz`
    - `..`
    - `gs://[bucket-name]/key/to/my/b-chr1.vcf.gz`
    - `gs://[bucket-name]/key/to/my/b-chr2.vcf.gz`
    """

table.delete("vcf-merge-input-bucket")
with herzog.Cell("python"):
    table_name = "vcf-merge-input-bucket"
    table.put_row(table_name, dict(workspace=workspace,
                                   billing_project=workspace_namespace,
                                   inputs=[f"{workspace_bucket}/vcfsa/chr1.vcf.gz",
                                           f"{workspace_bucket}/vcfsb/chr1.vcf.gz"],
                                   output=f"{workspace_bucket}/merged/chr1.vcf.gz"))
    table.put_row(table_name, dict(workspace=workspace,
                                   billing_project=workspace_namespace,
                                   inputs=[f"{workspace_bucket}/vcfsa/chr2.vcf.gz",
                                           f"{workspace_bucket}/vcfsb/chr2.vcf.gz"],
                                   output=f"{workspace_bucket}/merged/chr2.vcf.gz"))

with herzog.Cell("markdown"):
    """
    ## Option C: VCF merge workflow input for mixed Google bucket URLs and DRS URIs
    This workflow preparation uses VCFs that are referenced with either Google bucket URLs or DRS URIs.
    Results will be placed in your workspace bucket.

    Google bucket URLs should have the following format:
    - `gs://[bucket-name]/key/to/my/a-chr1.vcf.gz`
    - `gs://[bucket-name]/key/to/my/a-chr2.vcf.gz`
    - `..`
    - `gs://[bucket-name]/key/to/my/b-chr1.vcf.gz`
    - `gs://[bucket-name]/key/to/my/b-chr2.vcf.gz`
    """

table.delete("vcf-merge-input-bucket")
with herzog.Cell("python"):
    table_name = "vcf-merge-input-mixed"
    table.put_row(table_name,
                  dict(workspace=workspace,
                       billing_project=workspace_namespace,
                       inputs=["drs://dg.4503/697f611b-aa8a-4bd7-a80b-946276273833",
                               f"{workspace_bucket}/vcfs_to_merge/ce212b62-e796-4b32-becb-361f272cead0.vcf.g"],
                       output=f"{workspace_bucket}/merged/mixed.vcf.gz"))
    table.put_row(table_name,
                  dict(workspace=workspace,
                       billing_project=workspace_namespace,
                       inputs=["drs://dg.4503/93286e47-3d09-47e6-ac87-4c2975ef0c3f",
                               f"{workspace_bucket}/vcfs_to_merge/aba6b011-2ab4-4739-beb4-c1eeaee60c74.vcf.gz"],
                       output=f"{workspace_bucket}/merged/mixed.vcf.gz"))


################################################ TESTS ################################################ noqa
resp = fiss.fapi.get_entities(os.environ['GOOGLE_PROJECT'], os.environ['WORKSPACE_NAME'], "vcf-merge-input-drs")
resp.raise_for_status()
rows = resp.json()
row_data = list()
for row in rows:
    assert row['attributes']['workspace'] == os.environ['WORKSPACE_NAME']
    assert row['attributes']['billing_project'] == os.environ['GOOGLE_PROJECT']
    row_data.append((tuple(row['attributes']['inputs']['items']), row['attributes']['output']))

assert (("drs://dg.4503/697f611b-aa8a-4bd7-a80b-946276273833",
         "drs://dg.4503/ce212b62-e796-4b32-becb-361f272cead0"),
        f"{os.environ['WORKSPACE_BUCKET']}/merged/drs_combined_a.vcf.gz") in row_data
assert (("drs://dg.4503/93286e47-3d09-47e6-ac87-4c2975ef0c3f",
         "drs://dg.4503/aba6b011-2ab4-4739-beb4-c1eeaee60c74"),
        f"{os.environ['WORKSPACE_BUCKET']}/merged/drs_combined_b.vcf.gz") in row_data

# assert rows[0]['attributes']['inputs']['items'] == ["drs://dg.4503/697f611b-aa8a-4bd7-a80b-946276273833", "drs://dg.4503/ce212b62-e796-4b32-becb-361f272cead0"]
# assert rows[0]['attributes']['output'] == f"{os.environ['WORKSPACE_BUCKET']}/merged/drs_combined_a.vcf.gz"
# assert rows[1]['attributes']['inputs']['items'] == ["drs://dg.4503/93286e47-3d09-47e6-ac87-4c2975ef0c3f", "drs://dg.4503/aba6b011-2ab4-4739-beb4-c1eeaee60c74"]
# assert rows[1]['attributes']['output'] == f"{os.environ['WORKSPACE_BUCKET']}/merged/drs_combined_b.vcf.gz"

resp = fiss.fapi.get_entities(os.environ['GOOGLE_PROJECT'], os.environ['WORKSPACE_NAME'], "vcf-merge-input-bucket")
resp.raise_for_status()
rows = resp.json()
row_data = list()
for row in rows:
    assert row['attributes']['workspace'] == os.environ['WORKSPACE_NAME']
    assert row['attributes']['billing_project'] == os.environ['GOOGLE_PROJECT']
    row_data.append((tuple(row['attributes']['inputs']['items']), row['attributes']['output']))

assert ((f"{os.environ['WORKSPACE_BUCKET']}/vcfsa/chr1.vcf.gz",
         f"{os.environ['WORKSPACE_BUCKET']}/vcfsb/chr1.vcf.gz"),
        f"{os.environ['WORKSPACE_BUCKET']}/merged/chr1.vcf.gz") in row_data
assert ((f"{os.environ['WORKSPACE_BUCKET']}/vcfsa/chr2.vcf.gz",
         f"{os.environ['WORKSPACE_BUCKET']}/vcfsb/chr2.vcf.gz"),
        f"{os.environ['WORKSPACE_BUCKET']}/merged/chr2.vcf.gz") in row_data
