# publish to: "terra-notebook-utils-tests" "test"
import os
import callysto

with callysto.Cell("markdown"):
    """
    # xvcfmerge with `Array[String]` input

    This notebook demonstrates the creation of Terra data tables as input to xvcfmerge workflows, referencing input VCFs with Array[String].

    This input style is in development. xvcfmerge workflows should use version "xbrianh-input-format"

    # Cohort VCF Merge

    The TopMED data indexed by BioData Catalyst has been jointly called for each Freeze. TOPMed researchers not part of the TOPMed Constorium receive jointly called VCFs subsetted by project and consent code. If users have access to multiple projects and consent codes, they may wish to re-combine these VCFs. This notebook demonstrates merge tooling for jointly called VCFs.

    ## Aquire the Cohort VCFs to be merged

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
     - [xvcfmerge](https://dockstore.org/workflows/github.com/DataBiosphere/xvcfmerge:xbrianh-input-format?tab=info)

    Workflow execution time is typically ~20 minutes per VCF.
    """

os.environ['WORKSPACE_NAME'] = "terra-notebook-utils-tests"
os.environ['WORKSPACE_BUCKET'] = "gs://fc-9169fcd1-92ce-4d60-9d2d-d19fd326ff10"
os.environ['GOOGLE_PROJECT'] = "firecloud-cgl"

with callysto.Cell("python"):
    import os
    from firecloud import fiss

    # Function to upload TSVs to a Terra Data Table
    def upload_data_table(tsv: str):
        resp = fiss.fapi.upload_entities(os.environ['GOOGLE_PROJECT'],
                                         os.environ['WORKSPACE_NAME'],
                                         tsv,
                                         model="flexible")
        resp.raise_for_status()

    # Function to modify Terra Data Table rows
    def update_row(table: str, row_name: str, updates: dict):
        fiss_updates = [fiss.fapi._attr_set(column, value)
                        for column, value in updates.items()]
        resp = fiss.fapi.update_entity(os.environ['GOOGLE_PROJECT'],
                                       os.environ['WORKSPACE_NAME'],
                                       table,
                                       row_name,
                                       fiss_updates)
        resp.raise_for_status()

with callysto.Cell("python"):
    # Prepare the merge workflow input data table for DRS URIs.
    # This is a typical workflow preparation for merging TOPMed VCFs _without_ downloading them to your workspace bucket.
    # Results will be placed in your workspace bucket.
    bucket = os.environ['WORKSPACE_BUCKET']
    table = "vcf-merge-workflow-input-drs"

    tsv_data = "\t".join([f"{table}_id", "workspace", "billing_project"])
    for row_name in ["drs_combined_a", "drs_combined_b"]:
        tsv_data += os.linesep + "\t".join([row_name, os.environ['WORKSPACE_NAME'], os.environ['GOOGLE_PROJECT']])
    upload_data_table(tsv_data)

    update_row(table, row_name="drs_combined_a", updates=dict(inputs=["drs://dg.4503/697f611b-aa8a-4bd7-a80b-946276273833",
                                                                      "drs://dg.4503/ce212b62-e796-4b32-becb-361f272cead0"],
                                                              output=f"{bucket}/merged/drs_combined_a.vcf.gz"))

    update_row(table, row_name="drs_combined_b", updates=dict(inputs=["drs://dg.4503/93286e47-3d09-47e6-ac87-4c2975ef0c3f",
                                                                      "drs://dg.4503/aba6b011-2ab4-4739-beb4-c1eeaee60c74"],
                                                              output=f"{bucket}/merged/drs_combined_b.vcf.gz"))

with callysto.Cell("python"):
    # Prepare the merge workflow input data table for Google Storage URIs.
    # This workflow preparation uses VCFs that are present in your workspace bucket.
    # Results will be placed in your workspace bucket.
    bucket = os.environ['WORKSPACE_BUCKET']
    table = "vcf-merge-workflow-input-bucket"

    tsv_data = "\t".join([f"{table}_id", "workspace", "billing_project"])
    for row_name in ["chr1", "chr2"]:
        tsv_data += os.linesep + "\t".join([row_name, os.environ['WORKSPACE_NAME'], os.environ['GOOGLE_PROJECT']])
    upload_data_table(tsv_data)

    update_row(table, row_name="chr1", updates=dict(inputs=[f"{bucket}/vcfsa/chr1.vcf.gz", f"{bucket}/vcfsb/chr1.vcf.gz"],
                                                    output=f"{bucket}/merged/chr1.vcf.gz"))

    update_row(table, row_name="chr2", updates=dict(inputs=[f"{bucket}/vcfsa/chr2.vcf.gz", f"{bucket}/vcfsb/chr2.vcf.gz"],
                                                    output=f"{bucket}/merged/chr2.vcf.gz"))


################################################ TESTS ################################################
from firecloud import fiss
resp = fiss.fapi.get_entities(os.environ['GOOGLE_PROJECT'], os.environ['WORKSPACE_NAME'], "vcf-merge-workflow-input-drs")
resp.raise_for_status()
rows = resp.json()
for row in rows:
    assert row['attributes']['workspace'] == os.environ['WORKSPACE_NAME']
    assert row['attributes']['billing_project'] == os.environ['GOOGLE_PROJECT']
assert rows[0]['attributes']['inputs'] == ["drs://dg.4503/697f611b-aa8a-4bd7-a80b-946276273833", "drs://dg.4503/ce212b62-e796-4b32-becb-361f272cead0"]
assert rows[0]['attributes']['output'] == f"{os.environ['WORKSPACE_BUCKET']}/merged/drs_combined_a.vcf.gz"
assert rows[1]['attributes']['inputs'] == ["drs://dg.4503/93286e47-3d09-47e6-ac87-4c2975ef0c3f", "drs://dg.4503/aba6b011-2ab4-4739-beb4-c1eeaee60c74"]
assert rows[1]['attributes']['output'] == f"{os.environ['WORKSPACE_BUCKET']}/merged/drs_combined_b.vcf.gz"

resp = fiss.fapi.get_entities(os.environ['GOOGLE_PROJECT'], os.environ['WORKSPACE_NAME'], "vcf-merge-workflow-input-bucket")
resp.raise_for_status()
rows = resp.json()
for row in rows:
    assert row['attributes']['workspace'] == os.environ['WORKSPACE_NAME']
    assert row['attributes']['billing_project'] == os.environ['GOOGLE_PROJECT']
assert rows[0]['attributes']['inputs'] == [f"{os.environ['WORKSPACE_BUCKET']}/vcfsa/chr1.vcf.gz", f"{os.environ['WORKSPACE_BUCKET']}/vcfsb/chr1.vcf.gz"]
assert rows[0]['attributes']['output'] == f"{os.environ['WORKSPACE_BUCKET']}/merged/chr1.vcf.gz"
assert rows[1]['attributes']['inputs'] == [f"{os.environ['WORKSPACE_BUCKET']}/vcfsa/chr2.vcf.gz", f"{os.environ['WORKSPACE_BUCKET']}/vcfsb/chr2.vcf.gz"]
assert rows[1]['attributes']['output'] == f"{os.environ['WORKSPACE_BUCKET']}/merged/chr2.vcf.gz"
