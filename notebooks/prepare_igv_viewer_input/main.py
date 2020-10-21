import os
import callysto
from unittest import mock

os.environ['WORKSPACE_NAME'] = "terra-notebook-utils-tests"
os.environ['WORKSPACE_BUCKET'] = "gs://fc-9169fcd1-92ce-4d60-9d2d-d19fd326ff10"
os.environ['GOOGLE_PROJECT'] = "firecloud-cgl"

with callysto.Cell("markdown"):
    """
    # Generate a data table with data from Gen3 for use with the IGV in Terra

    The [Integrative Genomics Viewer](http://software.broadinstitute.org/software/igv/) is an interactive visualization
    tool for large genomic files. The tool is available within Terra. This notebook leads you through some data
    wrangling steps to generate a data table that works with the [IGV tool in Terra](https://support.terra.bio/hc/en-us/articles/360029654831-Viewing-IGV-tracks-of-BAM-files-in-your-workspace-data)
    using data imported from Gen3.

    The final data table generated will look like this:

    | IGV_Viewer_id | crai           | cram           |
    |---------------|----------------|----------------|
    | 0         | NWD1.crai |NWD1.cram  |
    | 1         | NWD2.crai  | NWD2.cram  |


    Outline of steps in this notebook:
    1. Transfer a project with your samples of interest from Gen3 to your Terra workspace using these [instructions]().
       The genomic data that arrives in your data tables are DRS links.
    2. Use DRS tooling from [terra_notebook_utils package](https://support.terra.bio/hc/en-us/articles/360039330211)
       to physically copy the genomic data of interest to your Terra workspace. In this notebook, you will provide the
       TOPMed NWD sequencing ID to find the CRAM and CRAI files of interest.
       Note: The IGV tool in Terra cannot resolve the data through drs:// URLS. Note: you will be paying storage costs
       for any data you copy to your workspace. You may want to delete these files when you are finished viewing them.
    3. Generate a new data table, IGV_viwer_id, where each row represents an individual and columns contain links to
       the data in your workspace (gs://*.cram).
    4. Navigate to the data section of your workspace and open the IGV_viewer table. Follow the instructions in step 1
       of this [document](https://support.terra.bio/hc/en-us/articles/360029654831-Viewing-IGV-tracks-of-BAM-files-in-your-workspace-data).
    5. You may want to eventually delete the CRAM and CRAI files to avoid paying long-term storage costs.
    """

with callysto.Cell("markdown"):
    """
    Install DRS packages
    """

with callysto.Cell("python"):
    #%pip install --upgrade --no-cache-dir terra-notebook-utils
    #%pip install --upgrade --no-cache-dir gs-chunked-io
    pass

with callysto.Cell("markdown"):
    """
    Import the tooling and define some functions
    """

with callysto.Cell("python"):
    import os
    from firecloud import fiss
    import terra_notebook_utils as tnu

    def get_drs_urls(table_name):
        """
        Return a dictionary containing drs urls and file names, using sample as the key.
        """
        info = dict()
        for row in tnu.table.list_entities(table_name):
            drs_url = row['attributes']['object_id']
            file_name = row['attributes']['file_name']
            # Assume file names have the format `NWD244548.b38.irc.v1.cram`
            sample = file_name.split(".", 1)[0]
            info[sample] = dict(file_name=file_name, drs_url=drs_url)
        return info

    def upload_data_table(tsv):
        billing_project = os.environ['GOOGLE_PROJECT']
        workspace = os.environ['WORKSPACE_NAME']
        resp = fiss.fapi.upload_entities(billing_project, workspace, tsv, model="flexible")
        resp.raise_for_status()

get_drs_urls = mock.MagicMock()  # noqa

with callysto.Cell("python"):
    crams = get_drs_urls("submitted_aligned_reads")
    crais = get_drs_urls("aligned_reads_index")

with callysto.Cell("markdown"):
    """
    Select the samples you want to view
    """

with callysto.Cell("python"):
    samples = ["NWD263776", "NWD552521"]

with callysto.Cell("markdown"):
    """
    Check that this worked
    """

with callysto.Cell("python"):
    print(crams["NWD263776"])
    print(crais["NWD263776"])

crams = dict()
crais = dict()
for s in samples:
    crams[s] = dict(file_name=f"{s}.cram", drs_url="drs://{s}")
    crais[s] = dict(file_name=f"{s}.crai", drs_url="drs://{s}")
tnu.drs.copy = mock.MagicMock()

with callysto.Cell("markdown"):
    """
    Copy the CRAM and CRAI files for the selected samples to the Terra workspace bucket.
    """

with callysto.Cell("python"):
    bucket = os.environ['WORKSPACE_BUCKET']
    pfx = "test-crai-cram"
    tsv_data = "\t".join(["cram_crai_test_id", "inputs", "output"])
    for sample in samples:
        cram = crams[sample]
        crai = crais[sample]
        tnu.drs.copy(cram['drs_url'], f"{bucket}/{pfx}/{cram['file_name']}")
        tnu.drs.copy(crai['drs_url'], f"{bucket}/{pfx}/{crai['file_name']}")

upload_data_table = mock.MagicMock()  # noqa

with callysto.Cell("markdown"):
    """
    Create a data table called "IGV_viewer" with all CRAM and CRAI files that were copied to the workspace
    """

with callysto.Cell("python"):
    tsv_data = "\t".join(["IGV_viewer_id", "sample", "cram", "crai"])
    for i, sample in enumerate(samples):
        cram = crams[sample]
        crai = crais[sample]
        tsv_data += os.linesep + "\t".join([f"{i}",
                                            f"{sample}",
                                            f"{bucket}/{pfx}/{cram['file_name']}",
                                            f"{bucket}/{pfx}/{crai['file_name']}"])
    upload_data_table(tsv_data)

with callysto.Cell("markdown"):
    """
    When you are done viewing, delete the files you copied so you avoid paying long-term storage costs. If you delete
    the data table, this doesn't actually delete the files in your bucket. You will need to navigate to the "file"
    section of your workspace and individually delete the files in the "folders" labeled "cram" and "crai".
    """
