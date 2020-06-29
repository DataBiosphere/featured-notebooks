import os
import callysto
from unittest import mock

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

get_drs_urls = mock.MagicMock()

with callysto.Cell("python"):
    crams = get_drs_urls("submitted_aligned_reads")                                                           
    crais = get_drs_urls("aligned_reads_index")

with callysto.Cell("python"):
    # Select samples to view
    # to see a list of samples, uncomment the following line
    # print(crams.keys())
    samples = ["NWD263776", "NWD552521"]

crams = dict()
crais = dict()
for s in samples:
    crams[s] = dict(file_name=f"{s}.cram", drs_url="drs://{s}")
    crais[s] = dict(file_name=f"{s}.crai", drs_url="drs://{s}")
tnu.drs.copy = mock.MagicMock()

with callysto.Cell("python"):
    # Copy cram and crai files, for selected samples, to the workspace bucket
    bucket = os.environ['WORKSPACE_BUCKET']
    pfx = "test-crai-cram"
    tsv_data = "\t".join(["cram_crai_test_id", "inputs", "output"])
    for sample in samples:
        cram = crams[sample]
        crai = crais[sample]
        tnu.drs.copy(cram['drs_url'], f"{bucket}/{pfx}/{cram['file_name']}")
        tnu.drs.copy(crai['drs_url'], f"{bucket}/{pfx}/{crai['file_name']}")

upload_data_table = mock.MagicMock()

with callysto.Cell("python"):
    # Create the IGV viewer data table
    tsv_data = "\t".join(["IGV_viewer_id", "sample", "cram", "crai"])
    for i, sample in enumerate(samples):
        cram = crams[sample]
        crai = crais[sample]
        tsv_data += os.linesep + "\t".join([f"{i}",
                                            f"{sample}",
                                            f"{bucket}/{pfx}/{cram['file_name']}",
                                            f"{bucket}/{pfx}/{crai['file_name']}"])
    upload_data_table(tsv_data)
