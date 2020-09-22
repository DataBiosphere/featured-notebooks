#publish to: "terra-notebook-utils-tests" "test-byod-ash"
#!/usr/bin/env python
# coding: utf-8

# # Bring your own data to your Terra workspace and organize it in a data table
# *version: 2.0*

# If you are planning to upload many files to your Terra workspace, we recommend you organize your data into a Terra
# data table. This is especially helpful when you plan to run workflows with this data because you can avoid pasting
# in the "gs://" links to every file and instead use Terra's helpful UI features.
# 
# 
# In this example, we introduce tools to help you:
# 
# 1. Programmatically upload data from your local machine to your Terra workspace using `gsutil cp`.
# 
# 2. Programmatically generate a data table that conatins your samples CRAM and CRAI files. The end result will look
#    like this table:
# 
# | sample_id | cram       | crai      |
# | --------- | ---------  | --------- |
# | NWD1      | NWD1.cram  | NWD1.crai |
# | NWD2      | NWD2.cram  | NWD2.crai |

# ## Assumptions
# * You are not trying to overwrite a data table that already exists
# * Your files follow a naming convention either like this...
# NWD119844.CRAM
# NWD119844.CRAM.CRAI
# ...or this:
# NWD119844.CRAM
# NWD119844.CRAI
# 
# Files that lack the extension .cram or .crai will not be added to the data table.

# # Install requirements
# Whenever `pip install`ing on a notebook on Terra, restart the kernal after the installation.

# In[1]:


#install necessary libraries
get_ipython().run_line_magic('pip', 'install --upgrade --no-cache-dir terra-notebook-utils')
get_ipython().run_line_magic('pip', 'install --upgrade --no-cache-dir gs-chunked-io')


# Next come imports and environmental variables.

# In[45]:


import io
import os
import google.cloud.storage
import pandas as pd
import firecloud.api as fapi # is this redundant with fiss? my code uses firecloud.api, should it use fiss?
from uuid import uuid4
from firecloud import fiss
from collections import defaultdict
from typing import Any, List, Set, Dict, Iterable
from terra_notebook_utils import gs

storage_client = google.cloud.storage.Client()
google_project = os.environ['GOOGLE_PROJECT']
workspace = os.environ['WORKSPACE_NAME']
bucket_clipped = os.environ["WORKSPACE_BUCKET"][len("gs://"):]


# Finally, here are the functions we'll be using for creating data tables.

# In[46]:


def upload_columns(table: str, columns: Dict[str, List[Any]]):
    column_headers = sorted(columns.keys())
    number_of_rows = len(columns[set(columns.keys()).pop()])
    tsv_data = "\t".join([f"{table}_id", *column_headers])
    for i in range(number_of_rows):
        tsv_data += os.linesep + "\t".join([f"{i}", *[columns[h][i] for h in column_headers]])
    upload_data_table(tsv_data)

def upload_data_table(tsv):
    resp = fiss.fapi.upload_entities(google_project, workspace, tsv, model="flexible")
    resp.raise_for_status()

def create_cram_crai_table_pt(subdir: str):
    listOfRows = []
    PARENT_FILETYPE = "cram"
    CHILD_FILETYPE = "crai"
    
    # For item in Google Storage Bucket...
    for blob in storage_client.list_blobs(bucket_clipped, prefix=subdir):
        
        #If the item ends with the parent filetype (CRAM in this case)
        if blob.name.endswith(PARENT_FILETYPE):
            
            # Remove ".cram" extension and search for the basename (ie NWD2920.cram --> NWD2920)
            basename = blob.name[:-len(f'.{PARENT_FILETYPE}')]
            
            # For item containing the basename in Google Storage Bucket...
            for basename_blob in storage_client.list_blobs(bucket_clipped, prefix=basename):
                
                # If it ends with the child extension (in this case, CRAI)...
                if basename_blob.name.endswith(CHILD_FILETYPE):
                    
                    # Format the table's four cells
                    parent_filename = blob.name.split('/')[-1]
                    sample_id = parent_filename[:-5]
                    parent_location = "gs://"+f'{bucket_clipped}/{blob.name}'
                    child_location  = "gs://"+f'{bucket_clipped}/{basename_blob.name}'
                    
                    # Make a list representing a row (with four cells), and add that to the table list
                    table_row = ([sample_id, parent_location, child_location])
                    listOfRows.append(table_row)
                    
                # If no CRAI is found, the CRAM will not be added at all
                
    # Once all items have been gone through, add to a pandas dataframe
    dfCramsCrais = pd.DataFrame(listOfRows, 
                                columns=['sample'+'ID', 
                                         'cram'+'Location', 
                                         'crai'+'Location'])
    return dfCramsCrais


# # Upload to your bucket with gsutil

# ## Install gsutil as directed by this [document](https://support.terra.bio/hc/en-us/articles/360024056512-Uploading-to-a-workspace-Google-bucket#h_01EGP8GR3G10SKRXAC7H1ENXQ3).
# Use the option "Set up gsutil on your local computer (step-by-step install)" which will allow you to upload files from your computer directly to Terra. Using instructions from this document, you will use the gsutil tool to upload data from your local computer to your Terra workspace. To effectively use the example here, we suggest you upload the data with the suggestions
# below.

# ### Find the path to this workspace bucket
# 

# Using the os package, you can print your workspace bucket path.
# 

# In[60]:


bucket = os.environ["WORKSPACE_BUCKET"]
print(bucket)


# ### Add a prefix to your bucket path to organize your data
# In this example, we add the prefix 'test-crai-cram'. In the terminal of your computer, you will call something like:
# 
# `gsutil cp /Users/my-cool-username/Documents/Example.cram gs://your_bucket_info/my-crams/`
# 
# We will be calling what comes after your bucket info, here represented as `my-crams`, as your sudirectory.

# ## Preview the data in your workspace bucket
# Be aware that if you have uploaded multiple files, all of them will appear with this ls command. It will also contain one folder for every workflow you have run in this workspace. You may want to skip this step if you're after uploading hundreds of files. However, if you have imported data tables from Gen3, they will not show up here as the files within are only downloaded when their associated TSV tables are called upon by workflows or iPython notebooks.

# In[48]:


get_ipython().system('gsutil ls {bucket}')


# # Generate a data table that links to the data in your workspace bucket
# 
# To generate a Terra data table associating crams, crais, and sample ids (e.g. "NWD1"), use the snippet:
# ```
# listing = [key for key in gs.list_bucket("my-crams")]
# create_cram_crai_table("my-table-name", listing)
# ```
# 
# For example, the listing
# ```
# gs://my-workspace-bucket/my-crams/NWD1.cram
# gs://my-workspace-bucket/my-crams/NWD1.crai
# gs://my-workspace-bucket/my-crams/NWD2.cram
# gs://my-workspace-bucket/my-crams/NWD2.crai
# gs://my-workspace-bucket/my-crams/NWD3.cram
# gs://my-workspace-bucket/my-crams/NWD3.crai
# ```
# would produce a table that looks like this in Terra.
# 
# | sample_id | cramLocation       | craiLocation      |
# | --------- | -----------------  | ----------------- |
# | NWD1      | NWD1.cram          | NWD1.crai         |
# | NWD2      | NWD2.cram          | NWD2.crai         |
# | NWD3      | NWD3.cram          | NWD3.crai         |
# 
# However, the actual raw data generated will be more like this:
# 
# | entity:sample_id | cramLocation       | craiLocation      |
# | ---------        | -----------------  | ---------         |
# | NWD1             | gs://my-workspace-bucket/my-crams/NWD1.cram          | gs://my-workspace-bucket/my-crams/NWD1.crai         |
# | NWD2             | gs://my-workspace-bucket/my-crams/NWD2.cram          | gs://my-workspace-bucket/my-crams/NWD2.crai         |
# | NWD3             | gs://my-workspace-bucket/my-crams/NWD3.cram          | gs://my-workspace-bucket/my-crams/NWD3.crai         |

# In[49]:


# This needs to be the subdirectory that's used in your bucket
subdirectory = "my-crams"
dfCramsCrais = create_cram_crai_table_pt(subdirectory)


# Now, go check the data section of your workspace and you should a data table with the name you have given it, and that table should act as a directory of your files.
# 
# If you set the name of your table to a table that already exists, the old table will not be overwritten, but the new table won't be created either. Terra tables cannot be updated, they must be deleted and remade.
# 
# # Upload new table
# Run the code below to upload your data table to Terra. Then, if you go to the data section of your workspace, you will see a brand new table containing your data. This table can be used as an input to a workflow.

# In[61]:


dfCramsCrais.to_csv("dataframe.tsv", sep='\t')

# Format resulting TSV file to play nicely with Terra 
with open('dataframe.tsv', "r+") as file1:
    header = file1.readline()
    everything_else = file1.readlines()
full_header="entity:"+"cramsAndCrais"+"_id"+header
with open('final.tsv', "a") as file2:
    file2.write(full_header)
    for string in everything_else:
        # Zfill the index
        columns = string.split('\t')
        columns[0] = columns[0].zfill(5)
        file2.write('\t'.join(columns))
    
# Clean up
response = fapi.upload_entities_tsv(google_project, workspace, "final.tsv", "flexible")
fapi._check_response_code(response, 200)


# Note: If you get a FireCloudServerError that reads "Duplicated entities are not allowed in TSV", this may be because you ran the code more than once without deleting `final.tsv` first. If you don't delete that file before re-running the notebook, Firecloud will consider your second upload to be a duplicate and block it.

# ## Inspect head of the dataframe
# In addition to seeing the to being able to see the datatable in Terra directly, you can also get a peek at it here. As you can see, we had to do some manipulation, such as making the first row start with "entity" due to Terra's datatable limitations. If you adapt this code for your own purposes, your tables will need to follow the format of `entity:"+TABLE_NAME+"_id`, followed by tab seperating the rest of the columns, for their first line.

# In[54]:


get_ipython().system('head final.tsv')


# ## Tidy up notebook workspace

# In[55]:


get_ipython().system('rm dataframe.tsv')
get_ipython().system('rm final.tsv')


# # Merge data tables across sample ids
# 
# Data tables can be joined across any column of shared values. For instance, the following tables can be joined with
# the `sample_id` column:
# 
# | sample_id | cram       | crai      |
# | --------- | ---------  | --------- |
# | NWD1      | NWD1.cram  | NWD1.crai |
# | NWD2      | NWD2.cram  | NWD2.crai |
# | NWD3      | NWD3.cram  | NWD3.crai |
# 
# | sample_id | first_name | last_name |
# | --------- | ---------  | --------- |
# | NWD1      | Bob        | Frank     |
# | NWD2      | Sue        | Lee       |
# | NWD3      | Adrian     | Zap       |
# 
# | sample_id | Diabetic   |
# | --------- | ---------  |
# | NWD1      | No         |
# | NWD3      | Yes        |
# 
# The code snippet
# ```
# join_data_tables("joined_table_name", ["cram_crai_table", "name_table", "diabetic_table"], "sample_id")
# ```
# produces the combined table
# 
# | sample_id | cram       | crai      | first_name | last_name | Diabetic   |
# | --------- | ---------  | --------- | ---------  | --------- | ---------  |
# | NWD1      | NWD1.cram  | NWD1.crai | Bob        | Frank     | No         |
# | NWD3      | NWD3.cram  | NWD3.crai | Adrian     | Zap       | Yes        |
# 
# Note that the row for `NWD2` is missing from the combined table since it was not present in `diabetic_table`.
# 
# 

# ## Additional Functions
# We made some additional functions that you can use to organize your data further.

# In[ ]:


def upload_rows(table: str, rows: List[Dict[str, Any]]):
    assert rows
    columns = sorted(rows[0].keys())
    tsv_data = "\t".join([f"{table}_id", *columns])
    for i, row in enumerate(rows):
        tsv_data += os.linesep + "\t".join([f"{i}", *[row[c] for c in columns]])
    upload_data_table(tsv_data)

def iter_ents(table: str):
    resp = fiss.fapi.get_entities(google_project, workspace, table)
    resp.raise_for_status()
    for item in resp.json():
        yield item

def iter_rows(table: str):
    for item in iter_ents(table):
        yield item['attributes']

def get_columns(table: str) -> Dict[str, List[Any]]:
    columns = defaultdict(list)
    for row in iter_rows(table):
        for key, val in row.items():
            columns[key].append(val)
    return dict(columns)

def delete_table(table: str):
    rows_to_delete = [dict(entityType=e['entityType'], entityName=e['name'])
                      for e in iter_ents(table)]
    resp = fiss.fapi.delete_entities(google_project, workspace, rows_to_delete)
    resp.raise_for_status()

def get_keyed_rows(table_name: str, key_column: str) -> Dict[str, Dict[str, Any]]:
    keyed_rows = dict()
    for row in iter_rows(table_name):
        key = row[key_column]
        assert key not in keyed_rows
        keyed_rows[key] = row
        del keyed_rows[key][key_column]
    return keyed_rows

def keyed_row_columns(keyed_rows: Dict[str, Any]) -> Set[str]:
    if keyed_rows:
        random_key = set(keyed_rows.keys()).pop()
        return set(keyed_rows[random_key].keys())
    else:
        return set()

def join_keyed_rows(keyed_rows_a: Dict[str, Any], keyed_rows_b: Dict[str, Any]) -> Dict[str, Any]:
    a_columns = keyed_row_columns(keyed_rows_a)
    b_columns = keyed_row_columns(keyed_rows_b)
    assert not a_columns.intersection(b_columns), "Keyed rows to join may not share columns"
    common_keys = set(keyed_rows_a.keys()).union(set(keyed_rows_b.keys()))
    return {k: dict(**keyed_rows_a.get(k, {c: BLANK_CELL_VALUE for c in a_columns}),
                    **keyed_rows_b.get(k, {c: BLANK_CELL_VALUE for c in b_columns}))
            for k in common_keys}

def join_data_tables(new_table: str, tables_to_join: list, join_column: str):
    keyed_rows = get_keyed_rows(tables_to_join[0], join_column)
    for table_name in tables_to_join[1:]:
        keyed_rows = join_keyed_rows(keyed_rows, get_keyed_rows(table_name, join_column))
    upload_rows(new_table, [{join_column: k, **row} for k, row in keyed_rows.items()])


# In[ ]:




