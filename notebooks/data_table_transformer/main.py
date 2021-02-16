import os
import herzog

# Mock the notebook environment
os.environ['WORKSPACE_NAME'] = "terra-notebook-utils-tests"
os.environ['WORKSPACE_BUCKET'] = "gs://fc-9169fcd1-92ce-4d60-9d2d-d19fd326ff10"
os.environ['GOOGLE_PROJECT'] = "firecloud-cgl"

with herzog.Cell("markdown"):
    """
    ### Use [Pandas](https://pandas.pydata.org) `pivot` command  to convert [AnVIL](https://anvilproject.org)
        sequencing data table imported from Gen3 from long to wide format.
    - `index_column` long format column containing the unique keys of the wide formatted table rows
    - `header_column` long format column containing the new column headers of the wide formatted table
    - `value_columns` long format columns to carry over into the wide formatted table (These may be combined with the
      `header_column` names, e.g. "cram" -> "file_type-cram")

    While this notebook is focused on the AnVIL data model, it can work with any Terra data table with appropriate
    choices for `index_column`, `header_column`, and `value_columns`.

    *author: Brian Hannafious, Genomics Institute, University of California Santa Cruz, bhannafi@ucsc.edu*
    """

with herzog.Cell("markdown"):
    """
    Install the newest version of [terra-notebook-utils](https://github.com/DataBiosphere/terra-notebook-utils)
    and [terra-pandas](https://github.com/xbrianh/terra-pandas)
    """

with herzog.Cell("python"):
    #%pip install --upgrade --no-cache-dir terra-notebook-utils
    #%pip install --upgrade --no-cache-dir terra-pandas
    pass

with herzog.Cell("markdown"):
    """
    Import [Pandas](https://pandas.pydata.org) interface functions from
    [terra-pandas](https://github.com/xbrianh/terra-pandas)
    """

with herzog.Cell("python"):
    from terra_pandas import table_to_dataframe, dataframe_to_table, long_to_wide

with herzog.Cell("markdown"):
    """
    Carry out the long to wide data table transformation, which involves several steps:
      - Convert the Terra data table into a Pandas dataframe with `table_to_dataframe`
      - Filter out rows that cannot be transformed into long format. In this example we are transforming the data table
        into a wide format using sample id. Multisample files, such as jointly called VCFs, will not fit into the
        transformed dataframe. There are also extra complexities of the AnVIL data model that need to be addressed,
        as explained below.
      - Convert the wide-formatted dataframe into a Terra data table.
    """

###################################################################################### noqa
# Provide test fixtures for the following cells
import pandas
from uuid import uuid4
from random import choice, randint
from terra_notebook_utils import table

NUMBER_OF_WIDE_ROWS = 20

long_table = "test-long"
wide_table = "test-wide"

table.delete(long_table)
table.delete(wide_table)

lines = list()
for _ in range(NUMBER_OF_WIDE_ROWS):
    sample_id = f"{uuid4()}"
    sample_obj = dict(foo=f"{uuid4()}", entityName=sample_id)
    for fmt in ["cram", "crai", "vcf"]:
        lines.append({'pfb:sample': sample_obj,
                      'pfb:data_format': fmt,
                      'pfb:object_id': f"{uuid4()}",
                      'pfb:file_size': randint(1024, 1024 ** 3)})
dataframe_to_table("test-long", pandas.DataFrame(lines))
###################################################################################### noqa

with herzog.Cell("python"):
    index_column = "pfb:sample"
    header_column = "pfb:data_format"
    value_columns = ["pfb:object_id", "pfb:file_size"]

    # Uncomment the following lines with your table names
    # long_table = ""  # Include long formatted table to transform
    # wide_table = ""  # Include the wide formatted table to create

    # Convert the long table into a Pandas dataframe
    df = table_to_dataframe(long_table)
    # Filter out rows without pfb:sample (such as multisample files)
    df = df[df[index_column].notna()]
    # "pfb:sample" is a complex field in AnVIL. Convert it to a string container the sample id
    df[index_column] = df[index_column].apply(lambda item: item['entityName'])
    # Use Pandas "pivot" method to convert from long to wide
    df_wide = df.pivot(index=index_column, columns=header_column, values=value_columns)
    # Convert the wide dataframe into a Terra data table
    dataframe_to_table(wide_table, df_wide)

assert NUMBER_OF_WIDE_ROWS == len(table_to_dataframe(wide_table))

with herzog.Cell("markdown"):
    """
    Alternatively to the above cell, [terra-pandas](https://github.com/xbrianh/terra-pandas) provides a convenience
    function to carry out the long to wide data table transformation with a single command.
    """

with herzog.Cell("python"):
    # long_to_wide("sequencing",
    #              "sequencing-wide-two",
    #              index_column="pfb:specimen_id",
    #              header_column="pfb:data_format",
    #              value_columns=["pfb:sample", "pfb:object_id", "pfb:file_size"])"
    pass

with herzog.Cell("markdown"):
    """
    While data tables may be deleted via the Terra UI, it is often useful to programatical delete large tables, or
    several tables at the same time.
    """

with herzog.Cell("python"):
    from terra_notebook_utils import table
    # populate this list with the Terra data tables you would like to delete.
    # For example, tables_to_delete: list = [] = ["my-first-table", "my-second-table"]
    tables_to_delete: list = []
    for table_name in tables_to_delete:
        table.delete(table_name)

with herzog.Cell("markdown"):
    """
    ## Contributions
    Contributions, bug reports, and feature requests are welcome on:
      - [terra-notebook-utils GitHub](https://github.com/DataBiosphere/terra-notebook-utils) for low level data table interfaces.
      - [terra-pandas GitHub](https://github.com/xbrianh/terra-pandas) for integration between data tables and Pandas DataFrames.
      - [featured-notebooks GitHub](https://github.com/DataBiosphere/featured-notebooks) for this notebook.
    """
