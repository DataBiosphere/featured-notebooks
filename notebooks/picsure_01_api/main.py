import os
import shutil
import callysto

# Mock the notebook environment
os.environ['WORKSPACE_NAME'] = "terra-notebook-utils-tests"
os.environ['WORKSPACE_BUCKET'] = "gs://fc-9169fcd1-92ce-4d60-9d2d-d19fd326ff10"
os.environ['GOOGLE_PROJECT'] = "firecloud-cgl"


def picsure_setup():
    import os
    import requests
    url = 'https://raw.githubusercontent.com/hms-dbmi/Access-to-Data-using-PIC-SURE-API/master/NHLBI_BioData_Catalyst/python/python_lib/utils.py'
    r = requests.get(url)
    dir_name = os.path.join(os.path.dirname(__file__), 'python_lib')
    os.makedirs(dir_name, exist_ok=True)
    with open(f'{dir_name}/utils.py', 'wb') as f:
        f.write(r.content)
    with open(f'{dir_name}/__init__.py', 'wb') as f:
        f.write(b'\n')
    if not os.environ.get('PICSURE_TOKEN'):
        raise RuntimeError('Please follow the instructions here to set PICSURE_TOKEN: https://terra.biodatacatalyst.nhlbi.nih.gov/#workspaces/biodata-catalyst/BioData%20Catalyst%20PIC-SURE%20API%20Python%20examples/notebooks/launch/Workspace_setup.ipynb')
    with open('token.txt', 'w') as f:
        f.write(os.environ.get('PICSURE_TOKEN'))


def clean_up_notebook():
    os.remove('token.txt')
    dir_name = os.path.join(os.path.dirname(__file__), 'python_lib')
    shutil.rmtree(dir_name, ignore_errors=True)


picsure_setup()

##########################################
# begin actual notebook after this point #
##########################################
with callysto.Cell("python"):
    #!cat requirements.txt
    pass

with callysto.Cell("python"):
    import sys
    #!{sys.executable} -m pip install -r requirements.txt

with callysto.Cell("python"):
    # !{sys.executable} -m pip install --upgrade --force-reinstall git+https://github.com/hms-dbmi/pic-sure-python-adapter-hpds.git
    # !{sys.executable} -m pip install --upgrade --force-reinstall git+https://github.com/hms-dbmi/pic-sure-python-client.git
    pass

with callysto.Cell("python"):
    import json
    from pprint import pprint

    import pandas as pd
    import numpy as np
    import matplotlib.pyplot as plt
    from scipy import stats

    import PicSureHpdsLib
    import PicSureClient

    from python_lib.utils import get_multiIndex_variablesDict, joining_variablesDict_onCol

with callysto.Cell("python"):
    # Pandas DataFrame display options
    pd.set_option("max.rows", 100)

    # Matplotlib display parameters
    plt.rcParams["figure.figsize"] = (14, 8)
    font = {'weight': 'bold',
            'size': 12}
    plt.rc('font', **font)

with callysto.Cell("python"):
    PICSURE_network_URL = "https://picsure.biodatacatalyst.nhlbi.nih.gov/picsure"
    resource_id = "02e23f52-f354-4e8b-992c-d37c8b9ba140"
    token_file = 'token.txt'

with callysto.Cell("python"):
    with open(token_file, "r") as f:
        my_token = f.read()

with callysto.Cell("python"):
    client = PicSureClient.Client()
    connection = client.connect(PICSURE_network_URL, my_token)
    adapter = PicSureHpdsLib.Adapter(connection)
    resource = adapter.useResource(resource_id)

with callysto.Cell("python"):
    resource.help()

with callysto.Cell("python"):
    dictionary = resource.dictionary()
    dictionary_search = dictionary.find("COPD")

with callysto.Cell("python"):
    pprint({"Count": dictionary_search.count(),
            "Keys": dictionary_search.keys()[0:5],
            "Entries": dictionary_search.entries()[0:5]})

with callysto.Cell("python"):
    dictionary_search.DataFrame().head()

with callysto.Cell("python"):
    plain_variablesDict = resource.dictionary().find().DataFrame()

with callysto.Cell("python"):
    resource.dictionary().help()

with callysto.Cell("python"):
    plain_variablesDict.iloc[10:20, :]

with callysto.Cell("python"):
    variablesDict = get_multiIndex_variablesDict(plain_variablesDict)

with callysto.Cell("python"):
    variablesDict

with callysto.Cell("python"):
    # Now that we have seen how our entire dictionnary looked, we limit the number of lines to be displayed for the future outputs
    pd.set_option("max.rows", 50)

with callysto.Cell("python"):
    mask_medication = variablesDict.index.get_level_values(2) == "Medication History"
    mask_medical = variablesDict.index.get_level_values(2) == "Medical History"
    medication_history_variables = variablesDict.loc[mask_medical | mask_medication, :]
    medication_history_variables

with callysto.Cell("python"):
    my_query = resource.query()

with callysto.Cell("python"):
    mask = variablesDict["simplified_name"] == "How old were you when you completely stopped smoking? [Years old]"
    yo_stop_smoking_varname = variablesDict.loc[mask, "name"]

with callysto.Cell("python"):
    mask_cat = variablesDict["categorical"] is True
    mask_count = variablesDict["observationCount"].between(100, 2000)
    varnames = variablesDict.loc[mask_cat & mask_count, "name"]

with callysto.Cell("python"):
    my_query.filter().add(yo_stop_smoking_varname, min=20, max=70)
    my_query.select().add(varnames[:50])

with callysto.Cell("python"):
    query_result = my_query.getResultsDataFrame(low_memory=False)

with callysto.Cell("python"):
    query_result.shape

with callysto.Cell("python"):
    query_result[yo_stop_smoking_varname].plot.hist(legend=None, title="Age stopping smoking", bins=15)

with callysto.Cell("python"):
    DataSetID = 'ef00e9c1-526f-47d5-b376-ef4b08b2a544'

    results = resource.retrieveQueryResults(DataSetID)

    from io import StringIO

    df_UI = pd.read_csv(StringIO(results), low_memory=False)

    df_UI.head()

with callysto.Cell("python"):
    len(df_UI['Patient ID'].unique())

with callysto.Cell("python"):
    import io
    import os

    bucket = os.environ['WORKSPACE_BUCKET'] + "/"

with callysto.Cell("python"):
    df_UI.to_csv("harmonized_sex_male_data.csv")

with callysto.Cell("python"):
    #! gsutil cp harmonized_sex_male_data.csv {bucket}
    pass
