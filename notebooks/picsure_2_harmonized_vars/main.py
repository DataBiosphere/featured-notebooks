import os
import callysto

# Mock the notebook environment
os.environ['WORKSPACE_NAME'] = "terra-notebook-utils-tests"
os.environ['WORKSPACE_BUCKET'] = "gs://fc-9169fcd1-92ce-4d60-9d2d-d19fd326ff10"
os.environ['GOOGLE_PROJECT'] = "firecloud-cgl"


def initial_setup_from_previous_notebook():
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


initial_setup_from_previous_notebook()

##########################################
# begin actual notebook after this point #
##########################################


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
    print(
        "NB: This Jupyter Notebook has been written using PIC-SURE API following versions:\n- PicSureHpdsLib: 1.1.0\n- PicSureClient: 1.1.0")
    print("The installed PIC-SURE API libraries versions:\n- PicSureHpdsLib: {0}\n- PicSureClient: {1}".format(
        PicSureHpdsLib.__version__, PicSureClient.__version__))

with callysto.Cell("python"):
    PICSURE_network_URL = "https://picsure.biodatacatalyst.nhlbi.nih.gov/picsure"
    resource_id = "02e23f52-f354-4e8b-992c-d37c8b9ba140"
    token_file = "token.txt"

with callysto.Cell("python"):
    with open(token_file, "r") as f:
        my_token = f.read()

with callysto.Cell("python"):
    client = PicSureClient.Client()
    connection = client.connect(PICSURE_network_URL, my_token)
    adapter = PicSureHpdsLib.Adapter(connection)
    resource = adapter.useResource(resource_id)

with callysto.Cell("python"):
    harmonized_dic = resource.dictionary().find("Harmonized").DataFrame()

with callysto.Cell("python"):
    pd.set_option("display.max.rows", 50)

with callysto.Cell("python"):
    #%%capture
    multiIndexdic = get_multiIndex_variablesDict(harmonized_dic)
    multiIndexdic_sub = multiIndexdic.loc[~ multiIndexdic["simplified_name"].str.contains("(^[Aa]ge)|(SUBJECT_ID)", regex=True), :]

with callysto.Cell("python"):
    multiIndexdic_sub.shape

with callysto.Cell("python"):
    multiIndexdic_sub

with callysto.Cell("python"):
    mask_demo = multiIndexdic_sub.index.get_level_values(1) == '01 - Demographics'
    variablesDict = multiIndexdic_sub.loc[mask_demo, :]

with callysto.Cell("python"):
    selected_vars = variablesDict.loc[:, "name"].tolist()
    # selected_vars.append("\\_Consents\\Short Study Accession with Consent Code\\")

with callysto.Cell("python"):
    pprint(selected_vars[:5])

with callysto.Cell("python"):
    query = resource.query()
    query.select().add(selected_vars)
    facts = query.getResultsDataFrame(low_memory=False)

with callysto.Cell("python"):
    facts = facts.set_index("Patient ID") \
        .dropna(axis=0, how="all") \
        .drop(["\\_Consents\\Short Study Accession with Consent Code\\",
               "\\_Study Accession with Patient ID\\"], axis=1)
    facts.columns = variablesDict.set_index("name").loc[selected_vars, "simplified_name"]

with callysto.Cell("python"):
    sex_varname = "Subject sex  as recorded by the study."
    study_varname = "A distinct subgroup within a study  generally indicating subjects who share similar characteristics due to study design. Subjects may belong to only one subcohort."
    race_varname = "Harmonized race category of participant."

with callysto.Cell("python"):
    import matplotlib.patches as mpatches
    from matplotlib import cm
    from matplotlib.offsetbox import (TextArea, DrawingArea, OffsetImage,
                                      AnnotationBbox)

with callysto.Cell("python"):
    plt.rcParams["figure.figsize"] = (14, 8)
    font = {'weight': 'bold',
            'size': 12}
    plt.rc('font', **font)

with callysto.Cell("python"):
    facts.head()

with callysto.Cell("python"):
    subset_facts = facts.loc[pd.notnull(facts[sex_varname]), :]
    ratio_df = subset_facts.groupby(study_varname)[sex_varname] \
        .apply(lambda x: pd.value_counts(x) / (np.sum(pd.notnull(x)))) \
        .unstack(1)
    annotation_x_position = ratio_df.apply(np.max, axis=1)
    number_subjects = subset_facts.groupby(study_varname)[sex_varname].apply(lambda x: x.notnull().sum())
    annotation_gen = list(zip(number_subjects, annotation_x_position))

    fig = ratio_df.plot.barh(title="Subjects sex-ratio across studies", figsize=(10, 12))
    fig.legend(bbox_to_anchor=(1, 0.5))
    fig.set_xlim(0, 1.15)
    fig.set_ylabel(None)

    for n, p in enumerate(fig.patches[:27]):
        nb_subject, x_position = annotation_gen[n]
        fig.annotate(nb_subject, (x_position + 0.03, p.get_y() + 0.1), bbox=dict(facecolor='none',
                                                                                 edgecolor='black',
                                                                                 boxstyle='round'))

    handles, labels = fig.get_legend_handles_labels()
    red_patch = mpatches.Patch(label='Study nb subjects', edgecolor="black", facecolor="white")
    handles.append(red_patch)
    fig.legend(handles=handles)
