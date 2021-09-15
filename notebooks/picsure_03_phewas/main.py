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
    print("NB: This Jupyter Notebook has been written using PIC-SURE API following versions:\n- PicSureHpdsLib: 1.1.0\n- PicSureClient: 1.1.0")
    print("The installed PIC-SURE API libraries versions:\n- PicSureHpdsLib: {0}\n- PicSureClient: {1}".format(
        PicSureHpdsLib.__version__, PicSureClient.__version__))

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
    copdgene_dic = resource.dictionary().find("Genetic Epidemiology of COPD (COPDGene)").DataFrame()
    consent_dic = resource.dictionary().find("Study Accession with Consent Code").DataFrame()
    # line needed bc dictionaries doesn't return the same column type for categorical and numerical variables
    plain_variablesDict = copdgene_dic.transpose().join(consent_dic.transpose(), how="outer").transpose()

with callysto.Cell("python"):
    plain_variablesDict.head()

with callysto.Cell("python"):
    resource.dictionary().help()

with callysto.Cell("python"):
    # Display the variables tree hierarchy from the variables name
    variablesDict = get_multiIndex_variablesDict(plain_variablesDict)
    variablesDict.iloc[10:20, :]

with callysto.Cell("python"):
    dic = resource.dictionary().find().DataFrame()

with callysto.Cell("python"):
    mask_pheno = variablesDict.index.get_level_values(1) == 'Subject Phenotype'
    mask_status = variablesDict.index.get_level_values(2) == 'Affection status'
    mask_drop = variablesDict["simplified_name"].isin(
        ["Dbgap_id", "De-identified site code", "A1AD: phenotype/genotype"])
    mask_vars = mask_pheno | mask_status
    variablesDict = variablesDict.loc[mask_vars & ~ mask_drop, :]

with callysto.Cell("python"):
    selected_vars = variablesDict.loc[:, "name"].tolist()

with callysto.Cell("python"):
    pprint(selected_vars[0:5])

with callysto.Cell("python"):
    phs = pd.Series(consent_dic["categoryValues"].values[0])
    mask_phs_copdgene = phs.str.startswith("phs000179").tolist()
    phs_copdgene = phs[mask_phs_copdgene]

with callysto.Cell("python"):
    #%%capture
    query = resource.query()
    query.filter().add(consent_dic.index[0], phs_copdgene)
    query.select().add(selected_vars)
    facts = query.getResultsDataFrame(low_memory=False)

with callysto.Cell("python"):
    facts.head()

with callysto.Cell("python"):
    status_var = variablesDict.loc[variablesDict.index.get_level_values(2) == 'Affection status', "name"]
    facts = facts.dropna(subset=status_var) \
        .set_index("Patient ID")
    mask_to_drop = variablesDict["simplified_name"] \
        .isin(["Dbgap_id", "De-identified site code", "A1AD: phenotype/genotype"])
    variablesDict = variablesDict.loc[~mask_to_drop, :]
    var_to_keep = variablesDict.loc[:, "name"]
    facts = facts.loc[:, var_to_keep]

with callysto.Cell("python"):
    print("{0} rows, {1} columns".format(*facts.shape))

with callysto.Cell("python"):
    facts.tail(5)

with callysto.Cell("python"):
    mask_categories = variablesDict.loc[:, "categorical"] == True
    categorical_names = variablesDict.loc[mask_categories, "name"].tolist()
    continuous_names = variablesDict.loc[~mask_categories, "name"].tolist()

with callysto.Cell("python"):
    dependent_var_name = variablesDict.loc[variablesDict["simplified_name"] == "Affection status", "name"].values[0]
    categorical_names.remove(dependent_var_name)

with callysto.Cell("python"):
    mask_dependent_var_name = facts[dependent_var_name].isin(["Case", "Control"])
    facts = facts.loc[mask_dependent_var_name, :] \
        .astype({dependent_var_name: "category"})
    print("Control: {0} individuals\nCase: {1} individuals".format(*facts[dependent_var_name].value_counts().tolist()))

with callysto.Cell("python"):
    from statsmodels.discrete.discrete_model import Logit

with callysto.Cell("python"):
    independent_names = variablesDict["name"].tolist()
    independent_names.remove(dependent_var_name)
    dependent_var = facts[dependent_var_name].astype("category").cat.codes
    dic_pvalues = {}
    simple_index_variablesDict = variablesDict.set_index("name", drop=True)

with callysto.Cell("python"):
    from scipy.linalg import LinAlgError
    from statsmodels.tools.sm_exceptions import PerfectSeparationError
    from tqdm import tqdm

with callysto.Cell("python"):
    for independent_name in tqdm(independent_names, position=0, leave=True):
        matrix = facts.loc[:, [dependent_var_name, independent_name]] \
            .dropna(how="any")
        if matrix.shape[0] == 0:
            dic_pvalues[independent_name] = np.NaN
            continue
        if simple_index_variablesDict.loc[independent_name, "categorical"]:
            matrix = pd.get_dummies(matrix,
                                    columns=[independent_name],
                                    drop_first=False) \
                         .iloc[:, 0:-1]
        dependent_var = matrix[dependent_var_name].cat.codes
        independent_var = matrix.drop(dependent_var_name, axis=1) \
            .assign(intercept=1)
        model = Logit(dependent_var, independent_var)
        try:
            results = model.fit(disp=0)
            dic_pvalues[independent_name] = results.llr_pvalue
        except (LinAlgError, PerfectSeparationError) as e:
            dic_pvalues[independent_name] = np.NaN

with callysto.Cell("python"):
    pd.Series([v for v in dic_pvalues.values()]).plot.hist(bins=30)
    plt.suptitle("Distribution of individual p-values",
                 weight="bold",
                 fontsize=15)

with callysto.Cell("python"):
    #%%capture
    # Merging pvalues from different tests
    df_pvalues = pd.DataFrame.from_dict(dic_pvalues, orient="index", columns=["pvalues"]) \
        .rename_axis("name") \
        .reset_index(drop=False)

    # Adding pvalues results as a new column to variablesDict
    variablesDict = joining_variablesDict_onCol(variablesDict,
                                                df_pvalues,
                                                left_col="name",
                                                right_col="name")

    adjusted_alpha = 0.05 / len(variablesDict["pvalues"])
    variablesDict["p_adj"] = variablesDict["pvalues"] / len(variablesDict["pvalues"])
    variablesDict['log_p'] = -np.log10(variablesDict['pvalues'])
    variablesDict = variablesDict.sort_index()
    variablesDict["group"] = variablesDict.reset_index(level=2)["level_2"].values

with callysto.Cell("python"):
    print("Bonferonni adjusted significance threshold: {0:.2E}".format(adjusted_alpha))

with callysto.Cell("python"):
    mask = variablesDict["pvalues"].isna()
    df_results = variablesDict.loc[~mask, :].copy().replace([np.inf, -np.inf], np.nan)
    df_results = df_results.loc[~df_results["log_p"].isna().values, :]

    #### Specific adjustment to make this specific plot looks nicer
    ####### to adapt when changing data or dependent variable
    df_results = df_results.replace({"TLC": "Spirometry",
                                     "New Gold Classification": "Quantitative Analysis",
                                     "Other": "Demographics"})
    group_order = {'6MinWalk': 0,
                   'CT Acquisition Parameters': 1,
                   'CT Assessment Scoresheet': 2,
                   'Demographics and Physical Characteristics': 3,
                   'Eligibility Form': 10,
                   'Longitudinal Analysis': 5,
                   'Medical History': 4,
                   'Medication History': 13,
                   'Quantitative Analysis': 9,
                   'Respiratory Disease': 6,
                   'SF-36 Health Survey': 11,
                   'Sociodemography and Administration': 12,
                   'Spirometry': 7,
                   'VIDA': 15}
    df_results["group_order"] = df_results["group"].replace(group_order)
    df_results = df_results.sort_values("group_order", ascending=True)
    df_results["simplified_name"] = df_results["simplified_name"].str.replace("[0-9]+[A-z]*", "").to_frame()
    ###

    fig = plt.figure()
    ax = fig.add_subplot(111)
    colors = plt.get_cmap('Set1')
    x_labels = []
    x_labels_pos = []

    y_lims = (0, df_results["log_p"].max(skipna=True) + 50)
    threshold_top_values = df_results["log_p"].sort_values(ascending=False)[0:6][-1]

    df_results["ind"] = np.arange(1, len(df_results) + 1)
    df_grouped = df_results.groupby(('group'))
    for num, (name, group) in enumerate(df_grouped):
        group.plot(kind='scatter', x='ind', y='log_p', color=colors.colors[num % len(colors.colors)], ax=ax, s=20)
        x_labels.append(name)
        x_labels_pos.append(
            (group['ind'].iloc[-1] - (group['ind'].iloc[-1] - group['ind'].iloc[0]) / 2))  # Set label in the middle
        for n, row in group.iterrows():
            if row["log_p"] > threshold_top_values:
                ax.text(row['ind'] + 3, row["log_p"] + 0.05, row["simplified_name"], rotation=0, alpha=1, size=8,
                        color="black")

    ax.set_xticks(x_labels_pos)
    ax.set_xticklabels(x_labels)
    ax.set_xlim([0, len(df_results) + 1])
    ax.set_ylim(y_lims)
    ax.set_ylabel('-log(p-values)', style="italic")
    ax.set_xlabel('Phenotypes', fontsize=15)
    ax.axhline(y=-np.log10(adjusted_alpha), linestyle=":", color="black", label="Adjusted Threshold")
    plt.xticks(fontsize=9, rotation=90)
    plt.yticks(fontsize=8)
    plt.title("Statistical Association Between COPD Status and Phenotypes",
              loc="center",
              style="oblique",
              fontsize=20,
              y=1)
    xticks = ax.xaxis.get_major_ticks()
    xticks[0].set_visible(False)
    handles, labels = ax.get_legend_handles_labels()
    plt.legend(handles=handles, labels=labels, loc="upper left")
    plt.show()
