#!/usr/bin/env python
# coding: utf-8

# Notebook author: Beth Sheets
# Herzog version: Ash O'Farrell

import herzog

with herzog.Cell("markdown"):
	"""
	# Notebook Overview
	
	This notebook consolidates the phentotypic data in tables imported to Terra from Gen3 into one consolidated table of metadata with familiar subject IDs for downstream analysis. It uses functions defined in the terra-util notebook, nd can be adapted to consolidate different metadata. I

	# Set up your notebook
	----
	
	## Set runtime values
	If you are opening this notebook for the first time, and you did not edit your runtime settings before starting it, you will now need to change your settings. Click on the gear icon in the upper right to edit your Notebook Runtime. Set the values as specified below: 
	
	<table style="float:left">
	    <thead>
	        <tr><th>Option</th><th>Value</th></tr>
	    </thead>
	    <tbody>
	         <tr><td>ENVIRONMENT</td><td>New Default (Python 3.7.7)</td></tr>
	         <tr><td>COMPUTE POWER Profile</td><td>Default (Moderate Compute Power)</td></tr>   
	         <tr><td>CPUs</td><td>4</td></tr>     
	         <tr><td>Memory</td><td>15 GB</td></tr>    
	         <tr><td>Disk size</td><td>50</td></tr>   
	         <tr><td>Startup Script</td><td>leave blank</td></tr>  
	    </tbody>
	</table

	Click the "Replace" button when you are done, and Terra will begin to create a new runtime with your settings. When it is finished, it will pop up asking you to apply the new settings.  
	
	## Check kernel type  
	
	A kernel is a _computational engine_ that executes the code in the notebook. You can think of it as defining the programming language. For this notebook, we'll use a `Python 3` kernel. In the upper right corner of the notebook, just under the Notebook Runtime, it should say `Python 3`. If it doesn't, you can switch it by navigating to the Kernel menu and selecting `Change kernel`.

	## Install packages
	"""

with herzog.Cell("python"):
	get_ipython().run_line_magic('pip', 'install tenacit	y')

with herzog.Cell("markdown"):
	"""
	## Import all the packages this notebook will use
	"""

with herzog.Cell("python"):
	get_ipython().run_cell_magic('capture', '', "from firecloud import fiss\nimport tenacity\nimport pandas as pd \npd.set_option('display.max_row', 10)\nimport os \nimport io\nimport numpy as np\nimport seaborn as sn	s")

with herzog.Cell("markdown"):
	## Define filepaths and environmental variables

with herzog.Cell("python"):
	PROJECT = os.environ['GOOGLE_PROJECT']
	PROJECT

with herzog.Cell("python"):
	WORKSPACE = os.path.basename(os.path.dirname(os.getcwd()))
	WORKSPACE

with herzog.Cell("python"):
	bucket = os.environ['WORKSPACE_BUCKET']
	bucket = bucket + '/'
	bucket

with herzog.Cell("markdown"):
	"""
	Consolidate the Gen3 clinical entities into a single Terra data model using functions in the "terra_data_util" python notebook
	"""

with herzog.Cell("python"):
	# Run the companion notebook. Note: it must be in the same workspace you're currently working in.
	get_ipython().run_line_magic('run', 'terra_data_table_util.ipynb')

with herzog.Cell("python"):
	# Take a look at all the entities (tables) in the workspace
	ent_types = fiss.fapi.list_entity_types(PROJECT, WORKSPACE).json()
	for t in ent_types.keys():
    	print (t, "count:", ent_types[t]['count'])

with herzog.Cell("python"):
	# Set the name for your consolidated table
	consolidated_table_name = "consolidated_metadata"

with herzog.Cell("markdown"):
	"""
	The consolidate_gen3_pheno_tables function:
	* Joins all clinical data tables into a single consolidated_metadata table in the Terra data section
	* This join forces all entities (individuals) to be present in every clinical table, so some individuals may be removed. Consider how this affects your dataset.
	* Renames attribute fields to have a prefix of the original entity type (for example: "demographic_annotated_sex", where demographic is the original entity type, annotated_sex is the attribute field)
	"""

with herzog.Cell("python"):
	# Consolidate the phenotypic data using the function defined in the terra-util notebook
	consolidate_gen3_pheno_tables(PROJECT, WORKSPACE, consolidated_table_name)


with herzog.Cell("python"):




