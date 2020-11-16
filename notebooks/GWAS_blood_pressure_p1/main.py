#!/usr/bin/env python
# coding: utf-8

# notebook title: 1-unarchive-vcf-tar-file-to-workspace.py

import herzog

with herzog.Cell("markdown"):
	'''
	# Set up your notebook
	----

	## Set runtime values
	If you are opening this notebook for the first time, and you did not edit your runtime settings before starting it, you will now need to change your settings. Click on the gear icon in the upper right to edit your Notebook Runtime. Set the values as specified below: 

	| Option | Value |
	| ------ | ------ |
	| Environment | New Default (Python 3.7.7) |
	| Profile | Default (Moderate Compute Power) |
	| Master CPUs | 4 |
	| Master Memory | 15 GB |
	| Master Disk size | 50 |
	| Startup Script | leave blank |

	Click the "Replace" button when you are done, and Terra will begin to create a new runtime with your settings. When it is finished, it will pop up asking you to apply the new settings.  

	## Check kernel type  

	A kernel is a _computational engine_ that executes the code in the notebook. You can think of it as defining the programming language. For this notebook, we'll use a `Python 3` kernel. In the upper right corner of the notebook, just under the Notebook Runtime, it should say `Python 3`. If it doesn't, you can switch it by navigating to the Kernel menu and selecting `Change kernel`.

	Gen3 uploaded tar compressed bundles, as they are provided by dbGAP, into cloud buckets owned by BioData Catalyst. In order to make these tar files actionable and ready for use in an analysis, users will need to run this notebook to unarchive the tar files. Please understand that this step may be time consuming since TOPMed multi-sample VCF files are several hundred gigabytes in size. 

	If you do not wish to use this option, you can use your credentials to download these files directly from dbGAP, extract them locally, and upload them to your Terra workspace. 

	Learn more about [how not to lose data in notebooks](https://support.terra.bio/hc/en-us/articles/360027300571-Notebooks-101-How-not-to-lose-data-output-files). For tasks that take hours, like the ones below, we suggest keeping your computer connected to the internet so that you do not lose connection with the notebook VM and lose updates to the processes you started.

	## Install a package to access DRS URLs to genomic files
	'''

with herzog.Cell("python"):
	# %pip3 install --upgrade gs-chunked-io
	# %pip install --upgrade pip\n! pip install terra-notebook-utils')
	pass

with herzog.Cell("markdown"):
	'''
	Restart kernel after every pip install. You can do this using the toolbar above. 
	'''

with herzog.Cell("python"):
	from terra_notebook_utils import table, drs

with herzog.Cell("markdown"):
	'''
	The time packages and code below help you monitor how long tasks take.
	'''

with herzog.Cell("python"):
	import time
	from datetime import timedelta
	start_notebook_time = time.time()


with herzog.Cell("markdown"):
	'''
	## Load multi-sample VCF via DRS URL to your workspace

	The TOPMed genomic data that you import from Gen3 is controlled access and imported into Terra as a Data Repository Service (DRS) URL to the controlled access bucket that holds the file. The code below allows you to share your credentials and download the file to your workspace so that you can interact with the file in a notebook.

	See which files are available in the Reference File data table
	'''


with herzog.Cell("python"):
	from terra_notebook_utils import table
	data_table = "reference_file"
	table.print_column(data_table, "file_name")

with herzog.Cell("markdown"):
	'''
	Select which VCF you would like to use in your analysis from the printed list above.
	'''

with herzog.Cell("python"):
	# Get a drs url from our workspace data table
	file_name = "YOUR_FILE_NAME_.tar.gz"
	drs_url = table.fetch_drs_url(data_table, file_name)
	print(drs_url)

	# Copy object into our workspace bucket
	drs.copy(drs_url, file_name)

with herzog.Cell("python"):
	# Extract .tar.gz to our workspace bucket
	drs.extract_tar_gz(drs_url, file_name)

with herzog.Cell("python"):
	elapsed_notebook_time = time.time() - start_notebook_time
	print(timedelta(seconds=elapsed_notebook_time))

