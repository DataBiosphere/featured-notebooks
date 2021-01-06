# Notebook author: Beth Sheets
# Herzog version: Ash O'Farrell

# Notebook title: 3-GWAS-genomic-data-preparation



with herzog.Cell("markdown"):
	"""
	# GWAS Genomic Analysis
	*version: 2.0.0*



	## Set runtime values
    If you are opening this notebook for the first time, and you did not edit your runtime settings before starting it, you will now need to change your settings. Click on the gear icon in the upper right to edit your Notebook Runtime. Set the values as specified below:
    <table style="float:left">
        <thead>
            <tr><th>Option</th><th>Value</th></tr>
        </thead>
        <tbody>
             <tr><td>Application Configuration</td><td>Hail: (Python 3.7.9, Spark 2.4.5, hail 0.2.61)</td></tr>
                          <tr><td>CPUs</td><td>8</td></tr>
                          <tr><td>Memory (GB)</td><td>30</td></tr>
                          <tr><td>Disk size (GB)</td><td>100</td></tr>
                          <tr><td>Startup script</td><td>(leave blank)</td></tr>
                          <tr><td>Compute Type</td><td>Spark cluster</td></tr>
             <tr><td>Workers</td><td>4</td></tr>
                          <tr><td>Preemptible</td><td>0</td></tr>
                          <tr><td>Workers CPUs</td><td>4</td></tr>
                          <tr><td>Workers Memory (GB)</td><td>15</td></tr>
             <tr><td>Workers Disk size (GB)</td><td>50</td></tr>
        </tbody>
    </table>
	"""