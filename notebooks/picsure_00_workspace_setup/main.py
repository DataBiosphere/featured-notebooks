import os
import callysto

# Mock the notebook environment
os.environ['WORKSPACE_NAME'] = "terra-notebook-utils-tests"
os.environ['WORKSPACE_BUCKET'] = "gs://fc-9169fcd1-92ce-4d60-9d2d-d19fd326ff10"
os.environ['GOOGLE_PROJECT'] = "firecloud-cgl"


with callysto.Cell("python"):
    my_token = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJMT05HX1RFUk1fVE9LRU58ZmVuY2V8NzkzIiwibmFtZSI6IlNBUkFWQU5BTVVUSFUwMSIsImlzcyI6ImVkdS5oYXJ2YXJkLmhtcy5kYm1pLnBzYW1hIiwiZXhwIjoxNTkzNzE2OTEzLCJpYXQiOjE1OTExMjQ5MTMsImVtYWlsIjoiY2FydGlrX3NhcmF2YW5hbXV0aHVAaG1zLmhhcnZhcmQuZWR1IiwianRpIjoid2hhdGV2ZXIifQ.EkV_eRqu6uAtvSIN1bb3qC1QPvhN-22OMDmbvBln6CY"


with callysto.Cell("python"):
    with open("token.txt", "w+") as f:
        f.write(my_token)


with callysto.Cell("python"):
    #!wget https://raw.githubusercontent.com/hms-dbmi/Access-to-Data-using-PIC-SURE-API/master/NHLBI_BioData_Catalyst/python/requirements.txt
    pass


with callysto.Cell("python"):
    #!mkdir python_lib
    #!cd python_lib && wget https://raw.githubusercontent.com/hms-dbmi/Access-to-Data-using-PIC-SURE-API/master/NHLBI_BioData_Catalyst/python/python_lib/utils.py
    pass


with callysto.Cell("python"):
    import sys
    #!{sys.executable} -m pip install -r requirements.txt


with callysto.Cell("python"):
    # !{sys.executable} -m pip install --upgrade --force-reinstall git+https://github.com/hms-dbmi/pic-sure-python-adapter-hpds.git
    # !{sys.executable} -m pip install --upgrade --force-reinstall git+https://github.com/hms-dbmi/pic-sure-python-client.git
    pass


with callysto.Cell("python"):
    from IPython.display import display_html

    def restartkernel():
        display_html("<script>Jupyter.notebook.kernel.restart()</script>", raw=True)


with callysto.Cell("python"):
    restartkernel()
