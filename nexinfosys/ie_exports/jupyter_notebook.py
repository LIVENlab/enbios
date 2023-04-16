from io import StringIO

from nexinfosys.model_services import State, get_case_study_registry_objects
from nbformat import write, v4
import nbformat


def generate_jupyter_notebook_python(state: State):
    glb_idx, p_sets, hh, datasets, mappings = get_case_study_registry_objects(state)

    fname = "<get_file_name>"
    nb = v4.new_notebook()
    nb['metadata'] = {
        "kernelspec": {
            "display_name": "Python 3",
            "language": "python",
            "name": "python3"
        },
        "language_info": {
            "codemirror_mode": {
                "name": "ipython",
                "version": 3
            },
            "file_extension": ".py",
            "mimetype": "text/x-python",
            "name": "python",
            "nbconvert_exporter": "python",
            "pygments_lexer": "ipython3",
            "version": "3.7.1"
        }
    }

    nb['cells'] = [
        v4.new_markdown_cell("""
# Jupyter Notebook generated automatically by NIS-frontend
        """),
        v4.new_code_cell("""
from nexinfosys import NISClient, display_visjs_jupyterlab
import io
import pandas as pd
import networkx as nx
        """, ),
        v4.new_code_cell(f"""
fname = "{fname}.xlsx"
            """),
        v4.new_code_cell("""
c = NISClient("https://one.nis.magic-nexus.eu/nis_api")
#c = NISClient("http://localhost:5000/nis_api")

# Login, open session, load a workbook (which is in Nextcloud), submit (execute!)
c.login("test_user")
c.open_session()
#print("Session opened")
n = c.load_workbook(fname)
#print("N worksheets: "+str(n))
r = c.submit()
#print("Returned from submit")
# Check if submission was successful (it should be with the provided workbook), then query 
# available datasets, and get one of them, converting it into a pd.DataFrame
any_error = False
if len(r) > 0:
    for i in r:
        if i["type"] == 3:
            any_error = True
            print(str(i))

if not any_error:
    # Obtain available datasets
    r = c.query_available_datasets()
    if len(r) > 0:
        results = {}
        for ds in r:
            results[ds["name"]] = {d["format"].lower(): d["url"] for d in ds["formats"]}
            #print(str(ds))
        #r = c.download_results([(results["FG"]["visjs"])])
        #visjs_data = r[0].decode("utf-8")
        #unique_name = None
        r = c.download_results([(results["PG"]["visjs"])])
        visjs_data2 = r[0].decode("utf-8")
        un2 = None

        #unique_name = display_visjs_jupyterlab(visjs_data, 800, unique_name)
        un2 = display_visjs_jupyterlab(visjs_data2, 1200, un2)

        """)
    ]

    return nbformat.writes(nb)


def generate_jupyter_notebook_r(state: State):
    glb_idx, p_sets, hh, datasets, mappings = get_case_study_registry_objects(state)

    nb = v4.new_notebook()
    nb['metadata'] = {
        "kernelspec": {
            "display_name": "R",
            "language": "R",
            "name": "ir"
        },
        "language_info": {
            "codemirror_mode": "r",
            "file_extension": ".r",
            "mimetype": "text/x-r-source",
            "name": "R",
            "pygments_lexer": "r",
            "version": "3.5.1"
        }
    }

    nb['cells'] = [
        v4.new_markdown_cell("""
# Jupyter Notebook generated automatically by NIS-frontend
        """),
        v4.new_code_cell("""
.libPaths( c("~/R-packages", "/opt/conda/lib/R/library") )
#install.packages("reticulate")
library("reticulate")
nexinfosys <- import("nexinfosys")
c <- nexinfosys$NISClient("https://one.nis.magic-nexus.eu/nis_api")        
        """),
        v4.new_code_cell("""
fname <- "https://nextcloud.data.magic-nexus.eu/remote.php/webdav/NIS_beta/CS_format_examples/08_caso_energia_eu_new_commands.xlsx"
        """),
        v4.new_code_cell("""
c$login("test_user")
print("Logged in")
c$open_session()
print("Session opened")
n <- c$load_workbook(fname, "NIS_agent", "NIS_agent@1")
print(paste("N worksheets: ",n))
r <- c$submit()
print("Returned from submit")
        """),
        v4.new_code_cell("""
# Obtain a list of available datasets
r <- c$query_available_datasets()
        """),
        v4.new_code_cell("""
# Obtain a specific dataset
ds <- c$query_datasets(c(tuple("ds1", "csv", "dataframe")))
        """),
        v4.new_code_cell("""
# Obtain an R data.frame
df <- py_to_r(ds[[1]][[3]])
        """),
        v4.new_code_cell("""
c$close_session()
c$logout()
        """)
    ]

    return nbformat.writes(nb)