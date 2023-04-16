import logging

from flask import Flask

from nexinfosys import initialize_configuration, cfg_file_env_var
from nexinfosys.model_services import get_case_study_registry_objects

nis_api_base = "/nis_api"  # Base for all RESTful calls
nis_client_base = "/nis_client"  # Base for the Angular2 client
nis_external_client_base = "/nis_external"  # Base for the Angular2 client called from outside

app = Flask(__name__)
app.debug = True
UPLOAD_FOLDER = '/tmp/'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Initialize configuration
initialize_configuration()
app.config.from_envvar(cfg_file_env_var)


def get_results_in_session(isess: "InteractiveSession"):
    """
    Obtain list of ALL possible outputs (not only datasets) IN the current state.
    :param isess:
    :return:
    """
    dataset_formats = ["CSV", "XLSX", "SDMX.json"] #, "XLSXwithPivotTable", "NISembedded", "NISdetached"]
    graph_formats = ["VisJS", "GML"] #, "GraphML"]
    ontology_formats = ["OWL"]
    geo_formats = ["GeoJSON"]
    # A reproducible session must be open, signal about it if not
    if isess.reproducible_session_opened():
        if isess.state:
            glb_idx, p_sets, hh, datasets, mappings = get_case_study_registry_objects(isess.state)
            r = {"datasets":
                     [dict(name=k,
                           type="dataset",
                           description=F"{datasets[k].description} [{datasets[k].data.shape[0]} rows, {datasets[k].data.size} cells, "
                           F"{datasets[k].data.memory_usage(True).sum()} bytes]",
                           # nelements=datasets[k].data.size,
                           # nrows=datasets[k].data.shape[0],
                           # size=datasets[k].data.memory_usage(True).sum(),
                           formats=[dict(format=f,
                                         url=nis_api_base + F"/isession/rsession/state_query/datasets/{k}.{f.lower()}")
                                    for f in dataset_formats],
                           ) for k in datasets
                      ] +
                     [dict(name="interfaces_graph",
                           type="graph",
                           description="Graph of Interfaces, Quantities; Scales and Exchanges",
                           formats=[dict(format=f,
                                         url=nis_api_base + F"/isession/rsession/state_query/flow_graph.{f.lower()}")
                                    for f in graph_formats]),
                      dict(name="processors_graph",
                           type="graph",
                           description="Processors and exchanges graph",
                           formats=[dict(format=f,
                                         url=nis_api_base + F"/isession/rsession/state_query/processors_graph.{f.lower()}")
                                    for f in graph_formats]),
                      ] +
                     [dict(name="Sankey_Graph",
                           type="Graph",
                           description="Dictionary of Sankey Graph for every scenario for implementation in JupyterLab using plotly",
                           formats=[
                               dict(format=f, url=nis_api_base + F"/isession/rsession/state_query/sankey_graph.{f.lower()}")
                               for f in ["JSON"]]),
                      ] +
                     [dict(name="processors_geolayer",
                           type="geolayer",
                           description="Processors",
                           formats=[
                               dict(format=f, url=nis_api_base + F"/isession/rsession/state_query/geolayer.{f.lower()}")
                               for f in geo_formats]),
                      ] +
                     [dict(name="model",
                           type="model",
                           description="Model",
                           formats=[
                               dict(format=f, url=nis_api_base + F"/isession/rsession/state_query/model.{f.lower()}")
                               for f in ["JSON", "XLSX", "XML"]]),
                      ] +
                     [dict(name="ontology",
                           type="ontology",
                           description="OWL ontology",
                           formats=[
                               dict(format=f, url=nis_api_base + F"/isession/rsession/state_query/ontology.{f.lower()}")
                               for f in ontology_formats]),
                      ] +
                     [dict(name="Python script",
                           type="script",
                           description="Python script",
                           formats=[dict(format=f,
                                         url=nis_api_base + F"/isession/rsession/state_query/python_script.{f.lower()}")
                                    for f in ["Python", "JupyterNotebook"]]),
                      dict(name="R script",
                           type="script",
                           description="R script",
                           formats=[
                               dict(format=f, url=nis_api_base + F"/isession/rsession/state_query/r_script.{f.lower()}")
                               for f in ["R", "JupyterNotebook"]]),
                      ] +
                     [dict(name="Commands reference",
                           type="document",
                           description="Reference of all commands and their fields",
                           formats=[
                               dict(format=f,
                                    url=nis_api_base + F"/isession/rsession/state_query/commands_reference_document.{f}")
                               for f in ["html"]]),
                      ]
                 }

    return r