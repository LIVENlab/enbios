from enbios2.bw2.bw_autoimporter import setup_bw_project
from enbios2.const import BASE_DATA_PATH
import bw2data as bd

from enbios2.models.bw_project_models import BWProject, BWProjectDatabase


def test_basic_bw():
    setup_bw_project(project=BWProject(project_name="py_test"))


def test_load_ecoinvent():
    ecoinvent_db_path = BASE_DATA_PATH / "ecoinvent/ecoinvent 3.9.1_cutoff_ecoSpold02/datasets"
    project_data = BWProject(project_name="py_test", databases=[
        BWProjectDatabase(name="eco-invent", source=ecoinvent_db_path, format="Ecospold2")
    ])
    setup_bw_project(project=project_data)

    assert bd.projects.current == "py_test"
    for db in project_data.databases:
        assert db.name in bd.databases


def clean_project():
    bd.projects.delete_project("py_test", delete_dir=True)
