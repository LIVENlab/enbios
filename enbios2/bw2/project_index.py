"""
A shortcut of initializing the project in brightway2. The motivation is to have a local brightway project index file.
Now if someone uses this module to set the current index, that is independent of the machine that is used, as long as the index file exists.
This helps with the problem of different bw project names on different machines
"""
from typing import Optional

import bw2data
import yaml
from bw2data.backends import SQLiteBackend

from enbios2.base.databases import init_databases
from enbios2.base.db_models import BWProjectIndex, EcoinventDataset
from enbios2.ecoinvent.ecoinvent_index import get_ecoinvent_dataset_index
from enbios2.generic.enbios2_logging import get_logger

projects = bw2data.projects

logger = get_logger(__file__)

def _read_bw_index_file() -> list[BWProjectIndex]:
    return list(BWProjectIndex.select())


def print_bw_index():
    """
    Prints the index file
    :return:
    """
    print(_read_bw_index_file())


def get_existing(project_name: str, database_name: str) -> Optional[BWProjectIndex]:
    """
    Get an existing index based on the project and database name
    :param project_name:
    :param database_name:
    :return:
    """
    existing = list(BWProjectIndex.select(BWProjectIndex, EcoinventDataset).join(EcoinventDataset).where(
        (BWProjectIndex.project_name == project_name) &
        (BWProjectIndex.database_name == database_name)))
    if existing:
        return existing[0]


def set_bw_index(project_name: str, database_name: str, ecoinvent_dataset: EcoinventDataset) -> BWProjectIndex:
    """
    set a new index
    :param project_name:
    :param database_name:

    :return:
    """
    existing = get_existing(project_name, database_name)
    if existing:
        logger.info(
            f"Index for {project_name},{database_name} exists already with Ecoinvent dataset: {existing.ecoinvent_dataset.identity}")
        return existing

    bw_project_index = BWProjectIndex.create(project_name=project_name, database_name=database_name,
                                             ecoinvent_dataset=ecoinvent_dataset)
    return bw_project_index


def project_index_creation_helper():
    """
    A helper to get an overview of the projects and databases (should go somewhere else)
    :return:
    """
    projects_overview = {}
    for project in projects:
        bw2data.projects.set_current(project.name)
        projects_overview[project.name] = {k: {key: value
                                               for key, value in v.items() if key in ["format", "number"]}
                                           for k, v in bw2data.databases.data.items()}
    print(yaml.dump(projects_overview))


def set_bw_current_project(system_model: str, version: str) -> Optional[BWProjectIndex]:
    """

    :param system_model:
    :param version:
    :return: the BWProjectIndex, if the project was set
    """
    bwp = BWProjectIndex.select().join(
        EcoinventDataset,
        on=(BWProjectIndex.ecoinvent_dataset == EcoinventDataset.id)).where(
        (EcoinventDataset.system_model == system_model) &
        (EcoinventDataset.version == version) &
        (EcoinventDataset.type == "default") &
        (EcoinventDataset.xlsx == False))
    if bwp:
        bwp = bwp[0]
        bw2data.projects.set_current(bwp.project_name)
        logger.info(f"Set current project to '{bwp.project_name}'. Ecoinvent database is '{bwp.database_name}'")
        return bwp
    else:
        logger.error(f"No brightway project found for ecoinvent dataset: {system_model}, {version}")
        return None


def get_bw_database(system_model: str, version: str) -> Optional[SQLiteBackend]:
    bwp = set_bw_current_project(system_model, version)
    if bwp:
        return bw2data.Database(bwp.database_name)


if __name__ == "__main__":
    init_databases()
    project_index_creation_helper()
    # print_bw_index()
    candidates = list(get_ecoinvent_dataset_index(version="3.9.1", system_model="cutoff", xlsx=False))
    # print(list(candidates))
    set_bw_index("ecoi_dbs", "cutoff391", candidates[0])


#
# def add_bw_project_index(project_name: str):
#     assert project_name in bw2data.projects
