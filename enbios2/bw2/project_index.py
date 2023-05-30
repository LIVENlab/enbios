"""
A shortcut of initializing the project in brightway2. The motivation is to have a local brightway project index file.
Now if someone uses this module to set the current index, that is independet of the machine that is used, as long as the index file exists.
This helps with the problem of different bw project names on different machines
"""
import json
from dataclasses import dataclass, asdict
from enum import Enum
from typing import Literal, Optional

import bw2data

from enbios2.const import BW_PROJECT_INDEX_FILE
from enbios2.generic.files import ReadDataPath, ReadPath

projects = bw2data.projects

BW_Databases = Literal["ecovinvent391cutoff", "ecovinvent391consequential", "ecovinvent391apos"]


class BWIndex(Enum):
    ecovinvent391cutoff = "ecovinvent391cutoff"
    ecovinvent391consequential = "ecovinvent391consequential"
    ecovinvent391apos = "ecovinvent391apos"


@dataclass
class BWProjectIndex:
    ecovinvent391cutoff: Optional[str] = None
    ecovinvent391consequential: Optional[str] = None
    ecovinvent391apos: Optional[str] = None


def _read_bw_index_file() -> BWProjectIndex:
    return BWProjectIndex(**ReadPath(BW_PROJECT_INDEX_FILE).read_data())


def print_current_index():
    """
    Prints the index file
    :return:
    """
    print(_read_bw_index_file)


def set_bw_current_project(bw_project_index: BWIndex):
    """
    Sets the current bw project
    :param bw_project_index:
    :return:
    """
    project_index = getattr(_read_bw_index_file(), bw_project_index.value)
    if not project_index:
        raise ValueError(f"Project index '{bw_project_index.value}' not found")
    bw2data.projects.set_current(project_index)


def set_bw_index(bw_project_index: BWIndex, project_name: str):
    """
    set a new index
    :param bw_project_index:
    :param project_name:
    :return:
    """
    project_index = _read_bw_index_file()
    setattr(project_index, bw_project_index.value, project_name)
    ReadDataPath(BW_PROJECT_INDEX_FILE).write_text(json.dumps(asdict(project_index), indent=2))


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
    print(json.dumps(projects_overview, indent=2))


if __name__ == "__main__":
    # project_index_creation_helper()
    # print_current_index()
    set_bw_index(BWIndex.ecovinvent391cutoff, "uab_bw_ei39")
    set_bw_current_project(BWIndex.ecovinvent391cutoff)
