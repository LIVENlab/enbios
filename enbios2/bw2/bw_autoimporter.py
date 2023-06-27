"""
Mostly deprecated, because we use the indexers now.
"""

import importlib
import inspect
from pathlib import Path
from typing import Any, TypeVar, Union

import bw2data as bd
import bw2io as bi

from enbios2.generic.enbios2_logging import get_logger
from enbios2.models.bw_project_models import BWProject, BWProjectDatabase

logger = get_logger(__file__)


def list_importers() -> dict[str, Union[dict[str, list[TypeVar]], dict[str, TypeVar]]]:
    """
    :return: format map to list of importers, names to importers map
    """
    package = importlib.import_module("bw2io.importers")
    format2importer: dict[str, list[TypeVar]] = {}
    name2importer: dict[str, TypeVar] = {}
    for name, obj in inspect.getmembers(package):
        if inspect.isclass(obj):
            format_value = getattr(obj, 'format', None)
            if format_value is not None:
                format2importer.setdefault(format_value, []).append(obj)
                name2importer[name] = obj
                # print(format_value, name)
    return {"formats": format2importer, "importers": name2importer}


def get_bw_importer(bw_database: BWProjectDatabase) -> Any:
    if bw_database.importer:
        return list_importers()["importers"][bw_database.importer]
    else:
        formats = list_importers()["formats"][bw_database.format]
        if len(formats) == 1:
            return formats[0]
        else:
            raise Exception("Multiple importers for format, please specify importer")


def setup_bw_project(project: BWProject, require_fresh: bool = False) -> None:
    """
    Set up a Brightway2 project and import databases
    :param project:
    :param require_fresh:
    :return:
    """
    logger.info(f"Setup {project.project_name}")
    if project.project_name in bd.projects:
        if require_fresh:
            raise Exception(f"project {project.project_name} already exists and 'require_fresh' is set to True.")
        logger.info(f"project '{project.project_name}' already exists.")
        bd.projects.set_current(project.project_name)
    else:
        logger.info("creating project")
        bd.projects.create_project(project.project_name)
        bd.projects.set_current(project.project_name)
        bi.bw2setup()

    for db in project.databases:
        setup_bw_db(db)


def setup_bw_db(db: BWProjectDatabase):
    if db.name in bd.databases:
        logger.info(f"Database {db.name} already exists, skipping")
        return
    if not Path(db.source).exists:
        raise Exception(f"Source {db.source} does not exist")
    logger.info(f"Importing database ")
    bw_importer = get_bw_importer(db)
    logger.info(f"Importing {db.name} from '{db.source}' using '{bw_importer.__name__}'")
    # return bw_importer
    imported = bw_importer(str(db.source), db.name)
    imported.apply_strategies()
    # print(type(imported))
    if imported.all_linked:
        imported.write_database()
