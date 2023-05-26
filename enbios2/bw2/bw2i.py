from pathlib import Path

from enbios2.bw2.bw_autoimporter import get_bw_importer
from enbios2.generic.enbios2_logging import get_logger
from enbios2.models.bw_project_models import BWProject, BWProjectDatabase

import bw2data as bd
import bw2io as bi


logger = get_logger(__file__)


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
