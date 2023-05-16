from pathlib import Path

from enbios2.bw2.bw_autoimporter import get_bw_importer
from enbios2.models.project import BWProject, BWProjectDatabase

import bw2data as bd
import bw2io as bi


def setup_bw_project(project: BWProject, require_fresh: bool = False) -> None:
    """
    Setup a Brightway2 project and import databases
    :param project:
    :param require_fresh:
    :return:
    """
    print(f"Setup {project.project_name}")
    if project.project_name in bd.projects:
        if require_fresh:
            raise Exception(f"project {project.project_name} already exists and 'require_fresh' is set to True.")
        print(f"project '{project.project_name}' already exists.")
        bd.projects.set_current(project.project_name)
    else:
        print("creating project")
        bd.projects.create_project(project.project_name)
        bd.projects.set_current(project.project_name)
        bi.bw2setup()

    for db in project.databases:
        setup_bw_db(db)


def setup_bw_db(db: BWProjectDatabase):
    if db.name in bd.databases:
        print(f"Database {db.name} already exists, skipping")
        return
    if not Path(db.source).exists:
        raise Exception(f"Source {db.source} does not exist")
    print(f"Importing database ")
    bw_importer = get_bw_importer(db)
    print(f"Importing {db.name} from '{db.source}' using '{bw_importer.__name__}'")
    # return bw_importer
    imported = bw_importer(str(db.source), db.name)
    imported.apply_strategies()
    # print(type(imported))
    if imported.all_linked:
        imported.write_database()
