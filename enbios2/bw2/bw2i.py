from pathlib import Path

from enbios2.bw2.bw_autoimporter import get_bw_importer
from enbios2.const import BASE_DATA_PATH
from enbios2.models.multi_scale_bw import BWSetup, BWDatabase

# import bw2analyzer as ba
import bw2data as bd
# import bw2calc as bc
import bw2io as bi
# import matrix_utils as mu
# import bw_processing as bp


def bw_setup(setup: BWSetup, force_db_setup: bool = False) -> None:
    print(f"Setup {setup.project_name}")
    if setup.project_name in bd.projects:
        print(f"project {setup.project_name} already exists.")
        bd.projects.set_current(setup.project_name)
        if not force_db_setup:
            return
    else:
        print("creating project")
        bd.projects.create_project(setup.project_name)
        bd.projects.set_current(setup.project_name)
        bi.bw2setup()

    for db in setup.databases:
        setup_bw_db(db)


def setup_bw_db(db: BWDatabase):
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
    print(type(imported))
    if imported.all_linked:
        imported.write_database()


#
