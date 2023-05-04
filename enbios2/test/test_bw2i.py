from bw2data.backends import SQLiteBackend

from enbios2.const import BASE_DATA_PATH
from enbios2.models.multi_scale_bw import BWSetup, BWDatabase

if __name__ == "__main__":
    from enbios2.bw2.bw2i import bw_setup
    # bw_setup(setup=BWSetup(
    #     project_name="test",
    #     databases=[
    #         BWDatabase(
    #             name="test",
    #             source="test",
    #             format="test",
    #             importer="test"
    #         )]
    # ))
    ecoinvent_db_path = BASE_DATA_PATH / "ecoinvent 3.9.1_cutoff_ecoSpold02/datasets"

    setup = BWSetup("test8", [BWDatabase("cutoff391", format="Ecospold2", source=ecoinvent_db_path)])
    bw_setup(setup)
    a: SQLiteBackend
