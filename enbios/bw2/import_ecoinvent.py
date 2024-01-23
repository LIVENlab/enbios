from pathlib import Path

import bw2data
import bw2io


def import_ecoinvent(project_name: str, dataset_path: str, database_name: str):
    project_created = False

    db_path: Path = Path(dataset_path)
    if not db_path.exists() and not db_path.is_dir():
        raise Exception(f"{db_path} does not exist or is not a directory")
    if project_name in bw2data.projects:
        raise Exception(
            f"project {project_name} already exists and "
            f"'require_fresh' is set to True."
        )
    try:
        bw2data.projects.create_project(project_name)
        project_created = True
        bw2data.projects.set_current(project_name)
        bw2io.bw2setup()

        imported = bw2io.SingleOutputEcospold2Importer(dataset_path, database_name)
        imported.apply_strategies()
        if imported.all_linked:
            imported.write_database()
    except Exception as e:
        print(e)
        if project_created:
            pass


if __name__ == "__main__":
    import_ecoinvent(
        "ei391",
        "/home/ra/projects/enbios/data/ecoinvent/ecoinvent 3.9.1_cutoff_ecoSpold02/datasets",
        "ei391_cutoff_ecoSpold02",
    )
