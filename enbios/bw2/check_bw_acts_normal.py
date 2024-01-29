if __name__ == "__main__":
    import bw2data as bd
    import bw2io as bi
    from bw2io.importers import SingleOutputEcospold2Importer
    import random
    import string
    from bw2data.project import projects as bw_projects
    from bw2data import databases as bw_databases

    # print(bd.projects)
    gen_project_name = True
    project_name = ""
    while gen_project_name:
        random_letters = "".join(random.choice(string.ascii_lowercase) for i in range(6))
        project_name = f"test_{random_letters}"
        gen_project_name = project_name in bd.projects

    print(project_name)

    # print(bd.projects.report())

    # project_name = "test_project"
    bd.projects.create_project(project_name)
    bd.projects.set_current(project_name)
    bi.bw2setup()

    imported = SingleOutputEcospold2Importer(
        "/home/ra/projects/enbios/data/ecoinvent/ecoinvent 3.9.1_cutoff_ecoSpold02/datasets",
        "ecoi_391",
    )

    imported.apply_strategies()
    # print(type(imported))
    if imported.all_linked:
        imported.write_database()
        print("db written")

    projects = list(bd.projects)
    for project in projects:
        print(project)
        bw_projects.set_current(project.name)
        print(bw_projects.dir)
        print(bd.database.databases)
        databases = bw_databases
        print(databases)
