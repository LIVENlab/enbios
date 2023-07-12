import bw2data

def delete_db(project_name: str, db_name: str):
    if project_name not in bw2data.projects:
        raise ValueError(f"Project '{project_name}'does not exist")
    bw2data.projects.set_current(project_name)
    if db_name not in bw2data.databases:
        raise ValueError(f"Database '{db_name}'does not exist")
    print("proceeding to delete database ...")
    bw2data.Database(db_name).delete_instance()


if __name__ == '__main__':
    delete_db("ecoinvent", "cutoff_3.9.1_default")
