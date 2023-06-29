from enbios2.base.databases import init_databases
from enbios2.ecoinvent.ecoinvent_index import analyze_directory, get_ecoinvent_dataset_index

if __name__ == "__main__":
    init_databases()
    analyze_directory(store_to_index_file=True)

    all_indexes = get_ecoinvent_dataset_index()
    for index in all_indexes:
        if index.bw_project_db.exists():
            es = index.bw_project_db.get()
            print(es)
        else:
            if index.version == "3.9.1" and index.type == "default":
                print(f"Should add: {index}")
            print(f"Missing: {index}")
