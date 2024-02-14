from typing import Generator, Iterator, Literal

import bw2data
from bw2calc import LCA
from bw2data import databases as bw_databases
from bw2data.backends import Activity, ExchangeDataset, ActivityDataset
from bw2data.project import projects as bw_projects
from scipy.sparse import csr_matrix


def info_exchanges(act: Activity) -> dict:
    """
    Show exchanges of an activity
    :param act:
    :return:
    """
    exchanges = act.exchanges()
    for exc in exchanges:
        print(exc)
    for exc in act.upstream():
        print(exc)
    return {exc.input: exc for exc in act.exchanges()}


def iter_exchange_by_ids(
    ids: Iterator[int], batch_size: int = 1000
) -> Generator[ExchangeDataset, None, None]:
    """
    Iterate over exchanges by ids
    :param ids:
    :param batch_size:
    :return:
    """
    last_batch: bool = False
    while True:
        batch_ids: list[int] = []
        for i in range(batch_size):
            try:
                batch_ids.append(next(ids))
            except StopIteration:
                last_batch = True
                break
        for exc in ExchangeDataset.select().where(ExchangeDataset.id.in_(batch_ids)):
            yield exc
        if last_batch:
            break


def iter_activities_by_codes(
    codes: Iterator[str], batch_size: int = 1000
) -> Generator[ActivityDataset, None, None]:
    """
    Iterate over activities by codes
    :param codes:
    :param batch_size:
    :return:
    """
    last_batch: bool = False
    while True:
        batch_codes: list[str] = []
        for i in range(batch_size):
            try:
                batch_codes.append(next(codes))
            except StopIteration:
                last_batch = True
                break
        for act in ActivityDataset.select().where(ActivityDataset.code.in_(batch_codes)):
            yield act
        if last_batch:
            break


def get_activity(code: str) -> Activity:
    """
    Get activity by code
    :param code:
    :return:
    """
    activity_ds = ActivityDataset.get_or_none(ActivityDataset.code == code)
    if not activity_ds:
        raise ValueError(f"Activity with code '{code}' does not exist")
    return Activity(activity_ds)


def full_duplicate(activity: Activity, code=None, **kwargs) -> Activity:
    """
    Make a copy of an activity with its upstream exchanges
    (Otherwise, you cannot calculate the lca of the copy)
    :param activity: the activity to copy
    :param code: code of the new activity
    :param kwargs: other data for the copy
    :return: new activity
    """
    activity_copy = activity.copy(code, **kwargs)
    for upstream in activity.upstream():
        upstream.output.new_exchange(
            input=activity_copy, type=upstream["type"], magnitude=upstream.magnitude
        ).save()
    activity_copy.save()
    return activity_copy


def clean_delete(activity: Activity):
    """
    Delete an activity and its upstream exchanges.
    Otherwise, the system will be corrupted
    :param activity: The activity to delete
    """
    for link in activity.upstream():
        link.delete()
    activity.delete()


def report():
    current_ = bw2data.projects.current
    projects = list(bw2data.projects)
    for project in projects:
        print(project)
        bw_projects.set_current(project.name)
        databases = list(bw_databases)
        print(databases)
    bw2data.projects.set_current(current_)


def bw_unit_fix(unit_str: str):
    if unit_str == "kilowatt hour":
        return "kilowatt_hour"
    if unit_str == "unit":
        return "unspecificEcoinventUnit"
    return unit_str


def delete_db(project_name: str, db_name: str):
    if project_name not in bw2data.projects:
        raise ValueError(f"Project '{project_name}'does not exist")
    bw2data.projects.set_current(project_name)
    if db_name not in bw2data.databases:
        raise ValueError(f"Database '{db_name}'does not exist")
    print("proceeding to delete database ...")
    bw2data.Database(db_name).delete_instance()


def delete_project(project_name: str):
    if project_name == "default":
        print("Not gonna delete project 'default'")
        return
    if project_name not in bw2data.projects:
        print(f"Project '{project_name}'does not exist")
        return
    bw2data.projects.delete_project(project_name, True)


def delete_all_projects():
    resp = input("Are you sure you want to delete all projects? [y]")
    if resp != "y":
        print("cancelled")
        return
    for project in bw2data.projects:
        if project.name != "default":
            bw2data.projects.delete_project(project.name, True)
            print(f"Deleted {project.name}")
    bw2data.projects.purge_deleted_directories()


"""
Following two functions are from bw_utils...
"""


def _check_lca(
    lca: LCA,
    make_calculations: bool = True,
    inventory_name: Literal["inventory", "characterized_inventory"] = "inventory",
):
    if not hasattr(lca, "inventory"):
        if make_calculations:
            print("calculating inventory")
            lca.lci()
        else:
            raise ValueError("Must do lci first")
    if inventory_name == "inventory":
        return

    if not hasattr(lca, "characterization_matrix"):
        print("loading lcia data")
        lca.load_lcia_data()

    if not hasattr(lca, "characterized_inventory"):
        if make_calculations:
            print("calculating lcia")
            lca.lcia()
        else:
            raise ValueError("Must do lcia first")


def split_inventory(
    lca: LCA,
    technosphere_activities: list[int],
    inventory_name: Literal["inventory", "characterized_inventory"] = "inventory",
    make_calculations: bool = True,
) -> csr_matrix:
    """
    Split the results of a lcia calculation into groups. Each group is a list of activities, specified by their ids.
    Calculations of lci and lcia are performed when they are missing and `make_calculations` is set to `True`
    :param lca: bw LCA object
    :param technosphere_activities: list of (technosphere) activity-groups, activities are specified by their 'id'
    :param make_calculations: make lci and lcia calculations if the corresponding matrices are missing
    :return: a list of sparse matrices, which are characterized inventories split by the activity groups.
    score can be calculated by calling `sum()` for any matrix.
    """
    _check_lca(lca, make_calculations, inventory_name)
    inventory = getattr(lca, inventory_name)
    # do matrix multiplication for each final location
    return inventory[:, [lca.dicts.activity[c] for c in technosphere_activities]]
