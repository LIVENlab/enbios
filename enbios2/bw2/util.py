from typing import Generator, Iterator

import bw2data
from bw2data import databases as bw_databases
from bw2data.project import projects as bw_projects

from bw2data.backends import Activity, ExchangeDataset, ActivityDataset


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
            input=activity_copy, type=upstream["type"], amount=upstream.amount
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


if __name__ == "__main__":
    report()
    # bw2data.projects.purge_deleted_directories()
