from typing import Generator, Iterator, Union

import bw2data
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
    # print(exchanges)
    # return {exc.input: exc for exc in act.exchanges()}


def iter_exchange_by_ids(ids: Iterator[int], batch_size: int = 1000) -> Generator[ExchangeDataset, None, None]:
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


def iter_activities_by_codes(codes: Iterator[str], batch_size: int = 1000) -> Generator[ActivityDataset, None, None]:
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
        upstream.output.new_exchange(input=activity_copy, type=upstream["type"], amount=upstream.amount).save()
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


def method_search(project_name: str, method_tuple: tuple[str, ...]) -> Union[
    dict, tuple[tuple[str, ...], dict[str, str]]]:
    """
    Search for a method in a brightway project.
    Search name can be a incomplete tuple. IT will result the remaining parts in the method-tree
    In case of a match, it will result the full tuple and the method data
    todo: this method is weird and does too many things
    """
    assert project_name in bw2data.projects, f"Project '{project_name}' does not exist"
    bw2data.projects.set_current(project_name)
    all_methods = bw2data.methods

    bw_method = all_methods.get(method_tuple)
    if bw_method:
        return tuple(method_tuple), bw_method

    method_tree: dict = {}
    for bw_method in all_methods.keys():
        # iter through tuple
        current = method_tree
        for part in bw_method:
            current = current.setdefault(part, {})

    current = method_tree

    result = list(method_tuple)
    for index, part in enumerate(method_tuple):
        _next = current.get(part)
        assert _next, (f"Method not found. Part: '{part}' does not exist for {list(method_tuple)[index - 1]}. "
                       f"Options are '{current}'")
        current = _next

    while True:
        if len(current) > 1:
            return current.keys()
        if len(current) == 0:
            break
        elif len(current) == 1:
            _next = list(current.keys())[0]
            result.append(_next)
            current = current[_next]
    bw_method = all_methods.get(tuple(result))

    if bw_method:
        return tuple(result), bw_method
    raise ValueError(f"Method does not exist {method_tuple}")


def report():
    projects = list(bw2data.projects)
    for project in projects:
        print(project)
        bw2data.projects.set_current(project.name)
        databases = list(bw2data.databases)
        print(databases)


if __name__ == '__main__':
    report()
    # bw2data.projects.purge_deleted_directories()
