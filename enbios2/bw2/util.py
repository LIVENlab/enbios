from typing import Generator, Iterator

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
    return Activity(ActivityDataset.get(ActivityDataset.code == code))
