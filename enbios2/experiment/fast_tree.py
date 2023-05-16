from bw2data.backends import ActivityDataset, ExchangeDataset, Activity
from bw2data import Database, databases, methods, config, projects


print(projects)
projects.set_current("uab_bw_ei39")
db = Database("ei391")
random_act = db.random()


def check_unique_codes():
    activity_codes = [a.code for a in ActivityDataset.select(ActivityDataset.code)]
    assert len(activity_codes) == len(set(activity_codes))


def get_tree(code: str, keep_exchange_type: list[str] = None, check_unique_code: bool = True):
    """
    Get all nodes that are connected to the given node. (as inputs)
    :param code: root code
    :param keep_exchange_type: keep the exchanges of the given type
    :param check_unique_code: check if the codes are unique
    :return:
    """
    if check_unique_code:
        check_unique_codes()

    # all visited nodes (codes)
    visited = set()
    # nodes to visit next
    to_visit = {code}
    # nodes to visit in the next iteration
    to_visit_next = set()
    # all exchanges (eventually filtered by type)
    all_exchanges = []

    while len(to_visit):
        # get all exchanges that we could currently reach
        exchanges = list(ExchangeDataset.select().where(ExchangeDataset.output_code.in_(to_visit)))
        # add nodes that we did not visit yet for the next iteration
        for exc in exchanges:
            if exc.input_code not in visited:
                to_visit_next.add(exc.input_code)
        # updated visited nodes
        visited.update(to_visit)
        # save exchanges
        if not keep_exchange_type:
            all_exchanges.extend(exchanges)
        else:
            all_exchanges.extend([exc for exc in exchanges if exc.type in keep_exchange_type])
        # update nodes to visit for next iteration
        to_visit = to_visit_next.copy()
        # reset nodes to visit in next iteration
        to_visit_next.clear()

        print(len(visited), len(to_visit), len(all_exchanges))
    return visited, all_exchanges


print(random_act)


visited, exchanges = get_tree(random_act["code"], ["technosphere"])