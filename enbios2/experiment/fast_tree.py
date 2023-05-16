from bw2data.backends import ActivityDataset, ExchangeDataset, Activity
from bw2data import Database, databases, methods, config, projects




print(projects)
projects.set_current("uab_bw_ei39")
db = Database("ei391")
random_act = db.random()


def check_unique_codes():
    activity_codes = [a.code for a in ActivityDataset.select(ActivityDataset.code)]
    print(len(activity_codes), len(set(activity_codes)))
    assert len(activity_codes) == len(set(activity_codes))


def get_tree(code: str, keep_exchange_type: list[str] = None):
    """
    Get all nodes that are connected to the given node. (as inputs)
    :param code: root code
    :param keep_exchange_type: keep the exchanges of the given type
    :return:
    """
    visited = set()

    to_visit = {code}
    to_visit_next = set()
    all_exchanges = []

    while len(to_visit):
        exchanges = list(ExchangeDataset.select().where(ExchangeDataset.output_code.in_(to_visit)))
        for exc in exchanges:
            if exc.input_code not in visited:
                to_visit_next.add(exc.input_code)
        visited.update(to_visit)
        if not keep_exchange_type:
            all_exchanges.extend(exchanges)
        else:
            all_exchanges.extend([exc for exc in exchanges if exc.type in keep_exchange_type])
        to_visit = to_visit_next.copy()
        to_visit_next.clear()

        print(len(visited), len(to_visit), len(all_exchanges))
    return visited, all_exchanges

print(random_act)

visited, exchanges = get_tree(random_act.key[1], ["technosphere"])