from typing import Optional

import bw2data
from bw2data.backends import ActivityDataset, ExchangeDataset, Activity, SQLiteBackend
from tqdm import tqdm


def check_unique_codes():
    activity_codes = [a.code for a in ActivityDataset.select(ActivityDataset.code)]
    assert len(activity_codes) == len(set(activity_codes))


def get_tree(code: str,
             keep_exchange_type: list[str] = None,
             check_unique_code: bool = True,
             max_level: int = -1) -> tuple[set[str], list[int]]:
    """
    Get all nodes that are connected to the given node. (as inputs)
    :param code: root code
    :param keep_exchange_type: keep the exchanges of the given type
    :param check_unique_code: check if the codes are unique
    :param max_level: max level to go down. -1 for infinite
    :return:
    """
    if check_unique_code:
        check_unique_codes()

    # all visited nodes (codes)
    visited: set[str] = set[str]()
    # nodes to visit next
    to_visit = {code}
    # nodes to visit in the next iteration
    to_visit_next = set()
    # all exchanges (eventually filtered by type). just ids for memory efficiency
    all_exchanges: list[int] = []
    current_level: int = 0

    while len(to_visit) and max_level != 0:
        # get all exchanges that we could currently reach
        exchanges = list(
            ExchangeDataset.select(ExchangeDataset.id, ExchangeDataset.input_code, ExchangeDataset.output_code).where(
                ExchangeDataset.output_code.in_(to_visit)))
        # add nodes that we did not visit yet for the next iteration
        for exc in exchanges:
            if exc.input_code == exc.output_code:
                continue
            if exc.input_code not in visited:
                to_visit_next.add(exc.input_code)
        # updated visited nodes
        visited.update(to_visit)
        # save exchanges
        if not keep_exchange_type:
            all_exchanges.extend([e.id for e in exchanges])
        else:
            all_exchanges.extend([exc.id for exc in exchanges if exc.type in keep_exchange_type])
        # update nodes to visit for next iteration
        to_visit = to_visit_next.copy()
        # reset nodes to visit in next iteration
        to_visit_next.clear()

        print(f"visited: {len(visited)}, total exchanges: {len(all_exchanges)}, next nodes: {len(to_visit)}")
        max_level -= 1
        current_level += 1
    return visited, all_exchanges


def get_tree_with_levels(code: str,
                         keep_exchange_type: Optional[list[str]] = None,
                         check_unique_code: Optional[bool] = True,
                         store_ids: bool = False,
                         max_level: Optional[int] = -1) -> tuple[dict[str, int], dict[str, set[str]], list[int]]:
    """
    Get all nodes that are connected to the given node. (as inputs)
    :param code: root code
    :param keep_exchange_type: keep the exchanges of the given type
    :param check_unique_code: check if the codes are unique
    :param store_ids: store the ids instead of codes
    :param max_level: max level to go down. -1 for infinite
    :return:
    """
    if check_unique_code:
        check_unique_codes()

    visited: set[str] = set[str]()
    # keep a dict of code -> level
    level: dict[str, int] = {}
    # keep a dict. code -> all input codes
    inputs: dict[str, set[str]] = {}
    # nodes to visit next
    to_visit: set[str] = set[str]()
    # nodes to visit in the next iteration
    # all exchanges (eventually filtered by type). just ids for memory efficiency
    all_exchanges: list[int] = []
    to_visit.add(code)
    current_level: int = 0

    while len(to_visit) and max_level != 0:
        print("---")
        # get all exchanges that we could currently reach
        visited.update(to_visit)
        for v in to_visit:
            level[v] = current_level
            inputs[v] = set()

        exchanges = list(
            ExchangeDataset.select(ExchangeDataset.id,
                                   ExchangeDataset.input_code,
                                   ExchangeDataset.output_code,
                                   ExchangeDataset.type).where(
                ExchangeDataset.output_code.in_(to_visit)))

        to_visit.clear()
        # add nodes that we did not visit yet for the next iteration
        for exc in exchanges:
            # ignore if production
            if exc.input_code == exc.output_code:
                continue
            if exc.input_code not in visited:
                to_visit.add(exc.input_code)
                inputs[exc.output_code].add(exc.input_code)
            else:
                # get level of visited node
                def rec_inc(node: str, new_level):
                    level[node] = new_level
                    for inp in inputs.get(node, []):
                        rec_inc(inp, new_level + 1)

                rec_inc(exc.input_code, level[exc.output_code] + 1)
        # save exchanges
        if not keep_exchange_type:
            all_exchanges.extend([e.id for e in exchanges])
        else:
            all_exchanges.extend([exc.id for exc in exchanges if exc.type in keep_exchange_type])
        print(f"visited: {len(visited)}, total exchanges: {len(all_exchanges)}, next nodes: {len(to_visit)}")
        max_level -= 1
        current_level += 1
    # sort by the second element of the tuple (level)
    if to_visit:
        for node in to_visit:
            level[node] = current_level
    return level, inputs, all_exchanges


def code_transform(level: dict[str, int], inputs: dict[str, set[str]]) -> tuple[dict[int, int], dict[int, set[int]]]:
    """
    transform codes to ids
    :return:
    """
    activities = list(ActivityDataset.select().where(ActivityDataset.code.in_(list(level.keys()))))
    activity_code2id: dict[str, int] = {a.code: a.id for a in activities}
    level_id: dict[int, int] = {
        activity_code2id[k]: v
        for k, v in level.items()
    }
    inputs_ids: dict[int, set[int]] = {
        activity_code2id[k]: set(activity_code2id[c] for c in v)
        for k, v in inputs.items()
    }
    return level_id, inputs_ids


def get_activities_by_code(codes: list[str]) -> list[Activity]:
    return [
        Activity(a) for a in ActivityDataset.select().where(ActivityDataset.code.in_(codes))
    ]


if __name__ == "__main__":
    pass
    bw2data.projects.set_current("ecoinvent_391")
    act_code = 'b9d74efa4fd670b1977a3471ec010737'
    # tree = get_tree('b9d74efa4fd670b1977a3471ec010737')
    # level, inputs, all_exchanges = get_tree_with_levels(act_code, max_level=1)
    # level_id, input_id = code_transform(level, inputs)

