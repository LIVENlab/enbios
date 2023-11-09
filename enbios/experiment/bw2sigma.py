import json
from typing import Any, Iterable, Union

import bw2data as bd
from bw2data.backends import Exchange, Activity, ActivityDataset, ExchangeDataset


def system_overview() -> list[tuple]:
    _current = bd.projects.current
    data = []

    names = sorted([x.name for x in bd.projects])
    for obj in names:
        bd.projects.set_current(obj, update=False, writable=False)
        data.append([bd.projects.current] + [[k, v, len(bd.Database(k))] for k, v in bd.databases.items()])
    bd.projects.set_current(_current)
    return data


def graph(database: str) -> Any:
    db = bd.Database(database)
    result = {"nodes": [], "edges": []}
    nodes = result["nodes"]
    edges = result["edges"]

    for obj in db._get_queryset().dicts():
        data = obj["data"]
        nodes.append(
            {"key": obj["code"], "attributes": {**{"size": 15, "label": data["name"], "x": 100, "y": 100}, **{}}})

    from bw2data.backends.schema import ExchangeDataset

    edges_q = ExchangeDataset.select(ExchangeDataset.id, ExchangeDataset.data).where(
        (ExchangeDataset.output_database == database) & (ExchangeDataset.input_database == database)).limit(100)

    for edge in edges_q:
        edges.append({"key": edge.id, "source": edge.data["output"][1], "target": edge.data["input"][1]
                         , "attributes": {"size": 1}})

    return result


def grap_nodes(activities: Iterable[Union[Activity, ActivityDataset]],
               exchanges: Iterable[Union[Exchange, ExchangeDataset]],
               level_infos: Iterable[int]) -> Any:
    result = {"nodes": [], "edges": []}
    nodes = result["nodes"]
    edges = result["edges"]
    levels_iter = iter(level_infos)

    activity_codes: list[str] = []

    current_level = -1
    node_on_current_level = 0
    for obj in activities:
        if isinstance(obj, ActivityDataset):
            dataset_obj: ActivityDataset = obj
        else:
            dataset_obj: ActivityDataset = obj._document
        activity_codes.append(dataset_obj.code)
        level = next(levels_iter)
        if level == current_level:
            node_on_current_level += 1
        else:
            current_level = level
            node_on_current_level = 0
        # print(dataset_obj.name, level, node_on_current_level)
        nodes.append(
            {"key": str(len(activity_codes) - 1),
             "attributes": {**{"size": 15,
                               "label": dataset_obj.name,
                               "color": "#00ff00" if "bio" in dataset_obj.database else "a0a0a0",
                               "x": 100 - level * 30,
                               "y": 100 + node_on_current_level * 30}, **{}}})

    for exc in exchanges:
        if isinstance(exc, ExchangeDataset):
            exc_obj: ExchangeDataset = exc
        else:
            exc_obj: ExchangeDataset = exc._document

        if exc_obj.output_code in activity_codes and exc_obj.input_code in activity_codes:
            edges.append({"key": str(exc_obj.id),
                          "source": str(activity_codes.index(exc_obj.output_code)),
                          "target": str(activity_codes.index(exc_obj.input_code)),
                          "type":"arrow",
                          "attributes": {"size": 1}})

    return result


if __name__ == "__main__":
    # print(json.dumps(system_overview(), indent=2))
    db_name = "cutoff391"
    bd.projects.set_current("ecoi_dbs")
    graph_data = graph(db_name)
    json_data = json.dumps(graph_data, indent=2)
    # open("test.json", "w", encoding="utf-8").write(json.dumps({"nodes": activities, "edges": []}, indent=2))
    # bd.Database("cutoff391").load()
