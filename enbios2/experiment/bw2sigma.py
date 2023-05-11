import json
from typing import Any

import bw2data as bd
from bw2data.backends import Exchange


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


if __name__ == "__main__":
    # print(json.dumps(system_overview(), indent=2))
    db_name = "cutoff391"
    bd.projects.set_current("ecoi_dbs")
    graph_data = graph(db_name)
    json_data  = json.dumps(graph_data, indent=2)
    # open("test.json", "w", encoding="utf-8").write(json.dumps({"nodes": activities, "edges": []}, indent=2))
    # bd.Database("cutoff391").load()
