import pickle
from pathlib import Path

import bw2data
import networkx as nx
from bw2data.backends import ActivityDataset, ExchangeDataset
from networkx import topological_sort, cycle_basis, dominating_set
from tqdm import tqdm
from playhouse.shortcuts import model_to_dict

from enbios2.bw2.project_index import set_bw_current_project
from enbios2.const import BASE_DATA_PATH
from enbios2.generic.files import DataPath

# Create an empty graph
G = nx.DiGraph()

set_bw_current_project("cutoff", "3.9.1")
# print(list(bw2data.databases))
# random_act = db.random()
db = bw2data.Database("cutoff_3.9.1_default")
activities = ActivityDataset.select()

# g_node_data = []
for act in tqdm(activities):
    # data = model_to_dict(act)
    # data["location_name"] = geo_code2_name.get(act.location, "")
    # G.add_nodes_from([(act.code, data)]
    # g_node_data.append((act.code, {"name": act.name}))
    G.add_node(act.code)
# G.add_nodes_from(g_node_data)

for exc in tqdm(ExchangeDataset.select()):
    # print(exc)
    # G.add_edges_from((exc.input_code, exc.output_code, {"weight": exc.data["amount"]}))
    G.add_edge(exc.input_code, exc.output_code)

# Add edges (links)
# edges_data = [
#     ('Node1', 'Node2', {'weight': 3.1415}),
#     # add more edges here
# ]
# G.add_edges_from(edges_data)


def serialize(graph: nx.Graph, path: Path):
    # Save to a pickle file
    pickle.dump(graph, path.open("wb"))


def deserialize(path: Path) -> nx.Graph:
    return pickle.load(path.open("rb"))

# serialize(G, (BASE_DATA_PATH / "graph.nx"))

# does not work for directed graphs
# ordered = topological_sort(G)
# with (BASE_DATA_PATH / "topolical_order.txt").open("w") as fout:
#     for node in ordered:
#         fout.write(node + "\n")



D = dominating_set(G)