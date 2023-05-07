from copy import copy
from dataclasses import dataclass
from logging import getLogger
from pathlib import Path
import pydot
import bw2data as bd
from bw2data import ProcessedDataStore
from bw2data.backends import Activity
from bw2data.errors import DuplicateNode

from enbios2.const import BASE_DATA_PATH

logger = getLogger(__name__)


def read_dot_file(file_path: Path):
    dot_text = file_path.read_text(encoding="utf-8")
    graph, = pydot.graph_from_dot_data(dot_text)
    return graph


@dataclass
class ActivityData:
    name: str
    code: str
    database: str
    location: str
    type: str
    unit: str
    categories: tuple[str]
    exchanges: list[dict[str, any]]


@dataclass
class ExchangeData:
    amount: float
    type: str


def collect_graph_node_names(graph: pydot.Dot) -> set[str]:
    edges = graph.get_edges()
    all_nodes = set()  # the implicit ones in the edges

    for edge in edges:
        source = edge.get_source()
        target = edge.get_destination()
        all_nodes.update([source, target])

    return all_nodes


def node2code(node: pydot.Node):
    return f"_{node.get_name()}_"


def activity2node(activity: Activity):
    return activity["code"][1:-1]


def parse_node_attributes(node: pydot.Node):
    attributes: dict[str, str] = copy(node.get_attributes())
    for k,v in attributes.items():

        if v.startswith('"') and v.endswith('"'):
            v = v[1:-1]
        # split lists # todo assumes there are no " in the list with commas in the strings.
        if v.startswith("[") and v.endswith("]"):
            v = v[1:-1]
            v = v.split(",")
            v = [part.strip() for part in v]
        attributes[k] = v
    return attributes


def get_external_activities(graph: pydot.Dot):
    external_activities_nodes = graph.get_nodes()
    external_activity_map: dict[str, Activity] = {}

    for node in external_activities_nodes:
        attributes = parse_node_attributes(node)
        if "database" in attributes:
            # more attributes, like code, location
            activity = bd.Database(attributes["database"]).get(name=attributes["name_"],
                                                    categories=tuple(attributes["categories"]),
                                                    type=attributes["type"])
            logger.debug(f"external activity : %s", attributes["name_"])
            external_activity_map[node.get_name()] = activity
        else:
            pass
            # no database attribute, so it is a reference to an activity in the current database
    return external_activity_map


def define_db_with_graph(db: ProcessedDataStore, graph: pydot.Dot):
    """
    Define a database with the activities based on the dot graph.
    :param db:
    :param graph:
    :return:
    """
    # get all activities in the graphDB
    act_map = {activity2node(act): act for act in db}

    # get all implicit nodes in the graph
    node_names: set[str] = collect_graph_node_names(graph)
    # get all external activities in the graph (defines explicitly in the graph)
    get_external_activities(graph)
    nodes_to_add = node_names.copy()
    # delete all activities that are not in the graph
    for act in act_map.keys():
        if act not in node_names:
            act_map[act].delete()
        else:
            nodes_to_add.discard(act)
    # todo there needs to be some deletion of exchanges that are not in the graph
    # print(nodes_to_add)
    for node in nodes_to_add:
        add_activity(node)

    act_map_exchanges = {key: [ex.get("sign") for ex in act.exchanges()] for key, act in act_map.items()}

    for edge in graph.get_edges():
        # print(edge.get_source(), edge.get_destination())
        from_act: Activity = act_map.get(edge.get_source())
        to_act = act_map.get(edge.get_destination())
        # print(from_act, to_act)
        attributes = edge.get_attributes()
        amount = float(attributes.get("amount", 1))
        type_ = attributes.get("type", "technology")

        sign = f"_{from_act['name']}>{to_act['name']}_"
        if sign in act_map_exchanges[from_act["name"]]:
            print(f"skipping {sign}")
            continue
        from_act.new_exchange(input=to_act, type=type_, amount=amount,
                              sign=sign).save()
        print(f"new exchange: {from_act['name']}>{to_act['name']}")


def add_activity(name: str):
    try:
        act = mydb.new_activity(name=name, code=f"_{name}_")
        act.save()
        print(f"activity saved: {name}")
    except DuplicateNode:
        pass


bd.projects.set_current("ecoinvent_test")
# bd.projects.migrate_project_25()
mydb = bd.Database("graphDB")  # Crear una database nova
# mydb.register()  # Perquè aparegui a la llista de databases

graph = read_dot_file(BASE_DATA_PATH / "dot/dot_example.dot")

define_db_with_graph(mydb, graph)
