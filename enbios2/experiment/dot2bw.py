from copy import copy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Union

import pydot
import bw2data as bd
from bw2data.backends import Activity, Exchange
from bw2data.errors import DuplicateNode

from enbios2.generic.enbios2_logging import get_logger
from enbios2.generic.util import get_data_file_path

logger = get_logger(__file__)


def read_dot_file(file_path: Path) -> pydot.Dot:
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
    for k, v in attributes.items():

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


@dataclass
class GraphData:
    activities: list[Activity]
    edges: list[ExchangeData]


@dataclass
class GraphDiffData:
    removed_activities: list[Activity] = field(default_factory=list)
    added_activities: list[Activity] = field(default_factory=list)
    removed_edges: list[Exchange] = field(default_factory=list)
    added_edges: list[Exchange] = field(default_factory=list)


class Dot2BW:

    def __init__(self, database_name: str = "graphDB", dot_file_path: Union[str, Path] = None):
        self.database_name = database_name
        if self.database_name not in bd.databases:
            bd.Database(database_name).register()
        self.database = bd.Database(database_name)

        self.graph_diff = GraphDiffData()

        if dot_file_path:
            dot_file_path = get_data_file_path(dot_file_path)
            graph = read_dot_file(dot_file_path)
            self.define_db_with_graph(graph)

    @staticmethod
    def edge_sign(edge: pydot.Edge) -> str:
        return f"_{edge.get_source()}>{edge.get_destination()}_"

    @staticmethod
    def resolve_edge_sign(edge_sign: str) -> tuple[str, str]:
        from_act, to_act, _ = edge_sign[1:-1].split(">")
        return from_act, to_act

    def define_db_with_graph(self, graph: pydot.Dot) -> GraphDiffData:
        """
        Define a database with the activities based on the dot graph.
        Returns new newly defined activities and edges.
        :param graph:
        :return:
        """

        # get all activities in the graphDB
        internal_act_map = {activity2node(act): act for act in self.database}
        # logger.debug(act_map.keys())

        # get all implicit nodes in the graph
        node_names: set[str] = collect_graph_node_names(graph)
        # print("node_names", node_names)
        activities_to_remove = set(internal_act_map.keys()) - node_names
        # print("activities_to_remove", activities_to_remove)
        # delete all activities that are not in the graph
        for act in activities_to_remove:
            internal_act_map[act].delete()
            self.graph_diff.removed_activities.append(internal_act_map[act])

        # get all external activities in the graph (defines explicitly in the graph)
        all_act_map = copy(internal_act_map)
        all_act_map.update(get_external_activities(graph))
        activities_to_add = node_names - set(all_act_map.keys())
        # print("activities_to_add", activities_to_add)

        for node in activities_to_add:
            activity = self.add_activity(node)
            internal_act_map[node] = activity
            all_act_map[node] = activity
            self.graph_diff.added_activities.append(activity)

        node_out_edges = {}
        for edge in graph.get_edges():
            node_out_edges.setdefault(edge.get_source(), []).append((edge.get_destination(), edge))

        for node_name, activity in internal_act_map.items():
            node_edges = node_out_edges.get(node_name, [])

            destination_nodes, edges = zip(*node_edges)

            exchanges = list(activity.exchanges())

            # print(node_edges, exchanges)
            # delete all exchanges that are not defined in the graph
            for ex in exchanges:
                if self.resolve_edge_sign(ex.get("sign"))[1] not in destination_nodes:
                    ex.delete()
                    self.graph_diff.removed_edges.append(ex)
            # add all edges that are not defined in the database
            edge_signs = [ex.get("sign") for ex in exchanges]
            for edge in edges:
                if self.edge_sign(edge) not in edge_signs:
                    from_act: Activity = all_act_map.get(edge.get_source())
                    to_act: Activity = all_act_map.get(edge.get_destination())
                    # print(from_act, to_act)
                    attributes = edge.get_attributes()
                    amount = attributes.get("amount", 1)
                    type_ = attributes.get("type", "technology")
                    unit = attributes.get("unit")

                    # print(from_act, to_act)
                    sign = self.edge_sign(edge)

                    print(f"adding exchange: {from_act['name']}, {to_act['name']}, {type_}, {amount}, {unit}, {sign}")
                    ex:Exchange = from_act.new_exchange(input=to_act, type=type_, amount=amount, unit=unit,
                                               sign=sign)
                    ex.save()
                    self.graph_diff.added_edges.append(ex)

        return self.graph_diff

    def add_activity(self, name: str) -> Activity:
        try:
            act = self.database.new_activity(name=name, code=f"_{name}_")
            act.save()
            print(f"activity saved: {name}")
            return act
        except DuplicateNode:
            logger.debug(f"Duplicate node not added {name}")
            pass
