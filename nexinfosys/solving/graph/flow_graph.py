import math
from functools import reduce
from operator import add
from typing import Dict, List, Tuple, Optional, Generator
import networkx as nx

from nexinfosys.solving.graph import Node, Weight, EdgeType
from nexinfosys.solving.graph.computation_graph import ComputationGraph
from nexinfosys.command_generators import IType, Issue


class FlowGraph:
    """
    A Flow Graph is a directed graph where:
     - Each node represents a variable to be computed or which value will be given.
     - Each directed edge represents the flowing of data from one node to another.
     - Each edge can have two attributes:
       * Direct weight: a weight affecting the amount of data flowing Top-Down
       * Reverse weight: a weight affecting the amount of data flowing Bottom-Up
    """
    def __init__(self, graph: Optional[nx.DiGraph] = None):
        self._direct_graph = nx.DiGraph()
        self._reverse_graph = nx.DiGraph()

        if graph:
            for u, v, data in graph.edges(data=True):
                self.add_edge(u, v, data["weight"], None)

    def add_edge(self, u: Node, v: Node, weight: Optional[Weight], reverse_weight: Optional[Weight]) -> None:
        """ Add an edge with weight attributes to the flow graph """
        self._direct_graph.add_edge(u, v, weight=weight)
        self._reverse_graph.add_edge(v, u, weight=reverse_weight)

    def edges(self) -> Generator[Tuple[Node, Node, Optional[Weight], Optional[Weight]], None, None]:
        """ Return the edges of the flow graph """
        for u, v, data in self._direct_graph.edges(data=True):  # type: Node, Node, Dict
            yield u, v, data["weight"], self._reverse_graph[v][u]["weight"]

    @property
    def nodes(self):
        """ Return the nodes of the flow graph """
        return self._direct_graph.nodes

    def get_computation_graph(self, structural_graph: Optional[nx.DiGraph] = None) -> Tuple[Optional[ComputationGraph], List[Issue]]:
        """ Get a Computation Graph out of the Flow Graph, after checking the validity (no cycles) and inferring
            missing weights and compute other node attributes.

            :return: the Computation Graph if no errors occurred and a list of messages given by the analysis
        """
        issues = self.analyze_and_complete(structural_graph)

        if IType.ERROR in [e.itype for e in issues]:
            return None, issues

        return self._create_computation_graph(), issues

    def _create_computation_graph(self) -> ComputationGraph:
        """ Create a Computation Graph based on the Flow Graph """
        graph = ComputationGraph()

        for u, v, weight, reverse_weight in self.edges():
            graph.add_edge(u, v, weight, reverse_weight)

        for n, split in self._direct_graph.nodes.data('split'):  # type: Node, bool
            if split:
                graph.mark_node_split(n, EdgeType.DIRECT)

        for n, split in self._reverse_graph.nodes.data('split'):  # type: Node, bool
            if split:
                graph.mark_node_split(n, EdgeType.REVERSE)

        return graph

    def analyze_and_complete(self, structural_graph: Optional[nx.DiGraph] = None) -> List[Issue]:
        """
        It analyzes the flow graph and completes it with inferrable data.

        First, it checks if Flow Graph is DAG (Directed Acyclic Graph), this is, no cycles exist
        Then, it follows these steps:

          How many output edges without weight has the node?
            * More than 1: missing weights (WARNING)
            * Only 1: how many output edges in total?
               * Only 1: is there an opposite weight?
                  * Yes: weight can be inferred as (1.0 / opposite weight) (INFO)
                  * No: weight can be inferred as 1.0 (INFO)
               * More than 1: compute sum of other edges with weight
                  * sum > 1: weight cannot be inferred (WARNING)
                  * sum <= 1: weight can be inferred as (1 - sum) (INFO)

        :return: a list of messages given by the analysis and completion of type INFO, WARNING or ERROR
        """
        issues: List[Issue] = []

        # Checking if graph is acyclic. Just looking at the direct graph is OK.
        # if not nx.algorithms.dag.is_directed_acyclic_graph(self._direct_graph):
        #     print("Cycles detected. The nodes connected in a cycle are: ")
        #     for n in nx.algorithms.cycles.simple_cycles(self._direct_graph):
        #         print(n)
        #     issues.append(Issue(IType.ERROR, 'The graph contains cycles'))
        #     return issues

        for graph, opposite_graph in [(self._direct_graph, self._reverse_graph), (self._reverse_graph, self._direct_graph)]:
            for n in nx.dfs_preorder_nodes(graph):

                graph.nodes[n]['split'] = False

                # Working on output edges only of node 'n'
                all_edges = graph.out_edges(n, data=True)

                if len(all_edges) == 0:
                    continue

                # How many output edges without weight has the node?
                edges_without_weight = [e for e in all_edges if e[2]['weight'] is None]

                if len(edges_without_weight) > 1:
                    str_edges = [f'({e[0]}, {e[1]})' for e in edges_without_weight]
                    issues.append(Issue(IType.WARNING,
                                        f'The following edges don\'t have a weight: {", ".join(str_edges)}'))

                elif len(edges_without_weight) == 1:

                    if len(all_edges) == 1:
                        edge = list(all_edges)[0]
                        u, v, data = edge
                        opposite_weight = opposite_graph[v][u]['weight']
                        if opposite_weight is not None:
                            data['weight'] = Weight(0.0) if opposite_weight == 0.0 else (Weight(1.0) / opposite_weight)
                            issues.append(Issue(IType.INFO,
                                                f'The weight of single output edge "{edge}" could be inferred from '
                                                f'opposite weight "{opposite_weight}"'))
                        elif structural_graph and (structural_graph.has_edge(u, v) or structural_graph.has_edge(v, u)):
                            issues.append(Issue(IType.WARNING,
                                                f'The weight of single output edge "{edge}" will not be inferred '
                                                f'because a structural edge exist'))
                        else:
                            data['weight'] = Weight(1.0)
                            issues.append(Issue(IType.INFO,
                                                f'The weight of single output edge "{edge}" could be inferred '
                                                f'without opposite weight'))
                    else:
                        sum_other_weights = reduce(add, [e[2]['weight'] for e in all_edges if e[2]['weight'] is not None])

                        if sum_other_weights > 1.0:
                            issues.append(Issue(IType.WARNING,
                                                f'The weight of edge "{edges_without_weight[0]}" cannot be inferred, '
                                                f'the sum of other weights is >= 1.0: {sum_other_weights}'))
                        else:
                            edges_without_weight[0][2]['weight'] = Weight(1.0) - sum_other_weights
                            graph.nodes[n]['split'] = True
                            issues.append(Issue(IType.INFO,
                                                f'The weight of edge "{edges_without_weight[0]}" could be inferred '
                                                f'from the sum of other weights'))

                elif len(all_edges) > 1:
                    # All edges have a weight
                    sum_all_weights = reduce(add, [e[2]['weight'] for e in all_edges])
                    if math.isclose(sum_all_weights, 1.0):
                        graph.nodes[n]['split'] = True

        return issues
