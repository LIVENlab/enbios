from typing import Dict, List, Tuple, Set, Optional, Container
import networkx as nx

from nexinfosys.common.decorators import memoized_method
from nexinfosys.solving.graph import Node, EdgeType, Weight, Value


class ComputationGraph:
    """
    A Computation Graph is a directed graph where:
     - Each node represents a variable to be computed, or whose value is given.
     - Each directed edge represents how the destination node can be computed based on the value of the source node, or
       in the opposite direction if a reverse weight exists.
     - Computations can be done in 'direct' order (Top-Down strategy) using the direct weight, and in the 'reverse'
       order (Bottom-Up strategy) using the reverse weight.

    How node values are calculated?

     - General rule: Adding the weighted input values, i.e., the value of the selected node is the sum of each incoming
                     (predecessor) node value multiplied by the "weight" attribute associated with the edge from the
                     incoming node.
     - Split rule:   If the node value is split entirely (100%) into the successors nodes, its value can be computed in
                     the opposite direction (from successors) only by computing one of the input nodes.
    """
    def __init__(self, graph: Optional[nx.DiGraph] = None, name: str = None):
        self.graph = nx.DiGraph()
        self.name = name
        self.descendants: Optional[Dict[Node, Set[Node]]] = None

        if graph:
            for u, v, data in graph.edges(data=True):
                reverse_weight = (Weight(1.0)/data["weight"]) if data.get("add_reverse_weight") else None
                self.add_edge(u, v, data["weight"], reverse_weight)

            for n, data in graph.nodes(data=True):
                if data.get("add_split"):
                    self.mark_node_split(n, EdgeType.DIRECT)

            self.compute_descendants()

    @property
    def nodes(self):
        """ Return the nodes of the flow graph """
        return self.graph.nodes

    def nodes_not_in_container(self, container: Container[Node]) -> List[Node]:
        """ Return the nodes of the graph not present in a container """
        return [n for n in self.graph.nodes if n not in container]

    def nodes_in_container(self, container: Container[Node]) -> List[Node]:
        """ Return the nodes of the graph present in a container """
        return [n for n in self.graph.nodes if n in container]

    @memoized_method(maxsize=None)
    def _inputs(self, n: Node, edge_type: EdgeType) -> List[Tuple[Node, Optional[Weight]]]:
        """ Return the predecessors of a node in the specified direction """
        return [(e[0], e[2]['weight']) for e in self.graph.in_edges(n, data=True) if e[2]['type'] == edge_type]

    def direct_inputs(self, n: Node):
        """ Return the predecessors of a node in the 'direct' (Top-Down) direction """
        return self._inputs(n, EdgeType.DIRECT)

    def reverse_inputs(self, n: Node):
        """ Return the predecessors of a node in the 'reverse' (Bottom-Up) direction """
        return self._inputs(n, EdgeType.REVERSE)

    def weighted_successors(self, n: Node) -> List[Tuple[Node, Weight]]:
        """ Return the successors of a node that have a valid weight """
        return [suc for suc in self.graph.successors(n) if self.graph[n][suc]['weight'] is not None]

    def add_edge(self, u: Node, v: Node, weight: Optional[Weight], reverse_weight: Optional[Weight]):
        """ Add an edge with weight attributes to the computation graph """
        self.graph.add_edge(u, v, weight=weight, type=EdgeType.DIRECT)
        self.graph.add_edge(v, u, weight=reverse_weight, type=EdgeType.REVERSE)
        self.init_node_split(u)
        self.init_node_split(v)

        # Invalidate descendants, it must be recomputed
        self.descendants = None

    def init_node_split(self, n: Node) -> None:
        """ Set the default value for attribute 'split' to a node """
        if not self.graph.nodes[n].get("split"):
            self.graph.nodes[n]["split"] = [False, False]

    def mark_node_split(self, n: Node, graph_type: EdgeType, split: bool = True) -> None:
        """ Set the attribute 'split' to a node """
        self.graph.nodes[n]["split"][graph_type.value] = split

    def get_node_split(self, n: Node, graph_type: EdgeType) -> bool:
        """ Get the attribute 'split' from a node """
        return self.graph.nodes[n]["split"][graph_type.value]

    def get_direct_node_split(self, n: Node) -> bool:
        """ Get the DIRECT attribute 'split' from a node """
        return self.get_node_split(n, EdgeType.DIRECT)

    def get_reverse_node_split(self, n: Node) -> bool:
        """ Get the REVERSE attribute 'split' from a node """
        return self.get_node_split(n, EdgeType.REVERSE)

    def compute_descendants(self):
        def visit_forward(node: Node) -> None:
            visited_nodes.add(node)

            if node in self.descendants:
                visited_nodes.update(self.descendants[node])
                return

            for successor in self.weighted_successors(node):
                if successor not in visited_nodes:
                    visit_forward(successor)

        self.descendants = {}

        for n in self.graph.nodes:  # type: Node
            visited_nodes: Set[Node] = set()
            visit_forward(n)
            self.descendants[n] = visited_nodes - {n}

    # DEPRECATED METHODS
    def compute_conflicts(self, new_computed_nodes: Set[Node], prev_computed_nodes: Set[Node]) -> Dict[Node, Set[Node]]:
        _new_computed_nodes = self.nodes_in_container(new_computed_nodes)
        if not _new_computed_nodes:
            return {}  # Nothing new to compute

        if not self.descendants:
            self.compute_descendants()

        _prev_computed_nodes = self.nodes_in_container(prev_computed_nodes)
        return self._compute_conflicts(set(_new_computed_nodes), set(_prev_computed_nodes))

    def _compute_conflicts(self, new_computed_nodes: Set[Node], prev_computed_nodes: Set[Node]) -> Dict[Node, Set[Node]]:
        assert self.descendants
        conflicts: Dict[Node, Set[Node]] = {}
        computed_nodes = prev_computed_nodes | new_computed_nodes

        for node in prev_computed_nodes:
            intersection = self.descendants[node] & new_computed_nodes
            if intersection:
                conflicts[node] = intersection

        for node in new_computed_nodes:
            intersection = self.descendants[node] & computed_nodes
            if intersection:
                conflicts[node] = intersection

        return conflicts

    def compute_param_conflicts(self, params: Set[Node]) -> Dict[Node, Set[Node]]:
        """ **DEPRECATED METHOD**

            Calculate the conflicts between nodes with values - the parameters - in a computation graph.
            If node A has a conflict with node B, means that B can be entirely or partially computed from node A,
            so both nodes cannot have input values at the same time (unless these values are consistent).

            Example]
            - Given the graph: a -> b -> c -> e <- d
            - Given the parameters: a, b, c, d
            - The conflicts are:
                a: {b, c}  (b and c can be computed from a)
                b: {c} (c can be computed from b)
                c: none
                d: none


            :param params: the set of parameters of a computation graph, i.e. the nodes that have values and we use
                           to compute the values in the remaining nodes of the graph.
            :return: a dictionary with an entry for each parameter where the value is a set with the names of other
                     conflicting parameters
        """
        def visit_forward(node: Node) -> Set[Node]:
            visited_nodes.add(node)

            result = set()

            if node in sub_params:
                result.add(node)

            for suc in self.weighted_successors(node):
                if suc not in visited_nodes:
                    result |= visit_forward(suc)

            return result

        all_conflicts: Dict[Node, Set[Node]] = {}
        # Filter out params not in nodes
        filtered_params = {n for n in params if n in self.nodes}

        for param in filtered_params:
            sub_params: Set[Node] = filtered_params - {param}
            visited_nodes: Set[Node] = set()
            conflicts = visit_forward(param)
            all_conflicts[param] = conflicts

        return all_conflicts

    @staticmethod
    def compute_param_combinations(conflicts: Dict[Node, Set[Node]]) -> Set[frozenset]:
        """ **DEPRECATED METHOD**

            Given the conflicts between parameters in a graph, compute the valid combinations of them.
            This is, if there are different nodes that can have a value, compute which combination of them can be used
            when calculating the values in the rest of nodes without having a conflict.
            A "conflict" should be read as: different ways of computing the value for a node.

            Example
            - Given the graph: a -> b -> c -> e <- d
            - Given the parameters: a, b, c, d
            - The conflicts are:
                a: {b, c}  (b and c can be computed from a)
                b: {c} (c can be computed from b)
                c: none
                d: none
            - The valid combinations are:
                {a, d}
                {b, d}
                {c, d}

            :param conflicts: a dictionary with an entry for each parameter where the value is a set with the names
                              of other conflicting parameters
            :return: a set with different combinations (sets) of parameters that can be used
        """
        def valid_combinations(param: Node, params: Set[Node]) -> Set[frozenset]:
            result: Set[frozenset] = set()
            other_params: Set[Node] = params - conflicts[param] - {param}
            non_conflicting = set()

            for other in other_params:
                if param not in conflicts[other]:
                    non_conflicting.add(other)

            for other in non_conflicting:
                for comb in valid_combinations(other, non_conflicting):
                    result |= {comb | frozenset({param})}

            if len(result) == 0:
                result = {frozenset({param})}

            return result

        combinations: Set[frozenset] = set()
        param_names = set(conflicts.keys())

        # This is a shortcut to avoid computation
        if all([len(s) == 0 for s in conflicts.values()]):
            return {frozenset(param_names)}

        # TODO: This routine needs optimization because its order of complexity is too big.
        #  It takes lot of time even with 10 parameters!

        for p in param_names:
            combinations |= valid_combinations(p, param_names)

        return combinations

    def compute_values(self, nodes: List[Node], params: Dict[Node, Value]) \
            -> Tuple[Dict[Node, Optional[Value]], Dict[Node, Optional[Value]]]:
        """ **DEPRECATED METHOD**

            Given a computation graph and a set of nodes with values (the parameters) compute the values of
            a list of nodes.

        :param nodes: the list of nodes whose value we are interested in
        :param params: a dictionary with an entry for each parameter and its value
        :return: a tuple with 1) the computed values for the desired nodes 2) computed values for other nodes during
                 the process.
        """
        def solve_inputs(inputs: List[Tuple[Node, Weight]], split: bool) -> Optional[Value]:
            input_values: List[Tuple[Value, Weight]] = []

            for n, weight in sorted(inputs):
                res_backward = solve_backward(n)

                # If node 'n' is a 'split' only one result is needed to compute the result
                if split:
                    if res_backward is not None:
                        return res_backward * weight
                else:
                    if res_backward is not None and weight is not None:
                        input_values.append((res_backward, weight))
                    else:
                        return None

            return Value.compute_weighted_addition(input_values)

        def solve_backward(node: Node) -> Optional[Value]:
            # Is the node already computed?
            if node in values:
                return values[node]

            # Does a parameter exist for this node?
            param = params.get(node)
            if param is not None:
                values[node] = param
                return param

            if node in pending_nodes:
                return None

            pending_nodes.append(node)

            split = self.graph.nodes[node]["split"]

            result = solve_inputs(self.direct_inputs(node), split[EdgeType.REVERSE.value])

            if result is None:
                result = solve_inputs(self.reverse_inputs(node), split[EdgeType.DIRECT.value])

            if result is not None:
                # TODO: this line is not generic
                result.name = node.name

            values[node] = result
            return result

        results: Dict[Node, Optional[Value]] = {}
        values: Dict[Node, Optional[Value]] = {}

        for n in nodes:
            pending_nodes: List[Node] = []
            value = solve_backward(n)
            results[n] = value

        return results, values
