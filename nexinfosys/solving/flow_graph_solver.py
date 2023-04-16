"""
A solver based on the elaboration of a flow graph to quantify interfaces, connected by flow or scale relationships.
It is assumed that other kinds of relationship (part-of, upscale, ...) are translated into these two basic ones.
Another type of relationship considered is linear transform from InterfaceType to InterfaceType, which is cascaded into
appearances of its instances.

Before the elaboration of flow graphs, several preparatory steps:
* Find the separate contexts. Each context is formed by the "local", "environment" and "external" sets of processors,
  and is -totally- isolated from other contexts
  - Context defining attributes are defined in the Problem Statement command. If not, defined, a "context" attribute in
    Processors would be assumed
    If nothing is found, all Processors are assumed to be under the same context (what will happen??!!)
  - Elaborate (add necessary entities) the "environment", "external" top level processors if none have been specified.
    - Opposite processor can be specified when defining Interface
      - This attribute is taken into account if NO relationship originates or ends in this Interface. Then, a default
        relationship would be created
      - If Processors are defined for environment or
* Unexecuted model parts
  - Connection of Interfaces
  - Dataset expansion
* [Datasets]
* Scenarios
  - Parameters
* Time. Classify QQs by time, on storage
* Observers (different versions). Take average always

"""
import logging
import traceback
from collections import defaultdict
from copy import deepcopy, copy
from enum import Enum
from typing import Dict, List, Set, Any, Tuple, Union, Optional, NamedTuple, Generator, NoReturn, Sequence

import networkx as nx
from nexinfosys.command_generators.parser_ast_evaluators import ast_evaluator, ast_to_string

from nexinfosys.command_field_definitions import orientations
from nexinfosys.command_generators import Issue
from nexinfosys.command_generators.parser_field_parsers import is_year, \
    is_month, parse_string_as_simple_ident_list
from nexinfosys.common.constants import Scope
from nexinfosys.common.helper import PartialRetrievalDictionary, ifnull, FloatExp, precedes_in_list, \
    replace_string_from_dictionary, brackets, CaseInsensitiveDict, create_dictionary
from nexinfosys.model_services import get_case_study_registry_objects, State
from nexinfosys.models.musiasem_concepts import ProblemStatement, Parameter, FactorsRelationDirectedFlowObservation, \
    FactorsRelationScaleObservation, Processor, FactorQuantitativeObservation, Factor, \
    ProcessorsRelationPartOfObservation, FactorType
from nexinfosys.models.musiasem_concepts_helper import find_quantitative_observations
from nexinfosys.solving.flow_graph_outputs import export_solver_data
from nexinfosys.solving.graph import InterfaceNode, InterfaceNodeHierarchy, NodeFloatComputedDict, \
    ProcessorsRelationWeights, ResultDict, ResultKey, ConflictResolution, FloatComputedTuple, \
    ComputationSource, Computed, evaluate_parameters_for_scenario, SolvingException, \
    evaluate_numeric_expression_with_parameters, AstType
from nexinfosys.solving.graph.computation_graph import ComputationGraph
from nexinfosys.solving.graph.flow_graph import FlowGraph, IType


class AggregationConflictResolutionPolicy(Enum):
    TakeUpper = 1
    TakeLowerAggregation = 2

    @staticmethod
    def get_key():
        return "NISSolverAggregationConflictResolutionPolicy"

    def resolve(self, computed_value: FloatComputedTuple, existing_value: FloatComputedTuple) \
            -> Tuple[FloatComputedTuple, FloatComputedTuple]:

        if self == self.TakeLowerAggregation:
            # Take computed aggregation over existing value
            return computed_value, existing_value
        elif self == self.TakeUpper:
            # Take existing value over computed aggregation
            return existing_value, computed_value


# Cache of sorted lists
cached_sorts = {}


# @cached(cache={}, key=lambda cache_key, list: cache_key)
def sort_something(cache_key, list_to_sort):
    return list_to_sort
    # c = cached_sorts.get(cache_key, [])
    # if len(c) != len(list_to_sort):
    #     c = sorted(list_to_sort)
    #     cached_sorts[cache_key] = c
    # else:
    #     print("Cache hit")
    # return c


class MissingValueResolutionPolicy(Enum):
    UseZero = 0
    Invalidate = 1

    @staticmethod
    def get_key():
        return "NISSolverMissingValueResolutionPolicy"


class ConflictResolutionAlgorithm:
    def __init__(self, computation_sources_priority_list: List[ComputationSource],
                 aggregation_conflict_policy: AggregationConflictResolutionPolicy):
        self.computation_sources_priority_list = computation_sources_priority_list
        self.aggregation_conflict_policy = aggregation_conflict_policy

    def resolve(self, value1: FloatComputedTuple, value2: FloatComputedTuple) \
            -> Tuple[FloatComputedTuple, FloatComputedTuple]:
        assert value1.computation_source != value2.computation_source, \
            f"The computation sources of both conflicting values cannot be the same: {value1.computation_source}"

        # Both values have been computed
        if value1.computation_source is not None and value2.computation_source is not None:
            value1_position = self.computation_sources_priority_list.index(value1.computation_source)
            value2_position = self.computation_sources_priority_list.index(value2.computation_source)

            if value1_position < value2_position:
                return value1, value2
            else:
                return value2, value1

        # One of the values has been computed by aggregation while the other is an observation
        if ifnull(value1.computation_source, value2.computation_source) in (ComputationSource.PartOfAggregation,
                                                                            ComputationSource.InterfaceTypeAggregation):
            if value1.computation_source is None:
                # value2 is computed value, value1 is existing value
                return self.aggregation_conflict_policy.resolve(value2, value1)
            else:
                # value1 is computed value, value2 is existing value
                return self.aggregation_conflict_policy.resolve(value1, value2)

        # One of the values has been computed by a non-aggregation computation while the other is an observation
        else:
            # Return the observation first
            if value1.computation_source is None:
                return value1, value2
            else:
                return value2, value1


def get_computation_sources_priority_list(s: str) -> List[ComputationSource]:
    """ Convert a list of strings into a list of valid ComputationSource values and also check its validity
        according to the parameter "NISSolverComputationSourcesPriority".
        The input list should contain all values of ComputationSource, without duplicates, in any order.
    """
    identifiers = parse_string_as_simple_ident_list(s)
    sources: List[ComputationSource] = []

    if identifiers is None:
        raise SolvingException(f"The priority list of computation sources is invalid: {identifiers}")

    for identifier in identifiers:
        try:
            sources.append(ComputationSource[identifier])
        except KeyError:
            raise SolvingException(f"The priority list of computation sources have an invalid value: {identifier}")

    if len(sources) != len(ComputationSource):
        raise SolvingException(
            f"The priority list of computation sources should have "
            f"length {len(ComputationSource)} but has length: {len(sources)}")

    if len(sources) != len(set(sources)):
        raise SolvingException(f"The priority list of computation sources cannot have duplicated values: {sources}")

    return sources


ObservationListType = List[Tuple[Optional[Union[float, AstType]], FactorQuantitativeObservation]]
TimeObservationsType = Dict[str, ObservationListType]
InterfaceNodeAstDict = Dict[InterfaceNode, Tuple[AstType, FactorQuantitativeObservation]]


class ProcessingItem(NamedTuple):
    source: ComputationSource
    hierarchy: Union[InterfaceNodeHierarchy, ComputationGraph]
    results: NodeFloatComputedDict
    partof_weights: Optional[ProcessorsRelationWeights] = None


def get_evaluated_observations_by_time(prd: PartialRetrievalDictionary) -> TimeObservationsType:
    """
        Get all interface observations (intensive or extensive) by time.
        Also resolve expressions without parameters. Cannot resolve expressions depending only on global parameters
        because some of them can be overridden by scenario parameters.

        Each evaluated observation is stored as a tuple:
        * First: the evaluated result as a float or the prepared AST
        * Second: the observation.

    :param prd: the dictionary of global objects
    :return: a time dictionary with a list of observation on each time
    """
    observations: TimeObservationsType = defaultdict(list)
    state = State()

    # Get all observations, grouped by time step (including generic time steps, like "Year" or "Month")
    for observation in find_quantitative_observations(prd, processor_instances_only=True):
        # Try to evaluate the observation value
        value, ast, _, issues = evaluate_numeric_expression_with_parameters(observation.value, state)

        # Store: (Value, FactorQuantitativeObservation)
        time = observation.attributes["time"].lower()
        observations[time].append((ifnull(value, ast), observation))

    if len(observations) == 0:
        return {}

    # Check all time periods are consistent. All should be Year or Month, but not both.
    time_period_type = check_type_consistency_from_all_time_periods(list(observations.keys()))
    assert (time_period_type in ["year", "month"])

    # Remove generic period type and insert it into all specific periods. E.g. "Year" into "2010", "2011" and "2012"
    if time_period_type in observations:
        # Generic monthly ("Month") or annual ("Year") data
        periodic_observations = observations.pop(time_period_type)

        for time in observations:
            observations[time] += periodic_observations

    return observations


def check_type_consistency_from_all_time_periods(time_periods: List[str]) -> str:
    """ Check if all time periods are of the same period type, either Year or Month:
         - general "Year" & specific year (YYYY)
         - general "Month" & specific month (mm-YYYY or YYYY-mm, separator can be any of "-/")

    :param time_periods:
    :return:
    """
    # Based on the first element we will check the rest of elements
    period = next(iter(time_periods))

    if period == "year" or is_year(period):
        period_type = "year"
        period_check = is_year
    elif period == "month" or is_month(period):
        period_type = "month"
        period_check = is_month
    else:
        raise SolvingException(f"Found invalid period type '{period}'")

    for time_period in time_periods:
        if time_period != period_type and not period_check(time_period):
            raise SolvingException(
                f"Found period type inconsistency: accepting '{period_type}' but found '{time_period}'")

    return period_type


def split_observations_by_relativeness(observations_by_time: TimeObservationsType) \
        -> Tuple[TimeObservationsType, TimeObservationsType]:
    observations_by_time_norelative = defaultdict(list)
    observations_by_time_relative = defaultdict(list)
    for time, observations in observations_by_time.items():
        for value, obs in observations:
            if obs.is_relative:
                observations_by_time_relative[time].append((value, obs))
            else:
                observations_by_time_norelative[time].append((value, obs))

    return observations_by_time_norelative, observations_by_time_relative


def create_interface_edges(edges: List[Tuple[Factor, Factor, Optional[str]]]) \
        -> Generator[Tuple[InterfaceNode, InterfaceNode, Dict], None, None]:
    for src, dst, weight in edges:
        src_node = InterfaceNode(src)
        dst_node = InterfaceNode(dst)
        if "Archetype" in [src.processor.instance_or_archetype, dst.processor.instance_or_archetype]:
            logging.warning(f"Excluding relation from '{src_node}' to '{dst_node}' because of Archetype processor")
        else:
            yield src_node, dst_node, dict(weight=weight)


def resolve_weight_expressions(graph_list: List[nx.DiGraph], state: State, raise_error=False) -> None:
    for graph in graph_list:
        for u, v, data in graph.edges(data=True):
            expression = data["weight"]
            if expression is not None and not isinstance(expression, FloatExp):
                value, ast, params, issues = evaluate_numeric_expression_with_parameters(expression, state)
                if raise_error and value is None:
                    raise SolvingException(
                        f"Cannot evaluate expression "
                        f"'{expression}' for weight from interface '{u}' to interface '{v}'. Params: {params}. "
                        f"Issues: {', '.join(issues)}"
                    )

                data["weight"] = ast if value is None else FloatExp(value, None, str(expression))


def resolve_partof_weight_expressions(weights: ProcessorsRelationWeights, state: State, raise_error=False) \
        -> ProcessorsRelationWeights:
    evaluated_weights: ProcessorsRelationWeights = {}

    for (parent, child), expression in weights.items():
        if expression is not None and not isinstance(expression, FloatExp):
            value, ast, params, issues = evaluate_numeric_expression_with_parameters(expression, state)
            if raise_error and value is None:
                raise SolvingException(
                    f"Cannot evaluate expression '{expression}' for weight from child processor '{parent}' "
                    f"to parent processor '{child}'. Params: {params}. Issues: {', '.join(issues)}"
                )

            evaluated_weights[(parent, child)] = ast if value is None else FloatExp(value, None, str(expression))

    return evaluated_weights


def create_scale_change_relations_and_update_flow_relations(relations_flow: nx.DiGraph, registry,
                                                            interface_nodes: Set[InterfaceNode]) -> nx.DiGraph:
    relations_scale_change = nx.DiGraph()

    edges = [(r.source_factor, r.target_factor, r.back_factor, r.weight, r.scale_change_weight)
             for r in registry.get(FactorsRelationDirectedFlowObservation.partial_key())
             if r.scale_change_weight is not None or r.back_factor is not None]

    for src, dst, bck, weight, scale_change_weight in edges:

        source_node = InterfaceNode(src)
        dest_node = InterfaceNode(dst)
        back_node = InterfaceNode(bck) if bck else None

        if "Archetype" in [src.processor.instance_or_archetype,
                           dst.processor.instance_or_archetype,
                           bck.processor.instance_or_archetype if bck else None]:
            logging.warning(f"Excluding relation from '{source_node}' to '{dest_node}' "
                            f"and back to '{back_node}' because of Archetype processor")
            continue

        hidden_node = InterfaceNode(src.taxon,
                                    processor_name=f"{src.processor.full_hierarchy_name}-"
                                                   f"{dst.processor.full_hierarchy_name}",
                                    orientation="Input/Output")

        relations_flow.add_edge(source_node, hidden_node, weight=weight)
        relations_scale_change.add_edge(hidden_node, dest_node, weight=scale_change_weight, add_reverse_weight="yes")
        if back_node:
            relations_scale_change.add_edge(hidden_node, back_node, weight=scale_change_weight,
                                            add_reverse_weight="yes")

        relations_scale_change.nodes[hidden_node]["add_split"] = "yes"

        real_dest_node = InterfaceNode(source_node.interface_type, dest_node.processor,
                                       orientation="Input" if source_node.orientation.lower() == "output" else "Output")

        # Check if synthetic interface is equal to an existing one
        matching_interfaces = [n for n in interface_nodes if n.alternate_key == real_dest_node.alternate_key]
        if len(matching_interfaces) == 1:
            real_dest_node = matching_interfaces[0]
        else:
            interface_nodes.add(real_dest_node)

        if relations_flow.has_edge(source_node, real_dest_node):
            # weight = relations_flow[source_node][real_dest_node]['weight']
            relations_flow.remove_edge(source_node, real_dest_node)
            # relations_flow.add_edge(source_node, hidden_node, weight=weight)  # This "weight" should be the same
            relations_flow.add_edge(hidden_node, real_dest_node, weight=1.0)

    return relations_scale_change


def convert_params_to_extended_interface_names(params: Set[str], obs: FactorQuantitativeObservation, registry) \
        -> Tuple[Dict[str, str], List[str], List[str]]:
    extended_interface_names: Dict[str, str] = create_dictionary()
    unresolved_params: List[str] = []
    issues: List[str] = []

    for param in params:
        # Check if param is valid interface name
        interfaces: Sequence[Factor] = registry.get(Factor.partial_key(processor=obs.factor.processor, name=param))
        if len(interfaces) == 1:
            node = InterfaceNode(interfaces[0])
            extended_interface_names[param] = node.name
        else:
            unresolved_params.append(param)
            if len(interfaces) > 1:
                issues.append(f"Multiple interfaces with name '{param}' exist, "
                              f"rename them to uniquely identify the desired one.")
            else:  # len(interfaces) == 0
                issues.append(f"No global parameter or interface exist with name '{param}'.")

    return extended_interface_names, unresolved_params, issues


def replace_ast_variable_parts(ast: AstType, variable_conversion: Dict[str, str]) -> AstType:
    new_ast = deepcopy(ast)

    for term in new_ast['terms']:
        if term['type'] == 'h_var':
            variable = term['parts'][0]
            if variable in variable_conversion:
                term['parts'] = [variable_conversion[variable]]

    return new_ast


def resolve_observations_with_only_values_and_parameters(parameters: CaseInsensitiveDict,
                                                         state: State, observations: ObservationListType,
                                                         observers_priority_list: Optional[List[str]], registry) \
        -> Tuple[NodeFloatComputedDict, InterfaceNodeAstDict, List[str]]:
    resolved_observations: NodeFloatComputedDict = {}
    unresolved_observations_with_interfaces: InterfaceNodeAstDict = {}

    p_set = set([k.lower() for k in parameters.keys()])
    exceptions = []
    for expression, obs in observations:
        interface_params: Dict[str, str] = {}
        obs_new_value: Optional[str] = None
        value, ast, not_found_vars, issues = evaluate_numeric_expression_with_parameters(expression, state)
        # If it is an expression (not a literal value), if there is an interface with same name as a parameter, assume
        # the interface, but if there is not one, take the parameter value

        if isinstance(expression, dict):
            # Parameter?  Interface?  Interface with Value?  BEHAVIOR
            # ----------  ----------  ---------------------  --------
            # Yes         Yes          Yes                   Interface value has precedence over parameter value <- NEW!
            # Yes         Yes          No                    Interface has precedence, but error because value is None
            # Yes         No           Yes                   NOT POSSIBLE
            # Yes         No           No                    Parameter value taken
            # No          Yes          Yes                   Interface value taken
            # No          Yes          No                    Interface defined, but error because value is None
            # No          No           Yes                   NOT POSSIBLE
            # No          No           No                    Total disaster!

            _, p = ast_evaluator(expression, State(), None, issues)
            p = set([p_.lower() for p_ in p]).intersection(p_set)
            if len(p) > 0:  # There is at least one parameter in the expression
                params_overridden_by_interface = set()
                # Get the parameter names from the expression and see if there is an interface with the same name
                for p_ in p:
                    interfaces: Sequence[Factor] = registry.get(
                        Factor.partial_key(processor=obs.factor.processor, name=p_))
                    if len(interfaces) == 1:
                        # print(f"Assuming interface '{p_}' for observation '{expression}'")
                        params_overridden_by_interface.add(p_)
                        value = None
                        ast = expression
                not_found_vars.update(params_overridden_by_interface)

        if value is None:
            interface_params, not_found_vars, issues = convert_params_to_extended_interface_names(not_found_vars, obs, registry)
            if interface_params and not issues:
                ast = replace_ast_variable_parts(ast, interface_params)
                obs_new_value = replace_string_from_dictionary(obs.value, interface_params)
            else:
                interface_params = not_found_vars
                exceptions.append(f"Cannot evaluate expression '{ast_to_string(expression)}' for observation at interface '{obs.factor.name}', processor '{obs.factor.processor.full_hierarchy_name}'. "
                                  f"Params: {not_found_vars}. Issues: {', '.join(issues)}")
                # raise SolvingException(
                #     f"Cannot evaluate expression '{expression}' for observation at interface '{obs.factor.name}'. "
                #     f"Params: {not_found_vars}. Issues: {', '.join(issues)}"
                # )

        # Get observer name
        observer_name = obs.observer.name if obs.observer else None

        if observer_name and observers_priority_list and observer_name not in observers_priority_list:
            raise SolvingException(
                f"The specified observer '{observer_name}' for the interface '{obs.factor.name}' has not been included "
                f"in the observers' priority list: {observers_priority_list}"
            )

        # Create node from the interface
        node = InterfaceNode(obs.factor)

        if node in resolved_observations or \
                node in unresolved_observations_with_interfaces:
            previous_observer_name: str = resolved_observations[node].observer \
                if node in resolved_observations else unresolved_observations_with_interfaces[node][1].observer.name

            if observer_name is None and previous_observer_name is None:
                raise SolvingException(
                    f"Multiple observations exist for the same interface '{node.name}' without a specified observer."
                )
            elif not observers_priority_list:
                raise SolvingException(
                    f"Multiple observations exist for the same interface '{node.name}' but an observers' priority list "
                    f"has not been (correctly) defined: {observers_priority_list}"
                )
            elif not precedes_in_list(observers_priority_list, observer_name, previous_observer_name):
                # Ignore this observation because a higher priority observation has previously been set
                continue

        if interface_params:
            new_obs = copy(obs)  # deepcopy(obs) ?
            if obs_new_value is not None:
                new_obs.value = obs_new_value
            unresolved_observations_with_interfaces[node] = (ast, new_obs)
            resolved_observations.pop(node, None)
        else:
            resolved_observations[node] = FloatComputedTuple(FloatExp(value, node.name, obs_new_value),
                                                             Computed.No, observer_name)
            unresolved_observations_with_interfaces.pop(node, None)

    return resolved_observations, unresolved_observations_with_interfaces, exceptions


def resolve_observations_with_interfaces(
        state: State, existing_unresolved_observations: InterfaceNodeAstDict, existing_results: NodeFloatComputedDict) \
        -> Tuple[NodeFloatComputedDict, InterfaceNodeAstDict]:
    state.update({k.name: v.value.val for k, v in existing_results.items()})
    results: NodeFloatComputedDict = {}
    unresolved_observations: InterfaceNodeAstDict = {}

    for node, (ast, obs) in existing_unresolved_observations.items():
        value, ast, params, issues = evaluate_numeric_expression_with_parameters(ast, state)
        if value is not None:
            observer_name = obs.observer.name if obs.observer else None
            results[node] = FloatComputedTuple(FloatExp(value, node.name, str(obs.value)), Computed.No, observer_name)
        else:
            unresolved_observations[node] = (ast, obs)

    return results, unresolved_observations


def compute_flow_and_scale_computation_graphs(state: State,
                                              relative_observations: ObservationListType,
                                              relations_flow: nx.DiGraph,
                                              relations_scale: nx.DiGraph,
                                              relations_scale_change: nx.DiGraph) \
        -> Tuple[ComputationGraph, ComputationGraph, ComputationGraph]:
    # Create a copy of the main relations structures that are modified with time-dependent values
    time_relations_flow = relations_flow.copy()
    time_relations_scale = relations_scale.copy()
    time_relations_scale_change = relations_scale_change.copy()

    # Add Processors internal -RelativeTo- relations (time dependent)
    # Transform relative observations into graph edges
    for expression, obs in relative_observations:
        time_relations_scale.add_edge(InterfaceNode(obs.relative_factor, obs.factor.processor),
                                      InterfaceNode(obs.factor),
                                      weight=expression)

    # Last pass to resolve weight expressions: expressions with parameters can be solved
    resolve_weight_expressions([time_relations_flow, time_relations_scale, time_relations_scale_change],
                               state, raise_error=True)

    # Create computation graphs
    comp_graph_flow = create_computation_graph_from_flows(time_relations_flow, time_relations_scale)
    comp_graph_flow.name = "Flow"
    comp_graph_scale = ComputationGraph(time_relations_scale, "Scale")
    comp_graph_scale_change = ComputationGraph(time_relations_scale_change, "Scale Change")

    return comp_graph_flow, comp_graph_scale, comp_graph_scale_change


def create_computation_graph_from_flows(relations_flow: nx.DiGraph,
                                        relations_scale: Optional[nx.DiGraph] = None) -> ComputationGraph:
    flow_graph = FlowGraph(relations_flow)
    comp_graph_flow, issues = flow_graph.get_computation_graph(relations_scale)

    for issue in issues:
        logging.debug(issue)

    error_issues = [e.description for e in issues if e.itype == IType.ERROR]
    if len(error_issues) > 0:
        raise SolvingException(f"The computation graph cannot be generated. Issues: {', '.join(error_issues)}")

    return comp_graph_flow


def compute_interfacetype_hierarchies(registry: PartialRetrievalDictionary,
                                      interface_nodes: Set[InterfaceNode]) -> InterfaceNodeHierarchy:
    def compute(parent: FactorType):
        """ Recursive computation for a depth-first search """
        if parent in visited_interface_types:
            return

        for child in interface_types_parent_relations[parent]:
            if child in interface_types_parent_relations:
                compute(child)

            for processor in {p.processor for p in interface_nodes}:  # type: Processor
                for orientation in orientations:
                    child_interfaces = interfaces_dict.get(
                        (processor.full_hierarchy_name, child.name, orientation), {})
                    if child_interfaces:
                        parent_interface = InterfaceNode(parent, processor, orientation)

                        interfaces = interfaces_dict.get(parent_interface.alternate_key, [])
                        if len(interfaces) == 1:
                            # Replace "ProcessorName:InterfaceTypeName:Orientation" -> "ProcessorName:InterfaceName"
                            parent_interface = interfaces[0]
                        else:  # len(interfaces) != 1
                            interface_nodes.add(parent_interface)
                            interfaces_dict.setdefault(parent_interface.alternate_key, []).append(parent_interface)

                        hierarchies.setdefault(parent_interface, set()).update(child_interfaces)

        visited_interface_types.add(parent)

    # Get all different existing interface types with children interface types
    interface_types_parent_relations: Dict[FactorType, Set[FactorType]] = \
        {ft: ft.get_children() for ft in registry.get(FactorType.partial_key()) if len(ft.get_children()) > 0}

    # Get the list of interfaces for each combination
    interfaces_dict: Dict[Tuple[str, str, str], List[InterfaceNode]] = {}
    for interface in interface_nodes:
        interfaces_dict.setdefault(interface.alternate_key, []).append(interface)

    hierarchies: InterfaceNodeHierarchy = {}
    visited_interface_types: Set[FactorType] = set()

    # Iterate over all relations
    for parent_interface_type in interface_types_parent_relations:
        compute(parent_interface_type)

    return hierarchies


def compute_partof_hierarchies(registry: PartialRetrievalDictionary,
                               interface_nodes: Set[InterfaceNode]) \
        -> Tuple[InterfaceNodeHierarchy, ProcessorsRelationWeights]:
    """
    Obtain the information to enable computing part-of hierarchies:
      - hierarchies: a dictionary(Parent InterfaceNode, set(Children InterfaceNodes)
      - weights:     a dictionary( (Parent InterfaceNode, Child InterfaceNode), weight)

    :param registry:
    :param interface_nodes: Set of ALL InterfaceNodes until the execution of this function
    :return: See function description
    """

    def compute(parent: Processor):
        """ Recursive computation for a depth-first search """
        if parent in visited_processors:
            return

        for child in processor_partof_children[parent]:
            if child in processor_partof_children:  # If child is also parent, compute first
                compute(child)

            child_interface_nodes: List[InterfaceNode] = processor_interface_nodes.get(child, [])

            if child_interface_nodes and (parent, child) in behave_as_differences:
                # Remove interfaces from child that don't belong to behave_as_processor
                child_interface_nodes = [n for n in child_interface_nodes if
                                         n.interface_name not in behave_as_differences[(parent, child)]]

            # Add the interfaces of the child processor to the parent processor
            for child_interface_node in child_interface_nodes:
                parent_interface_node = InterfaceNode(child_interface_node.interface, parent,
                                                      child_interface_node.orientation)
                interface_node = name_ifacenodes.get(parent_interface_node.name)
                # Search parent_interface in Set of existing interface_nodes, it can have same name but different
                # combination of (type, orientation). For example, we define:
                # - interface "ChildProcessor:Water" as (BlueWater, Input)
                # - interface "ParentProcessor:Water" as (BlueWater, Output)
                # In this case aggregating child interface results in a conflict in parent
                if not interface_node:
                    # Does not exist, just add it
                    name_ifacenodes[parent_interface_node.name] = parent_interface_node
                    interface_nodes.add(parent_interface_node)
                    processor_interface_nodes.setdefault(parent_interface_node.processor, []).append(
                        parent_interface_node)
                else:
                    pass
                    # # Exists, check the above possible issue: "conflict in interface orientation"
                    # if True:
                    #     if (interface_node.type, interface_node.orientation) != \
                    #             (parent_interface_node.type, parent_interface_node.orientation):
                    #         raise SolvingException(
                    #             f"Interface '{parent_interface_node}' already defined with type <{parent_interface_node.type}> and orientation <{parent_interface_node.orientation}> "
                    #             f"is being redefined with type <{interface_node.type}> and orientation <{interface_node.orientation}> when aggregating processor "
                    #             f"<{child_interface_node.processor_name}> to parent processor <{parent_interface_node.processor_name}>. Rename either the child or the parent interface.")
                    # else:
                    #     for interface_node in interface_nodes:
                    #         if interface_node == parent_interface_node:
                    #             if (interface_node.type, interface_node.orientation) != \
                    #                     (parent_interface_node.type, parent_interface_node.orientation):
                    #                 raise SolvingException(
                    #                     f"Interface '{parent_interface_node}' already defined with type <{parent_interface_node.type}> and orientation <{parent_interface_node.orientation}> "
                    #                     f"is being redefined with type <{interface_node.type}> and orientation <{interface_node.orientation}> when aggregating processor "
                    #                     f"<{child_interface_node.processor_name}> to parent processor <{parent_interface_node.processor_name}>. Rename either the child or the parent interface.")
                    #             break

                hierarchies.setdefault(parent_interface_node, set()).add(child_interface_node)

        visited_processors.add(parent)

    # Get the -PartOf- processor relations of the system
    processor_partof_children, weights, behave_as_dependencies = get_processor_partof_relations(registry)

    name_ifacenodes = {}

    # Get the list of interfaces of each processor
    processor_interface_nodes: Dict[Processor, List[InterfaceNode]] = {}
    for node in interface_nodes:
        processor_interface_nodes.setdefault(node.processor, []).append(node)
        name_ifacenodes[node.name] = node

    check_behave_as_dependencies(behave_as_dependencies, processor_interface_nodes)
    # For processors marked to BehaveAs, interfaces not to be aggregated
    behave_as_differences = compute_behave_as_differences(behave_as_dependencies, processor_interface_nodes)

    hierarchies: InterfaceNodeHierarchy = {}
    visited_processors: Set[Processor] = set()

    # Iterate over all relations
    # TODO maybe filter to only those parent processors in the top level? We have "visited_processors" but
    for parent_processor in processor_partof_children:
        compute(parent_processor)

    return hierarchies, weights


def check_behave_as_dependencies(
        behave_as_dependencies: Dict[Tuple[Processor, Processor], Processor],
        processor_interface_nodes: Dict[Processor, List[InterfaceNode]]):
    """ Make a check for the 'BehaveAs' property that can be defined in the 'BareProcessors' command.
        If defined, all the interfaces of the 'BehaveAs' processor must be specified in the selected processor."""
    for (_, child_processor), behave_as_processor in behave_as_dependencies.items():
        child_interfaces = {n.interface_name for n in processor_interface_nodes[child_processor]}
        behave_as_interfaces = {n.interface_name for n in processor_interface_nodes[behave_as_processor]}
        difference_interfaces = behave_as_interfaces.difference(child_interfaces)
        if difference_interfaces:
            raise SolvingException(
                f"The processor '{child_processor.name}' cannot behave as processor '{behave_as_processor.name}' on "
                f"aggregations because it doesn't have these interfaces: {difference_interfaces}")


def compute_behave_as_differences(
        behave_as_dependencies: Dict[Tuple[Processor, Processor], Processor],
        processor_interface_nodes: Dict[Processor, List[InterfaceNode]]) -> Dict[Tuple[Processor, Processor], Set[str]]:
    """ Compute the difference in interfaces from a processor and the associated BehaveAs processor """
    behave_as_differences: Dict[Tuple[Processor, Processor], Set[str]] = {}
    for (parent_processor, child_processor), behave_as_processor in behave_as_dependencies.items():
        child_interfaces = {n.interface_name for n in processor_interface_nodes[child_processor]}
        behave_as_interfaces = {n.interface_name for n in processor_interface_nodes[behave_as_processor]}
        behave_as_differences[(parent_processor, child_processor)] = child_interfaces.difference(behave_as_interfaces)

    return behave_as_differences


def get_processor_partof_relations(glb_idx: PartialRetrievalDictionary) \
        -> Tuple[
            Dict[Processor, Set[Processor]], ProcessorsRelationWeights, Dict[Tuple[Processor, Processor], Processor]]:
    """ Get in a dictionary the -PartOf- processor relations, ignoring Archetype processors """
    relations: Dict[Processor, Set[Processor]] = {}
    weights: ProcessorsRelationWeights = {}
    behave_as_dependencies: Dict[Tuple[Processor, Processor], Processor] = {}

    for parent, child, weight, behave_as_processor in \
            [(r.parent_processor, r.child_processor, r.weight, r.behave_as)
             for r in glb_idx.get(ProcessorsRelationPartOfObservation.partial_key())
             if "Archetype" not in [r.parent_processor.instance_or_archetype, r.child_processor.instance_or_archetype]]:
        relations.setdefault(parent, set()).add(child)
        weights[(parent, child)] = weight
        if behave_as_processor:
            behave_as_dependencies[(parent, child)] = behave_as_processor

    return relations, weights, behave_as_dependencies


def compute_hierarchy_graph_results(
        graph: ComputationGraph,
        computed_values: NodeFloatComputedDict,
        prev_computed_values: NodeFloatComputedDict,
        conflict_resolution_algorithm: ConflictResolutionAlgorithm,
        computation_source: ComputationSource) \
        -> Tuple[NodeFloatComputedDict, NodeFloatComputedDict, NodeFloatComputedDict]:
    """
    Compute nodes in a graph hierarchy and also mark conflicts with existing values (params)

    :param graph: hierarchy as a graph of interface nodes
    :param computed_values: all nodes with a known value
    :param prev_computed_values: all nodes that have been previously computed with same computation source
    :param conflict_resolution_algorithm: algorithm for resolution of conflicts
    :param computation_source: source of computation
    :return: a dict with all values computed now and in previous calls, a dict with conflicted values
             that have been taken, a dict with conflicted values that have been dismissed
    """

    def solve_inputs(inputs: List[FloatExp.ValueWeightPair], split: bool) -> Optional[FloatExp]:
        input_values: List[FloatExp.ValueWeightPair] = []

        for n, weight in inputs:
            res_backward: Optional[FloatExp] = compute_node(n)

            # If node 'n' is a 'split' only one result is needed to compute the result
            if split:
                if res_backward is not None:
                    return res_backward * weight
            else:
                if res_backward is not None and weight is not None:
                    input_values.append((res_backward, weight))
                else:
                    return None

        return FloatExp.compute_weighted_addition(input_values)

    def compute_node(node: InterfaceNode) -> Optional[FloatExp]:
        # If the node has already been computed return the value
        if new_values.get(node) is not None:
            return new_values[node].value

        # We avoid graphs with cycles
        if node in pending_nodes:
            return None

        pending_nodes.append(node)

        # NOTE: it is convenient to pass a sorted list so Expression shows always the same result at string level
        #       "sort_something" has the purpose of keeping this behavior while contributing to a speedup.
        sum_children = solve_inputs(sort_something((node, "f"), graph.direct_inputs(node)),
                                    graph.get_reverse_node_split(node))

        if sum_children is None:
            sum_children = solve_inputs(sort_something((node, "r"), graph.reverse_inputs(node)),
                                        graph.get_direct_node_split(node))

        float_value = computed_values.get(node)
        if sum_children is not None:
            # New value has been computed
            sum_children.name = node.name
            new_computed_value = FloatComputedTuple(sum_children, Computed.Yes, computation_source=computation_source)

            if float_value is not None:
                # Conflict here: applies strategy
                taken_conflicts[node], dismissed_conflicts[node] = \
                    conflict_resolution_algorithm.resolve(new_computed_value, float_value)

                new_values[node] = taken_conflicts[node]
                return_value = taken_conflicts[node].value
                # if new_computed_value.computation_source != float_value.computation_source:
                #     # Conflict here: applies strategy
                #     taken_conflicts[node], dismissed_conflicts[node] = \
                #         conflict_resolution_algorithm.resolve(new_computed_value, float_value)
                #
                #     new_values[node] = taken_conflicts[node]
                #     return_value = taken_conflicts[node].value
                # else:
                #     return_value = float_value.value
                #     print(f"WARNING: same source? HG. {node.name}")
            else:
                new_values[node] = new_computed_value
                return_value = new_computed_value.value
        else:
            # No value got from children, try to search in "params"
            return_value = float_value.value if float_value is not None else None
            # if float_value is not None:
            #     new_values[node] = float_value
            #     return_value = float_value.value
            # else:
            #     return_value = None

        return return_value

    new_values: NodeFloatComputedDict = {**prev_computed_values}  # All computed aggregations
    taken_conflicts: NodeFloatComputedDict = {}  # Taken values on conflicting nodes
    dismissed_conflicts: NodeFloatComputedDict = {}  # Dismissed values on conflicting nodes

    for parent_node in graph.nodes:
        pending_nodes: List[InterfaceNode] = []
        compute_node(parent_node)

    return new_values, taken_conflicts, dismissed_conflicts


def compute_hierarchy_aggregate_results(
        tree: InterfaceNodeHierarchy,
        computed_values: NodeFloatComputedDict,
        prev_computed_values: NodeFloatComputedDict,
        conflict_resolution_algorithm: ConflictResolutionAlgorithm,
        missing_values_policy: MissingValueResolutionPolicy,
        computation_source: ComputationSource,
        processors_relation_weights: ProcessorsRelationWeights = None) \
        -> Tuple[NodeFloatComputedDict, NodeFloatComputedDict, NodeFloatComputedDict]:
    """
    Compute aggregations of nodes in a hierarchy and also mark conflicts with existing values (params)

    :param tree: dictionary representing a hierarchy as a tree of interface nodes in the form [parent, set(child)]
    :param computed_values: all nodes with a known value
    :param prev_computed_values: all nodes that have been previously computed by aggregation
    :param conflict_resolution_algorithm: algorithm for resolution of conflicts
    :param missing_values_policy: policy for missing values when aggregating children
    :param computation_source: source of computation
    :param processors_relation_weights: weights to use computing aggregation for processor hierarchies
    :return: a dict with all values computed by aggregation now and in previous calls, a dict with conflicted values
             that have been taken, a dict with conflicted values that have been dismissed
    """

    def compute_node(node: InterfaceNode) -> Optional[FloatExp]:
        # If the node has already been computed return the value
        if new_values.get(node) is not None:
            return new_values[node].value

        # Make a depth-first search
        return_value: Optional[FloatExp]
        children_values: List[FloatExp.ValueWeightPair] = []
        invalidate_sum_children: bool = False
        sum_children: Optional[FloatExp] = None

        # Try to get the sum from children, if any
        for child in sorted(tree.get(node, {})):
            child_value = compute_node(child)
            if child_value is not None:
                weight: FloatExp = None if processors_relation_weights is None \
                    else processors_relation_weights[(node.processor, child.processor)]

                children_values.append((child_value, weight))
            elif missing_values_policy == MissingValueResolutionPolicy.Invalidate:
                # Invalidate current children computation and stop evaluating following children
                invalidate_sum_children = True
                break

        if not invalidate_sum_children:
            sum_children = FloatExp.compute_weighted_addition(children_values)

        float_value = computed_values.get(node)
        if sum_children is not None:
            # New value has been computed
            sum_children.name = node.name
            new_computed_value = FloatComputedTuple(sum_children, Computed.Yes, computation_source=computation_source)

            if float_value is not None:
                # Conflict here: applies strategy
                taken_conflicts[node], dismissed_conflicts[node] = \
                    conflict_resolution_algorithm.resolve(new_computed_value, float_value)

                new_values[node] = taken_conflicts[node]
                return_value = taken_conflicts[node].value
                # if new_computed_value.computation_source != float_value.computation_source:
                #     # Conflict here: applies strategy
                #     taken_conflicts[node], dismissed_conflicts[node] = \
                #         conflict_resolution_algorithm.resolve(new_computed_value, float_value)
                #
                #     new_values[node] = taken_conflicts[node]
                #     return_value = taken_conflicts[node].value
                # else:
                #     return_value = float_value.value
                #     print(f"WARNING: same source? AR. {node.name}")
            else:
                new_values[node] = new_computed_value
                return_value = new_computed_value.value
        else:
            # No value got from children, try to search in "params"
            return_value = float_value.value if float_value is not None else None

        return return_value

    new_values: NodeFloatComputedDict = {**prev_computed_values}  # All computed aggregations
    taken_conflicts: NodeFloatComputedDict = {}  # Taken values on conflicting nodes
    dismissed_conflicts: NodeFloatComputedDict = {}  # Dismissed values on conflicting nodes

    for parent_node in tree:
        compute_node(parent_node)

    return new_values, taken_conflicts, dismissed_conflicts


def init_processor_full_names(registry: PartialRetrievalDictionary):
    for processor in registry.get(Processor.partial_key()):
        processor.full_hierarchy_name = processor.full_hierarchy_names(registry)[0]


# ##########################################
# ## MAIN ENTRY POINT ######################
# ##########################################
def flow_graph_solver(global_parameters: List[Parameter], problem_statement: ProblemStatement,
                      global_state: State, dynamic_scenario: bool) -> List[Issue]:
    """
    A solver using the graph composed by the interfaces and the relationships (flows, part-of, scale, change-of-scale and relative-to).

    :param global_parameters: Parameters including the default value (if defined)
    :param problem_statement: ProblemStatement object, with scenarios (parameters changing the default)
                              and parameters for the solver
    :param global_state:      All variables available: object model, registry, datasets (inputs and outputs), ...
    :param dynamic_scenario:  If "True" store results in datasets separated from "fixed" scenarios.
                              Also "problem_statement" MUST have only one scenario with the parameters.
    :return: List of Issues
    """
    try:
        issues: List[Issue] = []
        glb_idx, _, _, _, _ = get_case_study_registry_objects(global_state)
        init_processor_full_names(glb_idx)

        # Get all the observations, parsed and evaluated, split into Absolute and Relative, then grouped-by Time Step
        time_absolute_observations, time_relative_observations = \
            split_observations_by_relativeness(get_evaluated_observations_by_time(glb_idx))

        if len(time_absolute_observations) == 0:
            return [Issue(IType.WARNING, f"No absolute observations have been found. The solver has nothing to solve.")]

        # Get all the interfaces of the model, and construct a set of InterfaceNodes
        interface_nodes: Set[InterfaceNode] = {InterfaceNode(i) for i in glb_idx.get(Factor.partial_key())}

        # Get hierarchies of processors and update interfaces to compute
        _ = compute_partof_hierarchies(glb_idx, interface_nodes)
        partof_hierarchies: InterfaceNodeHierarchy = _[0]
        partof_weights: ProcessorsRelationWeights = _[1]

        # Get hierarchies of interface types and update interfaces to compute
        interfacetype_hierarchies: InterfaceNodeHierarchy = compute_interfacetype_hierarchies(glb_idx, interface_nodes)

        _ = compute_flow_and_scale_relation_graphs(glb_idx, interface_nodes)
        relations_flow: nx.DiGraph = _[0]
        relations_scale: nx.DiGraph = _[1]
        relations_scale_change: nx.DiGraph = _[2]

        total_results: ResultDict = {}

        # SCENARIOS - Outermost loop of the solver
        for scenario_name, scenario_params in problem_statement.scenarios.items():  # type: str, Dict[str, Any]
            logging.debug(f"********************* SCENARIO: {scenario_name}")
            params: CaseInsensitiveDict[str, Any] = evaluate_parameters_for_scenario(global_parameters, scenario_params)
            scenario_state = State(params)
            scenario_partof_weights: ProcessorsRelationWeights = \
                resolve_partof_weight_expressions(partof_weights, scenario_state, raise_error=True)

            # Get scenario parameters
            observers_priority_list = parse_string_as_simple_ident_list(
                scenario_state.get('NISSolverObserversPriority'))
            missing_value_policy = MissingValueResolutionPolicy[
                scenario_state.get(MissingValueResolutionPolicy.get_key())]
            conflict_resolution_algorithm = ConflictResolutionAlgorithm(
                get_computation_sources_priority_list(scenario_state.get('NISSolverComputationSourcesPriority')),
                AggregationConflictResolutionPolicy[scenario_state.get(AggregationConflictResolutionPolicy.get_key())]
            )

            missing_value_policies: List[MissingValueResolutionPolicy] = [MissingValueResolutionPolicy.Invalidate]
            if missing_value_policy == MissingValueResolutionPolicy.UseZero:
                missing_value_policies.append(MissingValueResolutionPolicy.UseZero)

            # TIME STEPS - Second loop
            for time_period, absolute_observations in time_absolute_observations.items():
                logging.debug(f"********************* TIME PERIOD: {time_period}")

                total_taken_results: NodeFloatComputedDict = {}
                total_dismissed_results: NodeFloatComputedDict = {}

                try:
                    _ = compute_flow_and_scale_computation_graphs(scenario_state,
                                                                  time_relative_observations[time_period],
                                                                  relations_flow,
                                                                  relations_scale,
                                                                  relations_scale_change)
                    comp_graph_flow: ComputationGraph = _[0]
                    comp_graph_scale: ComputationGraph = _[1]
                    comp_graph_scale_change: ComputationGraph = _[2]

                    _ = resolve_observations_with_only_values_and_parameters(
                            params, scenario_state, absolute_observations, observers_priority_list, glb_idx)
                    results: NodeFloatComputedDict = _[0]
                    unresolved_observations_with_interfaces: InterfaceNodeAstDict = _[1]
                    f_issues = _[2]
                    issues.extend([Issue(IType.ERROR, f) for f in f_issues])

                    # Initializations
                    iteration_number = 1

                    processing_items = [
                        ProcessingItem(ComputationSource.Flow, comp_graph_flow, {}),
                        ProcessingItem(ComputationSource.Scale, comp_graph_scale, {}),
                        ProcessingItem(ComputationSource.ScaleChange, comp_graph_scale_change, {}),
                        ProcessingItem(ComputationSource.InterfaceTypeAggregation, interfacetype_hierarchies, {}),
                        ProcessingItem(ComputationSource.PartOfAggregation, partof_hierarchies, {},
                                       scenario_partof_weights)
                    ]

                    # ITERATIVE SOLVING, for a pair (Scenario, Time step)

                    # We first iterate with policy MissingValueResolutionPolicy.Invalidate trying to get as many results
                    # we can without assuming zero for missing values.
                    # Second, if specified in paramater "NISSolverMissingValueResolutionPolicy" we try to get further
                    # results with policy MissingValueResolutionPolicy.UseZero.
                    for missing_value_policy in missing_value_policies:
                        previous_len_results = len(results) - 1

                        # Iterate while there are new results
                        while len(results) > previous_len_results:
                            logging.debug(f"********************* Solving iteration: {iteration_number}")
                            previous_len_results = len(results)

                            for pi in processing_items:

                                # Core of the solver
                                if pi.source.is_aggregation():
                                    _ = compute_hierarchy_aggregate_results(
                                        pi.hierarchy,
                                        results,
                                        pi.results,
                                        conflict_resolution_algorithm,
                                        missing_value_policy,
                                        pi.source,
                                        pi.partof_weights)
                                else:
                                    _ = compute_hierarchy_graph_results(
                                        pi.hierarchy,
                                        results,
                                        pi.results,
                                        conflict_resolution_algorithm,
                                        pi.source)

                                new_results: NodeFloatComputedDict = _[0]
                                taken_results: NodeFloatComputedDict = _[1]
                                dismissed_results: NodeFloatComputedDict = _[2]

                                pi.results.update(new_results)
                                results.update(new_results)
                                total_taken_results.update(taken_results)
                                total_dismissed_results.update(dismissed_results)

                            if unresolved_observations_with_interfaces:
                                _ = resolve_observations_with_interfaces(
                                        scenario_state, unresolved_observations_with_interfaces, results)
                                new_results: NodeFloatComputedDict = _[0]
                                unresolved_observations_with_interfaces: NodeFloatComputedDict = _[1]
                                results.update(new_results)

                            iteration_number += 1

                    if unresolved_observations_with_interfaces:
                        issues.append(Issue(IType.WARNING, f"Scenario '{scenario_name}' - period '{time_period}'."
                                                           f"The following observations could not be evaluated: "
                                                           f"{[k for k in unresolved_observations_with_interfaces.keys()]}"))

                    issues.extend(check_unresolved_nodes_in_computation_graphs(
                        [comp_graph_flow, comp_graph_scale, comp_graph_scale_change], results, scenario_name,
                        time_period))

                    current_results: ResultDict = {}
                    result_key = ResultKey(scenario_name, time_period, Scope.Total)

                    # Filter out conflicted results from TOTAL results
                    current_results[result_key] = {k: v for k, v in results.items() if k not in total_taken_results}

                    if total_taken_results:
                        current_results[result_key._replace(conflict=ConflictResolution.Taken)] = total_taken_results
                        current_results[
                            result_key._replace(conflict=ConflictResolution.Dismissed)] = total_dismissed_results

                    hierarchical_structures = [
                        HierarchicalNodeStructure.from_flow_computation_graph(comp_graph_flow, True),
                        HierarchicalNodeStructure.from_partof_aggregation(partof_hierarchies, scenario_partof_weights),
                        HierarchicalNodeStructure.from_interfacetype_aggregation(interfacetype_hierarchies)]

                    additional_hierarchical_structure = HierarchicalNodeStructure.from_flow_computation_graph(
                        comp_graph_flow, False)

                    internal_results, external_results = \
                        compute_internal_external_results(results, hierarchical_structures,
                                                          additional_hierarchical_structure)

                    current_results[result_key._replace(scope=Scope.Internal)] = internal_results
                    current_results[result_key._replace(scope=Scope.External)] = external_results

                    total_results.update(current_results)

                except SolvingException as e:
                    return [Issue(IType.ERROR, f"Scenario '{scenario_name}' - period '{time_period}'. {e.args[0]}")]
            # End: Time step loop
        # End: Scenario loop

        # Prepare all output information (store it in the global state, variable "datasets")
        #  - Indicators are calculated inside
        export_solver_data(global_state, total_results, dynamic_scenario, global_parameters, problem_statement)

        return issues
    except SolvingException as e:
        traceback.print_exc()  # Print the Exception to std output
        return [Issue(IType.ERROR, e.args[0])]


def mark_observations_and_scales_as_internal_results(
        results: NodeFloatComputedDict, internal_results: NodeFloatComputedDict) -> NoReturn:
    for node, value in results.items():
        if (value.computed == Computed.No) or \
                (value.computation_source and value.computation_source == ComputationSource.Scale):
            internal_results[node] = deepcopy(value)


class HierarchicalNodeStructure:
    def __init__(self, structure: Union[ComputationGraph, InterfaceNodeHierarchy],
                 computation_source: ComputationSource,
                 weights: Optional[ProcessorsRelationWeights] = None,
                 direct: Optional[bool] = None):
        assert (isinstance(structure, ComputationGraph) or isinstance(structure, Dict))
        self.structure = structure
        self.computation_source = computation_source
        self.weights = weights
        self.direct = direct

    @classmethod
    def from_partof_aggregation(cls, structure: InterfaceNodeHierarchy,
                                weights: ProcessorsRelationWeights) -> 'HierarchicalNodeStructure':
        return cls(structure, ComputationSource.PartOfAggregation, weights)

    @classmethod
    def from_interfacetype_aggregation(cls, structure: InterfaceNodeHierarchy) -> 'HierarchicalNodeStructure':
        return cls(structure, ComputationSource.InterfaceTypeAggregation)

    @classmethod
    def from_flow_computation_graph(cls, structure: ComputationGraph,
                                    direct: Optional[bool]) -> 'HierarchicalNodeStructure':
        return cls(structure, ComputationSource.Flow, direct=direct)

    def __iter__(self):
        if isinstance(self.structure, ComputationGraph):
            return (n for n in self.structure.nodes)
        else:
            return (n for n in self.structure)

    def get_children(self, node: InterfaceNode) -> List[Tuple[InterfaceNode, Optional[FloatExp]]]:
        if isinstance(self.structure, ComputationGraph):
            if self.direct:
                return self.structure.direct_inputs(node)
            else:
                return self.structure.reverse_inputs(node)
        else:
            if node in self.structure:
                if self.weights:
                    return [(n, self.weights[(node.processor, n.processor)]) for n in self.structure[node]]
                else:
                    return [(n, None) for n in self.structure[node]]
            else:
                return []


def compute_internal_external_results(results: NodeFloatComputedDict, structures: List[HierarchicalNodeStructure],
                                      additional_structure: HierarchicalNodeStructure) \
        -> Tuple[NodeFloatComputedDict, NodeFloatComputedDict]:
    def compute_structures() -> int:
        unknown_nodes: Set[InterfaceNode] = set()
        for structure in structures:
            unknown_nodes |= compute_hierarchical_structure_internal_external_results(structure, results,
                                                                                      internal_results,
                                                                                      external_results)
        return len(unknown_nodes)

    internal_results: NodeFloatComputedDict = {}
    external_results: NodeFloatComputedDict = {}

    mark_observations_and_scales_as_internal_results(results, internal_results)

    len_unknown = len(results)
    prev_len_unknown = len_unknown + 1
    while len_unknown and len_unknown < prev_len_unknown:
        prev_len_unknown = len_unknown

        len_unknown = compute_structures()

        # If resolution is stuck try to solve flow graph in reverse order
        if len_unknown and len_unknown == prev_len_unknown:
            compute_hierarchical_structure_internal_external_results(additional_structure, results,
                                                                     internal_results, external_results)
            len_unknown = compute_structures()

    return internal_results, external_results


def compute_hierarchical_structure_internal_external_results(
        structure: HierarchicalNodeStructure,
        results: NodeFloatComputedDict,
        internal_results: NodeFloatComputedDict, external_results: NodeFloatComputedDict) -> Set[InterfaceNode]:
    def compute(node: InterfaceNode) -> Tuple[Optional[FloatComputedTuple], Optional[FloatComputedTuple]]:
        if node not in internal_results and node not in external_results:
            if not structure.get_children(node):
                unknown_nodes.add(node)
                return None, None
            else:
                internal_addends: List[FloatExp.ValueWeightPair] = []
                external_addends: List[FloatExp.ValueWeightPair] = []

                for child_node, weight in sorted(structure.get_children(node)):
                    if child_node in results:
                        child_value = deepcopy(results[child_node])
                        same_system = node.system == child_node.system and node.subsystem.is_same_scope(
                            child_node.subsystem)
                        if same_system:
                            child_internal_value, child_external_value = compute(child_node)

                            if not child_internal_value and not child_external_value:
                                unknown_nodes.add(node)
                                return None, None

                            if child_internal_value:
                                child_internal_value.value.name = Scope.Internal.name + brackets(child_node.name)
                                internal_addends.append((child_internal_value.value, weight))

                            if child_external_value:
                                child_external_value.value.name = Scope.External.name + brackets(child_node.name)
                                external_addends.append((child_external_value.value, weight))
                        else:
                            external_addends.append((child_value.value, weight))

                if internal_addends:
                    scope_value = FloatExp.compute_weighted_addition(internal_addends)
                    scope_value.name = node.name
                    internal_results[node] = FloatComputedTuple(scope_value, Computed.Yes,
                                                                computation_source=structure.computation_source)

                if external_addends:
                    scope_value = FloatExp.compute_weighted_addition(external_addends)
                    scope_value.name = node.name
                    external_results[node] = FloatComputedTuple(scope_value, Computed.Yes,
                                                                computation_source=structure.computation_source)

        return internal_results.get(node), external_results.get(node)

    unknown_nodes: Set[InterfaceNode] = set()
    for node in structure:
        compute(node)

    return unknown_nodes


def check_unresolved_nodes_in_computation_graphs(computation_graphs: List[ComputationGraph],
                                                 resolved_nodes: NodeFloatComputedDict,
                                                 scenario_name: str, time_period: str) -> List[Issue]:
    issues: List[Issue] = []
    for comp_graph in computation_graphs:
        unresolved_nodes = [n for n in comp_graph.nodes if n not in resolved_nodes]
        if unresolved_nodes:
            issues.append(Issue(IType.WARNING,
                                f"Scenario '{scenario_name}' - period '{time_period}'. The following nodes in "
                                f"'{comp_graph.name}' graph could not be evaluated: {unresolved_nodes}"))
    return issues


def compute_flow_and_scale_relation_graphs(registry, interface_nodes: Set[InterfaceNode]) -> Tuple[nx.DiGraph, nx.DiGraph, nx.DiGraph]:
    # Compute Interfaces -Flow- relations (time independent)
    relations_flow = nx.DiGraph(
        incoming_graph_data=create_interface_edges(
            [(r.source_factor, r.target_factor, r.weight)
             for r in registry.get(FactorsRelationDirectedFlowObservation.partial_key())
             if r.scale_change_weight is None and r.back_factor is None]
        )
    )
    # Compute Processors -Scale- relations (time independent)
    relations_scale = nx.DiGraph(
        incoming_graph_data=create_interface_edges(
            [(r.origin, r.destination, r.quantity)
             for r in registry.get(FactorsRelationScaleObservation.partial_key())]
        )
    )

    # Compute Interfaces -Scale Change- relations (time independent). Also update Flow relations.
    relations_scale_change = create_scale_change_relations_and_update_flow_relations(relations_flow, registry,
                                                                                     interface_nodes)

    # First pass to resolve weight expressions: only expressions without parameters can be solved
    # NOT WORKING:
    # 1) the method ast_evaluator() doesn't get global Parameters,
    # 2) the expression for the FloatExp() is not correctly computed on a second pass
    # resolve_weight_expressions([relations_flow, relations_scale, relations_scale_change], state)

    return relations_flow, relations_scale, relations_scale_change
