from enum import Enum
from typing import Dict, List, Set, Any, Tuple, Union, Optional, NamedTuple, TypeVar

import networkx as nx

from nexinfosys.command_generators.parser_ast_evaluators import ast_evaluator
from nexinfosys.command_generators.parser_field_parsers import string_to_ast, expression_with_parameters
from nexinfosys.common.constants import SubsystemType, Scope
from nexinfosys.common.helper import istr, FloatExp, create_dictionary, strcmp
from nexinfosys.model_services import State
from nexinfosys.models.musiasem_concepts import Processor, Factor, FactorType, Parameter


Node = TypeVar('Node')  # Generic node type
Weight = FloatExp  # Type alias
Value = FloatExp  # Type alias


class SolvingException(Exception):
    pass


class EdgeType(Enum):
    """ Type of edge of a ComputationGraph """
    DIRECT = 0
    REVERSE = 1


class ConflictResolution(Enum):
    No = 1
    Taken = 2
    Dismissed = 3


class InterfaceNode:
    """
    Identifies an interface which value should be computed by the solver.
    An interface can be identified in two different ways:
    1. In the common case there is an interface declared in the Interfaces command. The interface is identified
       with "ProcessorName:InterfaceName".
    2. When we are aggregating by the interface type and there isn't a declared interface. The interface is
       identified with "ProcessorName:InterfaceTypeName:Orientation"
    """

    def __init__(self, interface_or_type: Union[Factor, FactorType], processor: Optional[Processor] = None,
                 orientation: Optional[str] = None, processor_name: Optional[str] = None):
        if isinstance(interface_or_type, Factor):
            self.interface: Optional[Factor] = interface_or_type
            self.interface_type = self.interface.taxon
            self.orientation: Optional[str] = orientation if orientation else self.interface.orientation
            self.interface_name: str = interface_or_type.name
            self.processor = processor if processor else self.interface.processor
        elif isinstance(interface_or_type, FactorType):
            self.interface: Optional[Factor] = None
            self.interface_type = interface_or_type
            self.orientation = orientation
            self.interface_name: str = ""
            self.processor = processor
        else:
            raise Exception(f"Invalid object type '{type(interface_or_type)}' for the first parameter. "
                            f"Valid object types are [Factor, FactorType].")

        self.processor_name: str = self.processor.full_hierarchy_name if self.processor else processor_name
        # Optimization: instead of a virtual property, a direct property
        if self.interface_name:
            self.name = ":".join(self.key)
        else:
            self.name = ":".join(self.alternate_key)

    @property
    def key(self) -> Tuple:
        return self.processor_name, self.interface_name

    @property
    def alternate_key(self) -> Tuple:
        return self.processor_name, self.type, self.orientation

    @property
    def full_key(self) -> Tuple:
        return self.processor_name, self.interface_name, self.type, self.orientation

    @staticmethod
    def full_key_labels() -> List[str]:
        return ["Processor", "Interface", "InterfaceType", "Orientation"]

    # @property
    # def name(self) -> str:
    #     if self.interface_name:
    #         return ":".join(self.key)
    #     else:
    #         return ":".join(self.alternate_key)

    @property
    def type(self) -> str:
        return self.interface_type.name

    @property
    def unit(self):
        return self.interface_type.unit

    @property
    def roegen_type(self):
        if self.interface and self.interface.roegen_type:
            if isinstance(self.interface.roegen_type, str):
                return self.interface.roegen_type
            else:
                return self.interface.roegen_type.name.title()
        elif self.interface_type and self.interface_type.roegen_type:
            if isinstance(self.interface_type.roegen_type, str):
                return self.interface_type.roegen_type
            else:
                return self.interface_type.roegen_type.name.title()
        else:
            return ""

    @property
    def sphere(self) -> Optional[str]:
        if self.interface and self.interface.sphere:
            return self.interface.sphere
        else:
            return self.interface_type.sphere

    @property
    def system(self) -> Optional[str]:
        return self.processor.processor_system if self.processor else None

    @property
    def subsystem(self) -> Optional[SubsystemType]:
        return SubsystemType.from_str(self.processor.subsystem_type) if self.processor else None

    def has_interface(self) -> bool:
        return self.interface is not None

    def no_interface_copy(self) -> "InterfaceNode":
        return InterfaceNode(self.interface_type, self.processor, self.orientation)

    def __str__(self):
        return self.name

    def __eq__(self, other):
        return self.name == other.name

    def __repr__(self):
        return istr(str(self))

    def __hash__(self):
        return hash(repr(self))

    def __lt__(self, other):
        return self.name < other.name


class Computed(Enum):
    No = 1
    Yes = 2


class ComputationSource(Enum):
    Flow = 1
    Scale = 2
    ScaleChange = 3
    PartOfAggregation = 4
    InterfaceTypeAggregation = 5

    def is_aggregation(self) -> bool:
        return self in (self.PartOfAggregation, self.InterfaceTypeAggregation)


class FloatComputedTuple(NamedTuple):
    value: FloatExp
    computed: Computed
    observer: str = None
    computation_source: ComputationSource = None


ProcessorsRelationWeights = Dict[Tuple[Processor, Processor], Any]
InterfaceNodeHierarchy = Dict[InterfaceNode, Set[InterfaceNode]]

NodeFloatDict = Dict[InterfaceNode, FloatExp]
NodeFloatComputedDict = Dict[InterfaceNode, FloatComputedTuple]


class ResultKey(NamedTuple):
    scenario: str
    period: str
    scope: Scope
    conflict: ConflictResolution = ConflictResolution.No

    def as_string_tuple(self) -> Tuple[str, str, str, str]:
        return self.scenario, self.period, self.scope.name, self.conflict.name


ResultDict = Dict[ResultKey, NodeFloatComputedDict]
AstType = Dict


def evaluate_numeric_expression_with_parameters(expression: Union[float, str, dict], state: State) \
        -> Tuple[Optional[float], Optional[AstType], Set, List[str]]:
    issues: List[Tuple[int, str]] = []
    ast: Optional[AstType] = None
    value: Optional[float] = None
    params = set()

    if expression is None:
        value = None

    elif isinstance(expression, float):
        value = expression

    elif isinstance(expression, dict):
        ast = expression
        value, params = ast_evaluator(ast, state, None, issues)
        if value is not None:
            ast = None

    elif isinstance(expression, str):
        try:
            value = float(expression)
        except ValueError:
            # print(f"{expression} before")
            ast = string_to_ast(expression_with_parameters, expression)
            # print(f"{expression} after")
            value, params = ast_evaluator(ast, state, None, issues)
            if value is not None:
                ast = None

    else:
        issues.append((3, f"Invalid type '{type(expression)}' for expression '{expression}'"))

    return value, ast, params, [i[1] for i in issues]


def get_circular_dependencies(parameters: Dict[str, Tuple[Any, list]]) -> list:
    # Graph, for evaluation of circular dependencies
    G = nx.DiGraph()
    for param, (_, dependencies) in parameters.items():
        for param2 in dependencies:
            G.add_edge(param2, param)  # We need "param2" to obtain "param"
    return list(nx.simple_cycles(G))


def evaluate_parameters_for_scenario(base_params: List[Parameter], scenario_params: Dict[str, str]):
    """
    Obtain a dictionary (parameter -> value), where parameter is a string and value is a literal: number, boolean,
    category or string.

    Start from the base parameters then overwrite with the values in the current scenario.

    Parameters may depend on other parameters, so this has to be considered before evaluation.
    No cycles are allowed in the dependencies, i.e., if P2 depends on P1, P1 cannot depend on P2.
    To analyze this, first expressions are evaluated, extracting which parameters appear in each of them. Then a graph
    is elaborated based on this information. Finally, an algorithm to find cycles is executed.

    :param base_params:
    :param scenario_params:
    :return:
    """
    # Create dictionary without evaluation
    result_params = create_dictionary()
    result_params.update({p.name: p.default_value for p in base_params if p.default_value is not None})
    param_types = create_dictionary()
    param_types.update({p.name: p.type for p in base_params})

    # Overwrite with scenario expressions or constants
    result_params.update(scenario_params)

    state = State()
    known_params = create_dictionary()
    unknown_params = create_dictionary()

    # Now, evaluate ALL expressions
    for param, expression in result_params.items():
        ptype = param_types[param]
        if strcmp(ptype, "Number") or strcmp(ptype, "Boolean"):
            value, ast, params, issues = evaluate_numeric_expression_with_parameters(expression, state)
            if value is None:  # It is not a constant, store the parameters on which this depends
                unknown_params[param] = (ast, set([istr(p) for p in params]))
            else:  # It is a constant, store it
                result_params[param] = value  # Overwrite
                known_params[param] = value
        elif strcmp(ptype, "Code") or strcmp(ptype, "String"):
            result_params[param] = expression
            known_params[param] = expression

    cycles = get_circular_dependencies(unknown_params)
    if len(cycles) > 0:
        raise SolvingException(
            f"Parameters cannot have circular dependencies. {len(cycles)} cycles were detected: {':: '.join(cycles)}")

    # Initialize state with known parameters
    state.update(known_params)

    known_params_set = set([istr(p) for p in known_params.keys()])
    # Loop until no new parameters can be evaluated
    previous_len_unknown_params = len(unknown_params) + 1
    while len(unknown_params) < previous_len_unknown_params:
        previous_len_unknown_params = len(unknown_params)

        for param in list(unknown_params):  # A list(...) is used because the dictionary can be modified inside
            ast, params = unknown_params[param]

            if params.issubset(known_params_set):
                value, _, _, issues = evaluate_numeric_expression_with_parameters(ast, state)
                if value is None:
                    raise SolvingException(
                        f"It should be possible to evaluate the parameter '{param}'. Issues: {', '.join(issues)}")
                else:
                    del unknown_params[param]
                    result_params[param] = value
                    # known_params[param] = value  # Not necessary
                    known_params_set.add(istr(param))
                    state.set(param, value)

    if len(unknown_params) > 0:
        raise SolvingException(f"Could not evaluate the following parameters: {', '.join(unknown_params)}")

    return result_params
