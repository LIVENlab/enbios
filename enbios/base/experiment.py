import csv
import json
import math
import time
from datetime import timedelta
from pathlib import Path
from tempfile import gettempdir
from typing import Any, Optional, Union, Type, cast, TypeVar

from python_mermaid.diagram import MermaidDiagram
from python_mermaid.link import Link
from python_mermaid.node import Node

from enbios.base.adapters_aggregators.adapter import EnbiosAdapter
from enbios.base.adapters_aggregators.aggregator import EnbiosAggregator
from enbios.base.adapters_aggregators.builtin import BUILTIN_ADAPTERS, BUILTIN_AGGREGATORS
from enbios.base.adapters_aggregators.node_module import EnbiosNodeModule
from enbios.base.experiment_io import resolve_input_files
from enbios.base.models import ExperimentConfig, NodeOutput, ExperimentScenarioData, ExperimentDataResolved, \
    TechTreeNodeData, ScenarioResultNodeData, Settings
from enbios.base.pydantic_experiment_validation import validate_experiment_data
from enbios.base.scenario import Scenario
from enbios.base.tree_operations import validate_experiment_hierarchy
from enbios.base.unit_registry import register_units, get_pint_units_file_path
from enbios.base.validation import (
    validate_run_scenario_setting,
    validate_scenarios,
    validate_scenario,
    validate_adapters,
    validate_aggregators,
)
from enbios.bw2.MultiLCA_util import BaseStackedMultiLCA
from enbios.generic.enbios2_logging import get_logger, EnbiosLogger
from enbios.generic.files import PathLike, ReadPath
from enbios.generic.tree.basic_tree import BasicTreeNode

logger = get_logger(__name__)

T = TypeVar("T", bound=EnbiosNodeModule)


class Experiment:
    DEFAULT_SCENARIO_NAME = "default scenario"

    def __init__(self, data_input: Optional[Union[dict, str, Path]] = None):
        """
        Initialize the experiment
        :param data_input: dictionary or filename to load the data from
        """
        self.env_settings = Settings()
        if not data_input:
            data_input = self.env_settings.CONFIG_FILE
            if not isinstance(data_input, str):
                raise ValueError(
                    "Experiment config-file-path must be specified as environment "
                    "variable: 'CONFIG_FILE'"
                )
        if isinstance(data_input, str) or isinstance(data_input, Path):
            config_file_path = ReadPath(data_input)
            raw_data = config_file_path.read_data()
        else:
            raw_data = data_input
        register_units()

        raw_experiment_config = validate_experiment_data(raw_data)
        # resolve hierarchy and scenarios filepaths if present
        self.resolved_raw_data: ExperimentDataResolved = resolve_input_files(
            raw_experiment_config
        )

        # load and validate adapters and aggregators
        adapters, methods = validate_adapters(raw_experiment_config.adapters)
        self._adapters: dict[str, EnbiosAdapter] = adapters
        self.methods: list[str] = methods
        self._aggregators: dict[str, EnbiosAggregator] = validate_aggregators(
            raw_experiment_config.aggregators
        )

        # validate overall hierarchy
        self.hierarchy_root: BasicTreeNode[
            TechTreeNodeData
        ] = validate_experiment_hierarchy(self.resolved_raw_data.hierarchy)
        self._structural_nodes: dict[str, BasicTreeNode[TechTreeNodeData]] = {}
        # validate individual nodes based on their adapter/aggregator
        for node in self.hierarchy_root.iter_all_nodes():
            self.get_node_module(node).validate_node(node.name, node.data.config)
            if node.is_leaf:
                self._structural_nodes[node.name] = node

        def recursive_convert(
                node_: BasicTreeNode[TechTreeNodeData],
        ) -> BasicTreeNode[ScenarioResultNodeData]:
            output: list[NodeOutput] = []
            return BasicTreeNode(
                name=node_.name,
                data=ScenarioResultNodeData(
                    output=output,
                    adapter=node_.data.adapter,
                    aggregator=node_.data.aggregator,
                ),
                children=[recursive_convert(child) for child in node_.children],
            )

        self.base_result_tree: BasicTreeNode[ScenarioResultNodeData] = recursive_convert(
            self.hierarchy_root
        )

        self.scenarios: list[Scenario] = validate_scenarios(
            self.resolved_raw_data.scenarios, Experiment.DEFAULT_SCENARIO_NAME, self
        )
        validate_run_scenario_setting(self.env_settings, self.config, self.scenario_names)

        self._lca: Optional[BaseStackedMultiLCA] = None
        self._execution_time: float = float("NaN")

    def get_node(self, name: str) -> BasicTreeNode[TechTreeNodeData]:
        """
        Get a node from the hierarchy by its name
        :param name: name of the node
        :return: All node-data
        """
        node = self.hierarchy_root.find_subnode_by_name(name)
        if not node:
            raise ValueError(f"Node with name '{name}' not found")
        return node

    def get_structural_node(self, name: str) -> BasicTreeNode[TechTreeNodeData]:
        """
        Get a node by either its name as it is defined in the experiment data.
        :param name: Name of the node (as defined in the experiment hierarchy)
        :return: All node-data
        """
        node = self._structural_nodes.get(name, None)
        if not node:
            raise ValueError(f"Node with name '{name}' not found")
        return node

    def get_node_module(
            self,
            node: Union[str, BasicTreeNode[TechTreeNodeData]],
            module_type: Optional[T] = EnbiosNodeModule,
    ) -> T:
        """
        Get the module of a node in the experiment hierarchy
        """
        if isinstance(node, str):
            node = self.hierarchy_root.find_subnode_by_name(node)
        try:
            if node.is_leaf:
                return self.get_module_by_name_or_node_indicator(
                    node.data.adapter, EnbiosAdapter
                )
            else:
                return self.get_module_by_name_or_node_indicator(
                    node.data.aggregator, EnbiosAggregator
                )
        except ValueError as e:
            raise ValueError(f"Node '{node.name}' has no valid adapter/aggregator: {e}")

    def get_adapter_by_name(self, name: str) -> EnbiosAdapter:
        """
        Get an adapter by its name
        :param name: name of the adapter
        :return: The adapter
        """
        return cast(
            EnbiosAdapter, self.get_module_by_name_or_node_indicator(name, EnbiosAdapter)
        )

    def get_module_by_name_or_node_indicator(
            self,
            name_or_indicator: str,
            module_type: Type[T],
    ) -> T:
        modules: dict[str, Union[EnbiosAdapter, EnbiosAggregator]] = cast(
            dict[str, Union[EnbiosAdapter, EnbiosAggregator]],
            (self._adapters if module_type == EnbiosAdapter else self._aggregators),
        )
        module = modules.get(name_or_indicator)
        if module:
            return module
        # also check, if the name instead of the indicator was used
        for module in modules.values():
            if module.node_indicator() == name_or_indicator:
                return module

        raise ValueError(f"Module '{name_or_indicator}' not found")

    def get_scenario(self, scenario_name: str) -> Scenario:
        """
        Get a scenario by its name
        :param scenario_name: The name of the scenario as defined in the config or (Experiment.DEFAULT_SCENARIO_NAME)
        :return: The scenario object
        """
        for scenario in self.scenarios:
            if scenario.name == scenario_name:
                return scenario
        raise ValueError(f"Scenario '{scenario_name}' not found")

    def run_scenario(
            self, scenario_name: str, results_as_dict: bool = True
    ) -> Union[BasicTreeNode[ScenarioResultNodeData], dict]:
        """
        Run a specific scenario
        :param scenario_name: Name of the scenario to run
        :param results_as_dict: If the result should be returned as a dict instead of a tree object
        :return: The result_tree (eventually converted into a dict)
        """
        return self.get_scenario(scenario_name).run(results_as_dict)

    def run(
            self, results_as_dict: bool = True
    ) -> dict[str, Union[BasicTreeNode[ScenarioResultNodeData], dict]]:
        """
        Run all scenarios. Returns a dict with the scenario name as key and the result_tree as value
        :param results_as_dict: If the result should be returned as a dict instead of a tree object
        :return: dictionary scenario-name : result_tree  (eventually converted into a dict)
        """
        if self.config.run_scenarios:
            run_scenarios = [self.get_scenario(s) for s in self.config.run_scenarios]
            logger.info(f"Running selected scenarios: {[s.name for s in run_scenarios]}")
        else:
            run_scenarios = self.scenarios

        results = {}
        start_time = time.time()
        for scenario in run_scenarios:
            results[scenario.name] = scenario.run(results_as_dict)
        self._execution_time = time.time() - start_time
        return results

    @property
    def execution_time(self) -> str:
        """
        Get the execution time of the experiment (or all its scenarios) in a readable format
        :return: execution time in the format HH:MM:SS
        """
        if not math.isnan(self._execution_time):
            return str(timedelta(seconds=int(self._execution_time)))
        else:
            any_scenario_run = False
            scenario_results = ""
            for scenario in self.scenarios:
                if not math.isnan(scenario.get_execution_time()):
                    any_scenario_run = True
                    scenario_results += f"{scenario.name}: {scenario.execution_time}\n"
            if any_scenario_run:
                return scenario_results
            else:
                return "not run"

    def _scenario_select(
            self, scenarios: Optional[Union[str, list[str]]] = None
    ) -> list[str]:
        single_scenario = isinstance(scenarios, str)
        if single_scenario:
            return [scenarios]
        elif not scenarios:
            return [s.name for s in self.scenarios]
        else:
            return scenarios

    def results_to_csv(
            self,
            file_path: PathLike,
            scenarios: Optional[Union[str, list[str]]] = None,
            level_names: Optional[list[str]] = None,
            include_method_units: bool = True,
            include_output: bool = True,
            flat_hierarchy: Optional[bool] = False,
            include_extras: Optional[bool] = True,
            repeat_parent_name: bool = False,
            alternative_hierarchy: Optional[dict] = None,
    ):
        """
        Turn the results into a csv file. If no scenario name is given,
        it will export all scenarios to the same file,
        :param file_path: File path to export to
        :param scenarios: string or list of strings. If no scenario name is given, it will export all scenarios
        to the same file,
        with an additional column for the scenario alias
        :param level_names: (list of strings) If given, the results will be exported with the given level names.
        This is only effective when flat_hierarchy is False.
        :param include_method_units:  (Include the units of the methods in the header)
        :param include_output: Include the output of all nodes (default: True)
        :param flat_hierarchy: If instead of representing each level of the hierarchy with its own column,
        we just indicate the node levels.
        :param include_extras: Include extras from adapters and aggregators in the results (default: True)
        :param repeat_parent_name: If True, the parent name will be repeated for each child node in the
        corresponding level column.  This is only effective when flat_hierarchy is False. (default: False)
        :param alternative_hierarchy: If given, the results will be recalculated using the given alternative hierarchy.
        In this alternative hierarchy, tho, already defined nodeds need no config and no adapter/aggregator.
        """
        scenario_names: list[str] = self._scenario_select(scenarios)
        single_scenario = len(scenario_names) == 1
        header = []
        all_rows: list = []
        for scenario_name in scenario_names:
            scenario = self.get_scenario(scenario_name)
            temp_file_name = gettempdir() + f"/temp_scenario_{scenario.name}.csv"
            scenario.results_to_csv(
                temp_file_name,
                level_names=level_names,
                include_method_units=include_method_units,
                include_output=include_output,
                flat_hierarchy=flat_hierarchy,
                repeat_parent_name=repeat_parent_name,
                include_extras=include_extras,
                alternative_hierarchy=alternative_hierarchy,
            )
            rows = ReadPath(temp_file_name).read_data()
            if not single_scenario:
                for row in rows:
                    row["scenario"] = scenario.name
            # if not all_rows:
            for row in rows:
                for k in row.keys():
                    if k not in header:
                        header.append(k)
            all_rows.extend(rows)
            if (temp_file := Path(temp_file_name)).exists():
                temp_file.unlink()
        # put the scenario header at the start
        if not single_scenario:
            header.remove("scenario")
            header.insert(0, "scenario")
        with Path(file_path).open("w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, header)
            writer.writeheader()
            writer.writerows(all_rows)

    def result_to_dict(
            self,
            scenarios: Optional[Union[str, list[str]]] = None,
            include_method_units: bool = True,
            include_output: bool = True,
            include_extras: Optional[bool] = True,
            alternative_hierarchy: Optional[dict] = None,
    ) -> list[dict[str, Any]]:
        """
        Get the results of all scenarios as a list of dictionaries as dictionaries
        :param scenarios: A selection of scenarios to export. If None, all scenarios will be exported.
        :param alternative_hierarchy: If given, the results will be recalculated using the given alternative hierarchy.
        In this alternative hierarchy, tho, already defined nodeds need no config and no adapter/aggregator.
        :param include_method_units: Include the units of the methods in the header (default: True)
        :param include_output: Include the output of each node in the tree (default: True)
        :param include_extras: Include extras from adapters and aggregators in the results (default: True)
        :return:
        """
        scenario_names = self._scenario_select(scenarios)
        return [
            self.get_scenario(scenario).result_to_dict(
                include_output=include_output,
                include_method_units=include_method_units,
                include_extras=include_extras,
                alternative_hierarchy=alternative_hierarchy,
            )
            for scenario in scenario_names
        ]

    @property
    def config(self) -> ExperimentConfig:
        """
        get the config of the experiment
        :return:
        """
        return self.resolved_raw_data.config

    def __repr__(self):
        return (
            f"Experiment: (call info() for details)\n"
            f"Structural nodes: {len(self._structural_nodes)}\n"
            f"Methods: {len(self.methods)}\n"
            f"Hierarchy (depth): {self.hierarchy_root.depth}\n"
            f"Scenarios: {len(self.scenarios)}\n"
        )

    @property
    def structural_nodes_names(self) -> list[str]:
        """
        Return the names of all structural nodes (bottom)
        :return: names of all structural nodes
        """
        return list(self._structural_nodes.keys())

    @property
    def scenario_names(self) -> list[str]:
        """
        Get all scenario names
        :return: list of strings of the scenario names
        """
        return list([s.name for s in self.scenarios])

    @property
    def adapters(self) -> list[EnbiosAdapter]:
        """
        Get all adapters in a list
        :return: A list of all adapters
        """
        return list(self._adapters.values())

    def run_scenario_config(
            self,
            scenario_config: dict,
            result_as_dict: bool = True,
            append_scenario: bool = True,
    ) -> Union[BasicTreeNode[ScenarioResultNodeData], dict]:
        """
        Run a scenario from a config dictionary. Scenario will be validated and run. An
        :param scenario_config: The scenario config as a dictionary (as it would be defined in the experiment config)
        :param result_as_dict: If True, the result will be returned as a dictionary. If False, the result will be
        returned as a BasicTreeNode.
        :param append_scenario: If True, the scenario will be appended to the experiment. If False, the scenario will
        not be appended.
        :return: The scenario result as a dictionary or a BasicTreeNode
        """
        scenario_data = ExperimentScenarioData(**scenario_config)
        scenario_data.name_factory(len(self.scenarios))

        extra_index = 1
        if scenario_data.name in self.scenario_names:
            scenario_data.name = scenario_data.name + f" ({extra_index})"
            extra_index += 1
        scenario = validate_scenario(scenario_data, self)
        scenario.prepare_tree()
        if append_scenario:
            self.scenarios.append(scenario)
        return scenario.run(result_as_dict)

    def info(self) -> str:
        """
        Information about the experiment
        :return: Generated information as a string
        """
        node_rows: list[str] = []

        def print_node(node: BasicTreeNode[TechTreeNodeData], _):
            module_name: str = self.get_node_module(node).name()
            node_rows.append(f"{' ' * node.level}{node.name} - {module_name}")

        self.hierarchy_root.recursive_apply(print_node, False, False, None)

        node_rows_str = "\n".join(node_rows)
        methods_str = "\n".join([f" {m}" for m in self.methods])

        scenarios_done = [scenario.has_run for scenario in self.scenarios]
        all_scenarios_run = all(scenarios_done)
        no_scenarios_run = not any(scenarios_done)

        run_status_str = (
            "all scenarios run"
            if all_scenarios_run
            else "no scenarios run"
            if no_scenarios_run
            else "some scenarios run"
        )

        return (
            f"Experiment: \n"
            f"Structural nodes: {len(self._structural_nodes)}\n"
            f"{node_rows_str}\n"
            f"Methods: {len(self.methods)}\n"
            f"{methods_str}\n"
            f"Hierarchy (depth): {self.hierarchy_root.depth}\n"
            f"Scenarios: {len(self.scenarios)}\n"
            f"{run_status_str}\n"
        )

    @staticmethod
    def get_module_definition(
            clazz: Union[
                Type[EnbiosAdapter], EnbiosAdapter, Type[EnbiosAggregator], EnbiosAggregator
            ],
            details: bool = True,
    ) -> dict[str, Any]:
        """
        Get the 'node_indicator' and schema of a module (adapter or aggregator)
        :param clazz: The class of the module (adapter or aggregator)
        :param details: If the whole schema should be returned (True) or just the node_indicator (False) (default: True)
        :return: returns a dictionary {node_indicator: <node_indicator>, config: <schema>}
        """
        result: dict = {
            "node_indicator": clazz.node_indicator(),
        }
        if details:
            result["config"] = clazz.get_config_schemas()
        return result

    @staticmethod
    def get_builtin_adapters(details: bool = True) -> dict[str, dict[str, Any]]:
        """
        Get the built-in adapters
        :param details: If the schema should be included or not (default: True)
        :return: all built-in adapters as a dictionary name: {node_indicator: <node_indicator>, config: <json-schema>}
        """
        result = {}
        for name, clazz in BUILTIN_ADAPTERS.items():
            result[name] = Experiment.get_module_definition(clazz, details)
        return result

    @staticmethod
    def get_builtin_aggregators(details: bool = True) -> dict[str, dict[str, Any]]:
        """
        Get the built-in aggregators
        :param details: If the schema should be included or not (default: True)
        :return: all built-in aggregators as a dictionary name: {node_indicator: <node_indicator>, config: <json-schema>}
        """
        result = {}
        for name, clazz in BUILTIN_AGGREGATORS.items():
            result[name] = Experiment.get_module_definition(clazz, details)
        return result

    def get_all_configs(
            self, include_all_builtin_configs: bool = True
    ) -> dict[str, dict[str, dict[str, Any]]]:
        """
        Result structure:
            ```json
            {
                "adapters": { <adapter_name>: <adapter_config>},
                "aggregators": { ... }
            }
            ```
        :param include_all_builtin_configs:
        :return: all configs
        """
        result = {
            "adapters": {
                name: Experiment.get_module_definition(adapter, True)
                for name, adapter in (
                        self._adapters
                        | (BUILTIN_ADAPTERS if include_all_builtin_configs else {})
                ).items()
            },
            "aggregators": {
                name: Experiment.get_module_definition(aggregator, True)
                for name, aggregator in (
                        self._aggregators
                        | (BUILTIN_AGGREGATORS if include_all_builtin_configs else {})
                ).items()
            },
        }

        return result

    @property
    def method_names(self) -> list[str]:
        """
        Names of all methods
        :return: a list of method names
        """
        return [m.split(".")[-1] for m in self.methods]

    def hierarchy2mermaid(self) -> str:
        """
        Convert the hierarchy to a mermaid graph diagram
        :return: a string representing in mermaid syntax
        """
        mm_nodes_map: dict[str, Node] = {}
        nodes: list[Node] = []
        links: list[Link] = []
        for node in self.hierarchy_root.iter_all_nodes():
            mm_node = Node(node.name)
            nodes.append(mm_node)
            mm_nodes_map[node.name] = mm_node
            for child in node.children:
                links.append(Link(mm_node, mm_nodes_map[child.name]))
        return str(MermaidDiagram(nodes=nodes, links=links, orientation="top-down"))

    def get_simplified_hierarchy(
            self, print_it: bool = False
    ) -> dict[str, Optional[dict[str, Any]]]:
        """
        Get the hierarchy as a dictionary, but in a simplified form, i.e. only the nodes with children are included.
        :param print_it: Print it to the console
        :return: A simplified dictionary of the hierarchy
        """

        def rec_nodes(node: BasicTreeNode) -> dict[str, Optional[dict[str, Any]]]:
            res = {}
            if node.children:
                for child in node.children:
                    res.update(rec_nodes(child))
                return {node.name: res}
            else:
                return {node.name: None}

        result = rec_nodes(self.hierarchy_root)
        if print_it:
            print(json.dumps(result, indent=2, ensure_ascii=False))
        return result

    @staticmethod
    def delete_pint_and_logging_file():
        """ "
        Deletes the pint unit file and the logging config file
        """
        pint_units_file_path = Path(get_pint_units_file_path())
        pint_units_file_path.unlink(True)
        logging_config_file_path = EnbiosLogger.get_logging_config_file_path()
        logging_config_file_path.unlink(True)
