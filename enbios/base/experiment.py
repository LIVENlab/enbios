import csv
import math
import time
from datetime import timedelta
from pathlib import Path
from tempfile import gettempdir
from typing import Any, Optional, Union, Type
from typing import TypeVar

from enbios.base.adapters_aggregators.adapter import EnbiosAdapter
from enbios.base.adapters_aggregators.aggregator import EnbiosAggregator
from enbios.base.adapters_aggregators.builtin import BUILTIN_ADAPTERS, BUILTIN_AGGREGATORS
from enbios.base.experiment_io import resolve_input_files
from enbios.base.pydantic_experiment_validation import validate_experiment_data
from enbios.base.scenario import Scenario
from enbios.base.tree_operations import validate_experiment_hierarchy
from enbios.base.validation import (
    validate_run_scenario_setting,
    validate_scenarios,
    validate_scenario,
    validate_adapters,
    validate_aggregators,
)
from enbios.bw2.stacked_MultiLCA import StackedMultiLCA
from enbios.generic.enbios2_logging import get_logger
from enbios.generic.files import PathLike, ReadPath
from enbios.generic.tree.basic_tree import BasicTreeNode
from enbios.models.environment_model import Settings
from enbios.models.experiment_base_models import (
    ExperimentConfig,
    ExperimentScenarioData,
    NodeOutput,
)
from enbios.models.experiment_models import (
    ScenarioResultNodeData,
    TechTreeNodeData,
)

logger = get_logger(__name__)

# Define a TypeVar that is bound to EnbiosAdapter
EnbiosAdapterType = TypeVar("EnbiosAdapterType", bound=EnbiosAdapter)
EnbiosAggregatorType = TypeVar("EnbiosAggregatorType", bound=EnbiosAggregator)


class Experiment:
    DEFAULT_SCENARIO_NAME = "default scenario"

    def __init__(self, data: Optional[Union[dict, str]] = None):
        """
        Initialize the experiment
        :param data: dictionary or filename to load the data from
        """
        self.env_settings = Settings()
        if not data:
            data = self.env_settings.CONFIG_FILE
            if not isinstance(data, str):
                raise ValueError(
                    "Experiment config-file-path must be specified as environment "
                    "variable: 'CONFIG_FILE'"
                )
        if isinstance(data, str):
            config_file_path = ReadPath(data)
            data = config_file_path.read_data()

        self.raw_data = validate_experiment_data(data)
        # resolve hierarchy and scenarios filepaths if present
        resolve_input_files(self.raw_data)

        # load and validate adapters and aggregators
        self._adapters: dict[str, EnbiosAdapter]
        self.methods: list[str]
        self._adapters, self.methods = validate_adapters(self.raw_data.adapters)
        self._aggregators: dict[str, EnbiosAggregator] = validate_aggregators(
            self.raw_data.aggregators
        )

        # validate overall hierarchy
        self.hierarchy_root: BasicTreeNode[
            TechTreeNodeData
        ] = validate_experiment_hierarchy(self.raw_data.hierarchy)
        self._structural_nodes: dict[str, BasicTreeNode[TechTreeNodeData]] = {}
        # validate individual nodes based on their adapter/aggregator
        for node in self.hierarchy_root.iter_all_nodes():
            if node.is_leaf:
                self.get_node_adapter(node).validate_node(node.name, node.data.config)
                self._structural_nodes[node.name] = node
            else:
                self.get_node_aggregator(node).validate_node(node.name, node.data.config)

        def recursive_convert(
            node_: BasicTreeNode[TechTreeNodeData],
        ) -> BasicTreeNode[ScenarioResultNodeData]:
            output: Optional[NodeOutput] = None
            if node_.is_leaf:
                output = NodeOutput(
                    unit=self.get_node_adapter(node_).get_node_output_unit(node_.name),
                    magnitude=0,
                )
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
            self.raw_data.scenarios, Experiment.DEFAULT_SCENARIO_NAME, self
        )
        validate_run_scenario_setting(self.env_settings, self.config, self.scenario_names)

        self._lca: Optional[StackedMultiLCA] = None
        self._execution_time: float = float("NaN")

    def get_structural_node(self, name: str) -> BasicTreeNode[TechTreeNodeData]:
        """
        Get a node by either its name
        as it is defined in the experiment data
        :param name:
        :return: BasicTreeNode[TechTreeNodeData]
        """
        node = self._structural_nodes.get(name, None)
        if not node:
            raise ValueError(f"Node with name '{name}' not found")
        return node

    def get_node_adapter(
        self, node: BasicTreeNode[TechTreeNodeData]
    ) -> EnbiosAdapterType:
        """
        Get the adapter of a node in the experiment hierarchy

        :param node:
        :return:
        """
        return self._get_module_by_name_or_node_indicator(
            node.data.adapter, EnbiosAdapter, node.name
        )

    def get_adapter_by_name(self, name: str) -> EnbiosAdapterType:
        """
        Get an adapter by its name
        :param name:
        :return:
        """
        return self._get_module_by_name_or_node_indicator(name, EnbiosAdapter)

    def get_node_aggregator(
        self,
        node: Union[
            BasicTreeNode[ScenarioResultNodeData], BasicTreeNode[TechTreeNodeData]
        ],
    ) -> EnbiosAggregatorType:
        """
        Get the aggregator of a node
        :param node:
        :return:
        """
        return self._get_module_by_name_or_node_indicator(
            node.data.aggregator, EnbiosAggregator, node.name
        )

    def _get_module_by_name_or_node_indicator(
        self,
        name_or_indicator: str,
        module_type: Type[Union[EnbiosAdapter, EnbiosAggregator]],
        node_name: Optional[str] = None,
    ) -> Union[EnbiosAdapter, EnbiosAggregator]:
        modules: dict[str, Union[EnbiosAdapter, EnbiosAggregator]] = (
            self._adapters if module_type == EnbiosAdapter else self._aggregators
        )
        module = modules.get(name_or_indicator)
        if module:
            return module
        # also check, if the name instead of the indicator was used
        for module in modules.values():
            if module.node_indicator() == name_or_indicator:
                return module

        node_err = f"Node '{node_name}' specifies an " if node_name else ""
        raise ValueError(
            f"{node_err} unknown {module_type.__name__}: '{name_or_indicator}'. "
            + f"Available {module_type.__name__}s are: {[m.node_indicator() for m in modules.values()]}"
        )

    def get_scenario(self, scenario_name: str) -> Scenario:
        """
        Get a scenario by its name
        :param scenario_name:
        :return:
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
        :param scenario_name:
        :param results_as_dict:
        :return: The result_tree converted into a dict
        """
        return self.get_scenario(scenario_name).run(results_as_dict)

    def run(
        self, results_as_dict: bool = True
    ) -> dict[str, Union[BasicTreeNode[ScenarioResultNodeData], dict]]:
        """
        Run all scenarios. Returns a dict with the scenario name as key
        and the result_tree as value
        :return: dictionary scenario-name : result_tree
        """
        # methods = [m.id for m in self.methods.values()]
        # inventories: list[list[dict[Activity, float]]] = []

        if self.config.run_scenarios:
            run_scenarios = [self.get_scenario(s) for s in self.config.run_scenarios]
            logger.info(f"Running selected scenarios: {[s.name for s in run_scenarios]}")
        else:
            run_scenarios = self.scenarios

        results = {}
        start_time = time.time()
        for scenario in run_scenarios:
            scenario.reset_execution_time()
            results[scenario.name] = scenario.run(results_as_dict)
        self._execution_time = time.time() - start_time
        return results

    @property
    def execution_time(self) -> str:
        """
        Get the execution time of the experiment (or all its scenarios) in
        a readable format
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

    def results_to_csv(
        self,
        file_path: PathLike,
        scenario_name: Optional[str] = None,
        level_names: Optional[list[str]] = None,
        include_method_units: bool = True,
    ):
        """
        Turn the results into a csv file. If no scenario name is given,
        it will export all scenarios to the same file,
        :param file_path:
        :param scenario_name: If no scenario name is given, it will
        export all scenarios to the same file,
            with an additional column for the scenario alias
        :param level_names: (list of strings) If given, the results will be
        exported with the given level names
        :param include_method_units:  (Include the units of the methods in the header)
        :return:
        """
        if scenario_name:
            scenario = self.get_scenario(scenario_name)
            scenario.results_to_csv(
                file_path,
                level_names=level_names,
                include_method_units=include_method_units,
            )
            return
        else:
            header = []
            all_rows: list = []
            for scenario in self.scenarios:
                temp_file_name = gettempdir() + f"/temp_scenario_{scenario.name}.csv"
                scenario.results_to_csv(
                    temp_file_name,
                    level_names=level_names,
                    include_method_units=include_method_units,
                )
                rows = ReadPath(temp_file_name).read_data()
                rows[0]["scenario"] = scenario.name
                if not all_rows:
                    header = list(rows[0].keys())
                    header.remove("scenario")
                    header.insert(0, "scenario")
                all_rows.extend(rows)
                if (temp_file := Path(temp_file_name)).exists():
                    temp_file.unlink()
            with Path(file_path).open("w", newline="") as csvfile:
                writer = csv.DictWriter(csvfile, header)
                writer.writeheader()
                writer.writerows(all_rows)

    def result_to_dict(self, include_output: bool = True) -> list[dict[str, Any]]:
        """
        Get the results of all scenarios as a list of dictionaries as dictionaries
        :param include_output: Include the output of each node in the tree
        :return:
        """
        return [
            scenario.result_to_dict(include_output=include_output)
            for scenario in self.scenarios
        ]

    @property
    def config(self) -> ExperimentConfig:
        """
        get the config of the experiment
        :return:
        """
        return self.raw_data.config

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
        return list(self._structural_nodes.keys())

    @property
    def scenario_names(self) -> list[str]:
        """
        Get all scenario names
        :return: list of strings of the scenario names
        """
        return list([s.name for s in self.scenarios])

    @property
    def adapters(self) -> list[EnbiosAdapterType]:
        """
        Get all adapters in a list
        :return:
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
        :param scenario_config:
        :param result_as_dict:
        :return:
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
        :return:
        """
        node_rows: list[str] = []

        def print_node(node: BasicTreeNode[TechTreeNodeData], _):
            module_name: str
            if node.data.adapter:
                module_name = self.get_node_adapter(node).name()
            else:
                module_name = self.get_node_aggregator(node).name()
            node_rows.append(f"{' ' * node.level}{node.name} - {module_name}")

        self.hierarchy_root.recursive_apply(print_node, False, False, None)

        node_rows_str = "\n".join(node_rows)
        methods_str = "\n".join([f" {m}" for m in self.methods])
        return (
            f"Experiment: \n"
            f"Structural nodes: {len(self._structural_nodes)}\n"
            f"{node_rows_str}\n"
            f"Methods: {len(self.methods)}\n"
            f"{methods_str}\n"
            f"Hierarchy (depth): {self.hierarchy_root.depth}\n"
            f"Scenarios: {len(self.scenarios)}\n"
        )

    @staticmethod
    def get_module_definition(
        clazz: Union[EnbiosAdapter, EnbiosAggregator], details: bool = True
    ) -> dict[str, Any]:
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
        :return:
        """
        result = {}
        for name, clazz in BUILTIN_ADAPTERS.items():
            result[name] = Experiment.get_module_definition(clazz, details)
        return result

    @staticmethod
    def get_builtin_aggregators(details: bool = True) -> dict[str, dict[str, Any]]:
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

    def get_method_unit(self, method: str) -> str:
        adapter_indicator, method_name = "", ""
        if "." in method:
            assert (
                method in self.methods
            ), f"Method {method} missing. Candidates: {', '.join(self.methods)}"
            adapter_indicator, method_name = method.split(".")
        else:
            assert method in self.method_names
            found = False
            for m in self.methods:
                if m.split(".")[-1] == method:
                    adapter_indicator, method_name = m.split(".")
                    found = True
                    break
            if not found:
                raise ValueError(f"Method {method} not found")
        return self._get_module_by_name_or_node_indicator(
            adapter_indicator, EnbiosAdapter
        ).get_method_unit(method_name)

    @property
    def method_names(self) -> list[str]:
        return [m.split(".")[-1] for m in self.methods]
