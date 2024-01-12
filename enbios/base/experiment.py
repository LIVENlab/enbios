import csv
import math
import time
from collections import Counter
from datetime import timedelta
from pathlib import Path
from tempfile import gettempdir
from typing import Any, Optional, Union, Type
from typing import TypeVar

from enbios.base.adapters_aggregators.adapter import EnbiosAdapter
from enbios.base.adapters_aggregators.aggregator import EnbiosAggregator
from enbios.base.adapters_aggregators.builtin import BUILTIN_ADAPTERS, BUILTIN_AGGREGATORS
from enbios.base.adapters_aggregators.loader import load_adapter, load_aggregator
from enbios.base.experiment_io import resolve_input_files
from enbios.base.pydantic_experiment_validation import validate_experiment_data
from enbios.base.scenario import Scenario
from enbios.base.tree_operations import validate_experiment_hierarchy
from enbios.bw2.stacked_MultiLCA import StackedMultiLCA
from enbios.generic.enbios2_logging import get_logger
from enbios.generic.files import PathLike, ReadPath
from enbios.generic.tree.basic_tree import BasicTreeNode
from enbios.models.environment_model import Settings
from enbios.models.experiment_base_models import (
    ExperimentConfig,
    ExperimentScenarioData,
    ActivityOutput,
)
from enbios.models.experiment_models import (
    Activity_Outputs,
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
        resolve_input_files(self.raw_data)

        self._adapters: dict[str, EnbiosAdapter]
        self.methods: list[str]
        self._adapters, self.methods = self._validate_adapters()
        self._aggregators: dict[str, EnbiosAggregator] = self._validate_aggregators()

        self.hierarchy_root: BasicTreeNode[
            TechTreeNodeData
        ] = validate_experiment_hierarchy(self.raw_data.hierarchy)
        self._activities: dict[str, BasicTreeNode[TechTreeNodeData]] = {}

        for node in self.hierarchy_root.iter_all_nodes():
            if node.is_leaf:
                self._get_node_adapter(node).validate_activity(
                    node.name, node.data.config
                )
                self._activities[node.name] = node

        def recursive_convert(
            node_: BasicTreeNode[TechTreeNodeData],
        ) -> BasicTreeNode[ScenarioResultNodeData]:
            output: Optional[ActivityOutput] = None
            if node_.is_leaf:
                output = ActivityOutput(
                    unit=self._get_node_adapter(node_).get_activity_output_unit(
                        node_.name
                    ),
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

        self.scenarios: list[Scenario] = self._validate_scenarios()
        self._validate_run_scenario_setting()

        self._lca: Optional[StackedMultiLCA] = None
        self._execution_time: float = float("NaN")

    # def get_method_unit(self, method_name: str) -> str:
    #     adapter_name, method_name = method_name.split(".")
    #     return self._adapters[adapter_name].get_method_unit(method_name)

    def _validate_run_scenario_setting(self):
        """
        Validate a run environmental variable that is setting the scenario
        """
        if self.env_settings.RUN_SCENARIOS:
            if self.config.run_scenarios:
                logger.info(
                    "Environment variable 'RUN_SCENARIOS' is set "
                    "and overwriting experiment config."
                )
            self.config.run_scenarios = self.env_settings.RUN_SCENARIOS
        if self.config.run_scenarios:
            for scenario in self.config.run_scenarios:
                if scenario not in self.scenario_names:
                    raise ValueError(
                        f"Scenario '{scenario}' not found in experiment scenarios. "
                        f"Scenarios are: {self.scenario_names}"
                    )

    def _validate_adapters(self) -> tuple[dict[str:EnbiosAdapter], list[str]]:
        """
        Validate the adapters in this experiment data

        :return: adapter-dict and method names
        """
        adapters = []
        methods = []
        for adapter_def in self.raw_data.adapters:
            adapter = load_adapter(adapter_def)
            adapter.validate_definition(adapter_def)
            adapter.validate_config(adapter_def.config)
            adapters.append(adapter)
            adapter_methods = adapter.validate_methods(adapter_def.methods)
            # if any([m in self.methods for m in adapter_methods.keys()]):
            #     raise ValueError(
            #         f"Some Method(s) {adapter_methods.keys()} already defined in "
            #         f"another adapter"
            #     )
            methods.extend([f"{adapter.node_indicator()}.{m}" for m in adapter_methods])

        adapter_map = {adapter.name(): adapter for adapter in adapters}
        return adapter_map, methods

    def _validate_aggregators(self) -> dict[str, EnbiosAggregator]:
        """
        Validate the aggregators in this experiment data

        :return: a aggregator-name-Aggregator dict
        """
        aggregators = [load_aggregator(adapter) for adapter in self.raw_data.aggregators]

        aggregator_names = [a.name() for a in aggregators]
        for builtin_name, aggregator in BUILTIN_AGGREGATORS.items():
            if builtin_name not in aggregator_names:
                aggregators.append(aggregator())

        for aggregator in aggregators:
            aggregator.validate_config()

        return {aggregator.name(): aggregator for aggregator in aggregators}

    def _get_activity(self, name: str) -> BasicTreeNode[TechTreeNodeData]:
        """
        Get an activity by either its name
        as it is defined in the experiment data
        :param name:
        :return: ExtendedExperimentActivityData
        """
        activity = self._activities.get(name, None)
        if not activity:
            raise ValueError(f"Activity with name '{name}' not found")
        return activity

    def _get_node_adapter(
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

    def get_node_aggregator(
        self,
        node: Union[
            BasicTreeNode[ScenarioResultNodeData], BasicTreeNode[TechTreeNodeData]
        ],
    ) -> EnbiosAggregatorType:
        """
        Get the aggregator of a node
        :param node: node, either in some hierarchy
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

    def _get_activity_default_output(self, activity_name: str) -> float:
        """
        Get the default output of an activity (bottom node) by its name

        :param activity_name: name of the activity
        :return: magnitude value of the default output
        """
        # todo remove, since seems to be just called once
        activity = self._get_activity(activity_name)
        return self._get_node_adapter(activity).get_default_output_value(activity.name)

    def _get_activity_output_unit(self, activity_name: str) -> str:
        """
        Get the unit of the activity (bottom node) by its name
        :param activity_name:
        :return:
        """
        # todo : just used once
        activity = self._get_activity(activity_name)
        return self._get_node_adapter(activity).get_activity_output_unit(activity_name)

    def _validate_scenario(self, scenario_data: ExperimentScenarioData) -> Scenario:
        """
        Validate one scenario
        :param scenario_data:
        :return:
        """

        def validate_activities(scenario_: ExperimentScenarioData) -> Activity_Outputs:
            activities = scenario_.activities or {}
            result: dict[str, float] = {}

            for activity_name, activity_output in activities.items():
                activity = self._get_activity(activity_name)

                adapter = self._get_node_adapter(activity)

                if isinstance(activity_output, dict):
                    activity_output = ActivityOutput(**activity_output)
                result[activity_name] = adapter.validate_activity_output(
                    activity_name, activity_output
                )
            return result

        scenario_activities_outputs: Activity_Outputs = validate_activities(scenario_data)
        defined_activities = list(scenario_activities_outputs.keys())

        # fill up the missing activities with default values
        if not scenario_data.config.exclude_defaults:
            for activity_name in self._activities.keys():
                if activity_name not in defined_activities:
                    scenario_activities_outputs[
                        activity_name
                    ] = self._get_activity_default_output(activity_name)

        # todo shall we bring back. scenario specific methods??
        # resolved_methods: dict[str, ExperimentMethodPrepData] = {}
        # if _scenario.methods:
        #     for index_, method_ in enumerate(_scenario.methods):
        #         if isinstance(method_, str):
        #             global_method = self.methods.get(method_)
        #             assert global_method
        #             resolved_methods[global_method.name] = global_method

        return Scenario(
            experiment=self,  # type: ignore
            name=scenario_data.name,
            activities_outputs=scenario_activities_outputs,
            # methods={},
            config=scenario_data.config,
            result_tree=self.base_result_tree.copy(),
        )

    def _validate_scenarios(self) -> list[Scenario]:
        """
        :return:
        """

        scenarios: list[Scenario] = []

        # undefined scenarios. just one default scenario
        if not self.raw_data.scenarios:
            self.raw_data.scenarios = [
                ExperimentScenarioData(name=Experiment.DEFAULT_SCENARIO_NAME)
            ]

        # set names if not given
        for index, scenario_data in enumerate(self.raw_data.scenarios):
            scenario_data.name_factory(index)

        # check for name duplicates
        name_count = Counter([s.name for s in self.raw_data.scenarios])
        # get the scenarios that have the same name
        duplicate_names = [name for name, count in name_count.items() if count > 1]
        if duplicate_names:
            raise ValueError(f"Scenarios with the same name: {duplicate_names}")

        for index, scenario_data in enumerate(self.raw_data.scenarios):
            scenario = self._validate_scenario(scenario_data)
            scenarios.append(scenario)
            scenario.prepare_tree()
        return scenarios

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
        # TODO RUN AT ONCE...
        for scenario in run_scenarios:
            scenario.reset_execution_time()
            results[scenario.name] = scenario.run(results_as_dict)
        #     if scenario.methods:
        #         raise ValueError(
        #             f"Scenario cannot have individual methods. '{scenario.name}'"
        #         )
        #     inventory: list[dict[Activity, float]] = []
        #     for activity_name, act_out in scenario.activities_outputs.items():
        #         bw_activity = scenario.experiment.get_activity(
        #             activity_name.alias
        #         ).bw_activity
        #         inventory.append({bw_activity: act_out})
        #     inventories.append(inventory)

        # run experiment
        # start_time = time.time()
        # calculation_setup = BWCalculationSetup(
        #     "experiment", list(itertools.chain(*inventories)), methods
        # )

        # distribution_results = self.config.use_k_bw_distributions > 1
        # results: dict[str, BasicTreeNode[ScenarioResultNodeData]] = {}
        # for i in range(self.config.use_k_bw_distributions):
        #     raw_results = StackedMultiLCA(calculation_setup, distribution_results).results
        #     scenario_results = np.split(raw_results, len(run_scenarios))
        #     for index, scenario in enumerate(run_scenarios):
        #         results[scenario.alias] = scenario.set_results(
        #             scenario_results[index],
        #             distribution_results,
        #             i == self.config.use_k_bw_distributions - 1,
        #         )
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
        :param include_output: Include the output of each activity in the tree
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
            f"Activities: {len(self._activities)}\n"
            f"Methods: {len(self.methods)}\n"
            f"Hierarchy (depth): {self.hierarchy_root.depth}\n"
            f"Scenarios: {len(self.scenarios)}\n"
        )

    @property
    def activities_names(self) -> list[str]:
        return list(self._activities.keys())

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
        :return:
        """
        return list(self._adapters.values())

    def run_scenario_config(
        self, scenario_config: dict, result_as_dict: bool = True
    ) -> Union[BasicTreeNode[ScenarioResultNodeData], dict]:
        """
        Run a scenario from a config dictionary. Scenario will be validated and run. An
        :param scenario_config:
        :param result_as_dict:
        :return:
        """
        scenario_data = ExperimentScenarioData(**scenario_config)
        scenario_data.name_factory(len(self.scenarios))
        scenario = self._validate_scenario(scenario_data)
        scenario.prepare_tree()
        return scenario.run(result_as_dict)

    def info(self) -> str:
        """
        Information about the experiment
        :return:
        """
        activity_rows: list[str] = []

        def print_node(node: BasicTreeNode[TechTreeNodeData], _):
            module_name: str
            if node.data.adapter:
                module_name = self._get_node_adapter(node).name()
            else:
                module_name = self.get_node_aggregator(node).name()
            activity_rows.append(f"{' ' * node.level}{node.name} - {module_name}")

        self.hierarchy_root.recursive_apply(print_node, False, False, None)

        activity_rows_str = "\n".join(activity_rows)
        methods_str = "\n".join([f" {m}" for m in self.methods])
        return (
            f"Experiment: \n"
            f"Activities: {len(self._activities)}\n"
            f"{activity_rows_str}\n"
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
        assert method in self.methods
        adapter_indicator, method_name = method.split(".")
        return self._get_module_by_name_or_node_indicator(
            adapter_indicator, EnbiosAdapter
        ).get_method_unit(method_name)

    @property
    def method_names(self) -> list[str]:
        return [m.split(".")[-1] for m in self.methods]
