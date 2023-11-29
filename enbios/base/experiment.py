import csv
import math
from datetime import timedelta
from pathlib import Path
from tempfile import gettempdir
from typing import Any, Optional, Union

from enbios.base.adapters_aggregators.adapter import EnbiosAdapter
from enbios.base.adapters_aggregators.aggregator import EnbiosAggregator, SumAggregator
from enbios.base.adapters_aggregators.loader import load_adapter, load_aggregator
from enbios.base.experiment_io import resolve_input_files
from enbios.base.pydantic_experiment_validation import validate_experiment_data
from enbios.base.scenario import Scenario
from enbios.bw2.stacked_MultiLCA import StackedMultiLCA
from enbios.generic.enbios2_logging import get_logger
from enbios.generic.files import PathLike, ReadPath
from enbios.generic.tree.basic_tree import BasicTreeNode
from enbios.models.experiment_models import (
    Activity_Outputs,
    ExperimentConfig,
    ExperimentData,
    ExperimentScenarioData,
    ScenarioResultNodeData,
    Settings,
    TechTreeNodeData,
    ExperimentHierarchyNodeData,
)

logger = get_logger(__name__)


class Experiment:
    DEFAULT_SCENARIO_NAME = "default scenario"

    def __init__(self, raw_data: Optional[Union[ExperimentData, dict, str]] = None):
        self.env_settings = Settings()
        if not raw_data:
            raw_data = self.env_settings.CONFIG_FILE
            if not isinstance(raw_data, str):
                raise ValueError(
                    "Experiment config-file-path must be specified as environment "
                    "variable: 'CONFIG_FILE'"
                )
        if isinstance(raw_data, str):
            config_file_path = ReadPath(raw_data)
            raw_data = config_file_path.read_data()
        if isinstance(raw_data, dict):
            input_data = validate_experiment_data(raw_data)
        else:
            input_data = raw_data
        # todo. look at how reading a hierarchy from a csv file or a json
        # creates an additional root node
        resolve_input_files(input_data)
        self.raw_data = validate_experiment_data(input_data.model_dump())

        self._adapters: dict[str, EnbiosAdapter] = self._validate_adapters()
        self._aggregators: dict[str, EnbiosAggregator] = self._validate_aggregators()

        # todo: weird problem with Pydantic; Using self.raw_data.hierarchy fails...
        self.hierarchy_root: BasicTreeNode[TechTreeNodeData] = self.validate_hierarchy(
            ExperimentHierarchyNodeData(**raw_data["hierarchy"])
        )

        self._activities: dict[str, BasicTreeNode[TechTreeNodeData]] = {
            n.name: n for n in self.hierarchy_root.iter_leaves()
        }

        for node in self.hierarchy_root.iter_all_nodes():
            if node.is_leaf:
                self.get_activity_adapter(node).validate_activity(
                    node.name, node.data.id, node.data.output, False
                )

        def recursive_convert(
            node_: BasicTreeNode[TechTreeNodeData],
        ) -> BasicTreeNode[ScenarioResultNodeData]:
            output: tuple[Optional[str], Optional[float]] = (None, None)
            if node_.is_leaf:
                output = (
                    self.get_activity_adapter(node_).get_activity_output_unit(node_.name),
                    0,
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
        self.base_result_tree.recursive_apply(
            Experiment.recursive_resolve_outputs,
            experiment=self,
            depth_first=True,
            cancel_parents_of=set(),
        )

        self.methods: list[str] = []
        for idx, adapter in enumerate(self.adapters):
            adapter_methods_config = self.raw_data.adapters[idx].methods
            adapter_methods = adapter.validate_methods(adapter_methods_config)
            # if any([m in self.methods for m in adapter_methods.keys()]):
            #     raise ValueError(
            #         f"Some Method(s) {adapter_methods.keys()} already defined in "
            #         f"another adapter"
            #     )
            self.methods.extend([f"{adapter.name}.{m}" for m in adapter_methods])

        self.scenarios: list[Scenario] = self._validate_scenarios()
        self._validate_run_scenario_setting()

        self._lca: Optional[StackedMultiLCA] = None
        self._execution_time: float = float("NaN")

    @staticmethod
    def recursive_resolve_outputs(
        node: BasicTreeNode[ScenarioResultNodeData], experiment: "Experiment", **kwargs
    ):
        # todo, does this takes default values when an activity is not defined
        #  in the scenario?
        if node.is_leaf:
            return
        cancel_parts_of: set = kwargs["cancel_parents_of"]
        if any(child.id in cancel_parts_of for child in node.children):
            node.set_data(ScenarioResultNodeData())
            cancel_parts_of.add(node.id)

        aggregator = experiment.get_node_aggregator(node.data.aggregator)
        valid = aggregator.validate_node_output(node)
        if not valid:
            cancel_parts_of.add(node.id)

    def get_method_unit(self, method_name: str) -> str:
        adapter_name, method_name = method_name.split(".")
        return self._adapters[adapter_name].get_method_unit(method_name)

    def _validate_run_scenario_setting(self):
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

    def _validate_adapters(self) -> dict[str:EnbiosAdapter]:
        adapters = [load_adapter(adapter) for adapter in self.raw_data.adapters]

        for idx, adapter in enumerate(adapters):
            adapter.validate_config(self.raw_data.adapters[idx].config)

        return {adapter.activity_indicator: adapter for adapter in adapters}

    def _validate_aggregators(self) -> dict[str, EnbiosAggregator]:
        aggregators = [
            load_aggregator(adapter) for adapter in self.raw_data.aggregators
        ] + [SumAggregator()]

        for aggregator in aggregators:
            aggregator.validate_config()

        return {aggregator.node_indicator: aggregator for aggregator in aggregators}

    def get_activity(self, name: str) -> BasicTreeNode[TechTreeNodeData]:
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

    def get_activity_adapter(
        self, activity_node: BasicTreeNode[TechTreeNodeData]
    ) -> EnbiosAdapter:
        try:
            return self._adapters[activity_node.data.adapter]
        except KeyError:
            raise ValueError(
                f"Activity '{activity_node.name}' specifies an unknown adapter: {activity_node.data.adapter}."
                + f"Available adapters are: {[a.activity_indicator for a in self.adapters]}"
            )

    def get_activity_default_output(self, activity_name: str) -> float:
        activity = self.get_activity(activity_name)
        return self.get_activity_adapter(activity).get_default_output_value(activity.name)

    def get_activity_output_unit(self, activity_name: str) -> str:
        activity = self.get_activity(activity_name)
        return self.get_activity_adapter(activity).get_activity_output_unit(activity_name)

    def get_node_aggregator(self, aggregator_indicator: str) -> EnbiosAggregator:
        return self._aggregators[aggregator_indicator]

    def _validate_scenarios(self) -> list[Scenario]:
        """
        :return:
        """

        def validate_activities(scenario_: ExperimentScenarioData) -> Activity_Outputs:
            activities = scenario_.activities or {}
            result: dict[str, float] = {}

            for activity_name, activity_output in activities.items():
                activity = self.get_activity(activity_name)

                adapter = self._adapters[activity.data.adapter]
                result[activity_name] = adapter.validate_activity_output(activity_name, activity_output)
                # TODO VALIDATE ADAPTER OUTPUT
                # if activity.id.source == BRIGHTWAY_ACTIVITY:
                #     scenario_output = validate_brightway_output(
                #         output_, activity.bw_activity, activity.id
                #     )
                #     result[simple_id] = scenario_output
            return result

        def validate_scenario(_scenario_data: ExperimentScenarioData) -> Scenario:
            """
            Validate one scenario
            :param _scenario_data:
            :return:
            """
            scenario_activities_outputs: Activity_Outputs = validate_activities(
                _scenario_data
            )
            defined_activities = list(scenario_activities_outputs.keys())

            # fill up the missing activities with default values
            for activity_name in self._activities.keys():
                if activity_name not in defined_activities:
                    scenario_activities_outputs[
                        activity_name
                    ] = self.get_activity_default_output(activity_name)

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
                result_tree=self.base_result_tree.copy(),
            )

        scenarios: list[Scenario] = []

        # undefined scenarios. just one default scenario
        if not self.raw_data.scenarios:
            self.raw_data.scenarios = [
                ExperimentScenarioData(name=Experiment.DEFAULT_SCENARIO_NAME)
            ]

        for index, scenario_data in enumerate(self.raw_data.scenarios):
            scenario_data.name_factory(index)
            scenario = validate_scenario(scenario_data)
            scenarios.append(scenario)
            scenario.prepare_tree()
        return scenarios

    @staticmethod
    def validate_hierarchy(
        hierarchy: ExperimentHierarchyNodeData,
    ) -> BasicTreeNode[TechTreeNodeData]:
        # todo allow no output only when there are scenarios...
        tech_tree: BasicTreeNode[TechTreeNodeData] = BasicTreeNode.from_dict(
            hierarchy.model_dump(), dataclass=TechTreeNodeData
        )

        def validate_node_data(node: BasicTreeNode[TechTreeNodeData]):
            good_leaf = node.is_leaf and node.data.adapter
            good_internal = not node.is_leaf and node.data.aggregator
            assert good_leaf or good_internal, (
                f"Node should have the leaf properties (id, adapter) "
                f"or non-leaf properties (children, aggregator): "
                f"{node.location_names()})"
            )

        tech_tree.recursive_apply(validate_node_data, depth_first=True)
        return tech_tree

    # def _validate_node_calculations(self, tech_tree: BasicTreeNode[TechTreeNodeData]):
    #     def validate_node(node: BasicTreeNode[TechTreeNodeData]):
    #         if node.is_leaf:
    #             # todo we validate that before right?
    #             # self.get_activity(node.name).adapter
    #             # if not node.data.adapter:
    #             #     raise ValueError(
    #             #         f"Node '{node.name}' is a leaf and does not have an adapter"
    #             #     )
    #             # return
    #             pass
    #         if not node.data.aggregator:
    #             if self.config.auto_aggregate:
    #                 all_aggregators = [child.data.aggregator for child in node.children]
    #                 if len(set(all_aggregators)) == 1:
    #                     node.data.aggregator = all_aggregators[0]
    #                 raise ValueError(
    #                     f"Node '{node.name}' is not a leaf and does not have an aggregator. "
    #                     f"Auto-aggregate is on, but the children have different aggregators: {all_aggregators}"
    #                 )
    #
    #             raise ValueError(
    #                 f"Node '{node.name}' is not a leaf and does not have an aggregator"
    #             )
    #
    #     tech_tree.recursive_apply(validate_node, depth_first=True)

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

    def run_scenario(self, scenario_name: str) -> BasicTreeNode[ScenarioResultNodeData]:
        """
        Run a specific scenario
        :param scenario_name:
        :return: The result_tree converted into a dict
        """
        # todo return results
        return self.get_scenario(scenario_name).run()

    def run(self) -> dict[str, BasicTreeNode[ScenarioResultNodeData]]:
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

        # TODO JUST PREPARE SCENARIOS...
        # for scenario in run_scenarios:
        #     scenario.reset_execution_time()
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

        # todo not in this config
        # TODO RUN
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
        #     self._execution_time = time.time() - start_time
        # return results
        return {}

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
        return list([s.name for s in self.scenarios])

    @property
    def adapters(self) -> list[EnbiosAdapter]:
        return list(self._adapters.values())

    def adapter(self, activity_indicator: str) -> EnbiosAdapter:
        return self._adapters[activity_indicator]

    def info(self) -> str:
        """
        Information about the experiment
        :return:
        """
        activity_rows: list[str] = []
        for activity_name, activity in self._activities.items():
            activity_rows.append(f"  {activity.name} - {activity.data.id.name}")
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


