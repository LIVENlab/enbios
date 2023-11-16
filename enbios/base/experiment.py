import csv
import itertools
import math
import time
from dataclasses import asdict
from datetime import timedelta
from pathlib import Path
from tempfile import gettempdir
from typing import Any, Optional, Union

from bw2data.backends import Activity

from enbios.base.adapters_aggregators.adapter import EnbiosAdapter
from enbios.base.adapters_aggregators.aggregator import EnbiosAggregator, SumAggregator
from enbios.base.adapters_aggregators.loader import load_adapter, load_aggregator
from enbios.base.experiment_io import resolve_input_files
from enbios.base.pydantic_experiment_validation import validate_experiment_data
from enbios.base.scenario import Scenario
from enbios.base.stacked_MultiLCA import StackedMultiLCA
from enbios.bw2.util import bw_unit_fix
from enbios.generic.enbios2_logging import get_logger
from enbios.generic.files import PathLike, ReadPath
from enbios.generic.tree.basic_tree import BasicTreeNode
from enbios.models.experiment_models import (
    ActivityOutput,
    Activity_Outputs,
    BWCalculationSetup,
    ExperimentActivityId,
    ExperimentConfig,
    ExperimentData,
    ExperimentMethodPrepData,
    ExperimentScenarioData,
    ScenarioResultNodeData,
    Settings, TechTreeNodeData, ExperimentHierarchyNodeData
)

logger = get_logger(__name__)


class Experiment:
    DEFAULT_SCENARIO_ALIAS = "default scenario"

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
            input_data = validate_experiment_data(**raw_data)
        else:
            input_data = raw_data
        # todo. look at how reading a hierarchy from a csv file or a json
        # creates an additional root node
        resolve_input_files(input_data)
        self.raw_data = validate_experiment_data(asdict(input_data))

        # alias to activity
        self.adapters: list[EnbiosAdapter] = self._validate_adapters()
        self.aggregators: list[EnbiosAggregator] = self._validate_aggregators()

        self._validate_aggregators: list[EnbiosAggregator] = self._validate_aggregators()
        self.adapter_indicator_map: dict[str, EnbiosAdapter] = {adapter.activity_indicator: adapter for adapter in
                                                                self.adapters}
        self.aggregator_indicator_map: dict[str, EnbiosAggregator] = {aggregator.node_indicator: aggregator for
                                                                      aggregator in
                                                                      self.aggregators}

        for adapter in self.adapters:
            adapter.validate_config()

        self.hierarchy_root: BasicTreeNode[
            TechTreeNodeData
        ] = self.validate_hierarchy(self.raw_data.hierarchy)
        self._activities: dict[str, BasicTreeNode[TechTreeNodeData]] = {n.name: n for n in
                                                                        self.hierarchy_root.iter_leaves()}

        for node in self.hierarchy_root.iter_all_nodes():
            if node.is_leaf:
                self.get_activity_adapter(node).validate_activity(node.name,
                                                                  node.data.id,
                                                                  node.data.output,
                                                                  False)
        # make a validation of output units...
        self.base_result_tree = self.convert_tech_tree2result_tree()
        self.base_result_tree.recursive_apply(
            Experiment.recursive_resolve_outputs,
            experiment=self,
            depth_first=True,
            cancel_parents_of=set(),
        )

        self.methods: dict[str, ExperimentMethodPrepData] = {}
        for adapter in self.adapters:
            adapter_methods = adapter.validate_methods()
            if any([m in self.methods for m in adapter_methods.keys()]):
                raise ValueError(
                    f"Some Method(s) {adapter_methods.keys()} already defined in "
                    f"another adapter"
                )
            self.methods.update(adapter_methods)

        self.scenarios: list[Scenario] = self._validate_scenarios()
        self._validate_run_scenario_setting()

        self._lca: Optional[StackedMultiLCA] = None
        self._execution_time: float = float("NaN")

    @staticmethod
    def recursive_resolve_outputs(
            node: BasicTreeNode[ScenarioResultNodeData],
            experiment: "Experiment", **kwargs
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
                if scenario not in self.scenario_aliases:
                    raise ValueError(
                        f"Scenario '{scenario}' not found in experiment scenarios. "
                        f"Scenarios are: {self.scenario_aliases}"
                    )

    def _validate_adapters(self) -> list[EnbiosAdapter]:
        return [
            load_adapter(adapter)
            for adapter in self.raw_data.adapters
        ]

    def _validate_aggregators(self) -> list[EnbiosAggregator]:
        return [
            load_aggregator(adapter)
            for adapter in self.raw_data.aggregators
        ] + [SumAggregator()]

    # @staticmethod
    # def _prepare_activities(
    #         activities: ActivitiesDataTypes,
    # ) -> list[ExperimentActivityData]:
    #     raw_activities_list: list[ExperimentActivityData] = []
    #     if isinstance(activities, list):
    #         raw_activities_list = activities
    #         for activity in raw_activities_list:
    #             activity.orig_id = copy(activity.id)
    #
    #     elif isinstance(activities, dict):
    #         for activity_alias, activity in activities.items():
    #             if activity_alias == activity.alias:
    #                 raise ValueError(
    #                     f"Activity in activities-dict declared with alias: "
    #                     f"'{activity_alias}', "
    #                     f"different than in the activity.id: '{activity.alias}'"
    #                 )
    #             activity.orig_id = copy(activity.id)
    #             activity.id.alias = activity_alias
    #             raw_activities_list.append(activity)
    #     return raw_activities_list

    # def _validate_activities(self,
    #                          activities: list[ExperimentActivityData],
    #                          output_required: bool = False,
    #                          ) -> dict[str, ExperimentActivityData]:
    #     """
    #     Check if all activities exist in the bw database, and check if the
    #     given activities are unique
    #     In case there is only one scenario, all activities are required to have outputs
    #     """
    #     # if activities is a list, convert validate and convert to dict
    #     # default_id_data = ExperimentActivityId(database=bw_default_database)
    #     activities_map: dict[str, ExperimentActivityData] = {}
    #     # validate
    #
    #     for activity in activities:
    #         self._validate_activity(
    #             activity, output_required
    #         )
    #         # check unique aliases
    #         if activity.alias in activities_map:
    #             raise Exception(
    #                 f"Activity-alias '{activity.alias}' passed more then once. "
    #                 f"Consider using a dictionary for activities or include "
    #                 f"'alias' in the ids."
    #             )
    #         activities_map[activity.alias] = activity
    #
    #     # all codes should only appear once
    #     # unique_activities = set()
    #     # for ext_activity in activities_map.values():
    #     #     unique_activities.add((ext_activity.id.database, ext_activity.id.code))
    #     assert len(activities_map) > 0, "There are no activities in the experiment"
    #     return activities_map
    #
    # def _validate_activity(self,
    #                        activity: ExperimentActivityData,
    #                        required_output: bool = False,
    #                        ):
    #     """
    #     This method checks if the activity exists in the database by several ways.
    #     :param activity:
    #     :param required_output:
    #     :return:
    #     """
    #     adapter = self.adapter_indicator_map.get(activity.id.source)
    #     if not adapter:
    #         logger.debug(f"Candidates are: {self.adapter_indicator_map.keys()}")
    #         raise ValueError(
    #             f"Activity source '{activity.id.source}' not found in adapters"
    #         )
    #     adapter.validate_activity(activity, required_output)

    def _has_activity(
            self, name: str
    ) -> Optional[BasicTreeNode[TechTreeNodeData]]:
        activity = self._activities.get(name, None)
        return activity

    def get_activity(
            self, alias_or_id: Union[str, ExperimentActivityId]
    ) -> BasicTreeNode[TechTreeNodeData]:
        """
        Get an activity by either its alias or its original "id"
        as it is defined in the experiment data
        :param alias_or_id:
        :return: ExtendedExperimentActivityData
        """
        activity = self._has_activity(alias_or_id)
        if not activity:
            raise ValueError(f"Activity with id {alias_or_id} not found")
        return activity

    def get_activity_adapter(self, activity_node: BasicTreeNode[TechTreeNodeData]) -> EnbiosAdapter:
        return self.adapter_indicator_map[activity_node.data.adapter]

    def get_activity_default_output(self, activity_node_name: str) -> float:
        activity = self.get_activity(activity_node_name)
        return self.get_activity_adapter(activity).get_default_output_value(activity.name)

    def get_activity_unit(self, activity_node_name: str) -> str:
        activity = self.get_activity(activity_node_name)
        return self.get_activity_adapter(activity).get_activity_output_unit(activity_node_name)

    def get_node_aggregator(self, aggregator_indicator: str) -> EnbiosAggregator:
        return self.aggregator_indicator_map[aggregator_indicator]

    def convert_tech_tree2result_tree(self) -> BasicTreeNode[ScenarioResultNodeData]:
        def recursive_convert(node: BasicTreeNode[TechTreeNodeData]) -> BasicTreeNode[ScenarioResultNodeData]:
            output: tuple[Optional[str], Optional[float]] = (None, None)
            if node.is_leaf:
                output = (self.get_activity_adapter(node).get_activity_output_unit(node.name), 0)
            return BasicTreeNode(
                name=node.name,
                data=ScenarioResultNodeData(
                    output=output,
                    adapter=node.data.adapter,
                    aggregator=node.data.aggregator
                ),
                children=[recursive_convert(child) for child in node.children]
            )

        return recursive_convert(self.hierarchy_root)

    def _validate_scenarios(self) -> list[Scenario]:
        """
        :return:
        """

        def validate_activities(scenario_: ExperimentScenarioData) -> Activity_Outputs:
            activities = scenario_.activities
            result: dict[str, float] = {}

            def convert_output(output) -> ActivityOutput:
                if isinstance(output, tuple):
                    return ActivityOutput(
                        unit=bw_unit_fix(output[0]), magnitude=output[1]
                    )
                else:
                    return output  # type: ignore

            if not activities:
                return result

            scenarios_activities = (
                activities if isinstance(activities, list) else activities.items()
            )
            for activity_name, activity_output in scenarios_activities:
                activity = self.get_activity(activity_name)
                # simple_id = validate_activity_id(activity_id)
                output_ = convert_output(activity_output)

                adapter = self.adapter_indicator_map[activity.data.adapter]
                adapter.validate_activity_output(activity_name, output_)
                # TODO VALIDATE ADAPTER OUTPUT
                # if activity.id.source == BRIGHTWAY_ACTIVITY:
                #     scenario_output = validate_brightway_output(
                #         output_, activity.bw_activity, activity.id
                #     )
                #     result[simple_id] = scenario_output
            return result

        def validate_scenario(
                _scenario: ExperimentScenarioData, _scenario_alias: str
        ) -> Scenario:
            """
            Validate one scenario
            :param _scenario:
            :param _scenario_alias:
            :return:
            """
            scenario_activities_outputs: Activity_Outputs = validate_activities(_scenario)
            defined_aliases = list(scenario_activities_outputs.keys())

            # fill up the missing activities with default values
            for activity in self._activities.values():
                activity_alias = activity.name
                if activity_alias not in defined_aliases:
                    scenario_activities_outputs[activity.name] = self.get_activity_default_output(activity.name)

            resolved_methods: dict[str, ExperimentMethodPrepData] = {}
            if _scenario.methods:
                for index_, method_ in enumerate(_scenario.methods):
                    if isinstance(method_, str):
                        global_method = self.methods.get(method_)
                        assert global_method
                        resolved_methods[global_method.alias] = global_method

            return Scenario(
                experiment=self,  # type: ignore
                alias=_scenario_alias,
                activities_outputs=scenario_activities_outputs,
                methods=resolved_methods,
                result_tree=self.base_result_tree.copy(),
            )

        raw_scenarios = self.raw_data.scenarios
        scenarios: list[Scenario] = []

        # from Union, list or dict
        if isinstance(raw_scenarios, list):
            raw_list_scenarios: list[ExperimentScenarioData] = raw_scenarios
            for index, _scenario in enumerate(raw_list_scenarios):
                _scenario_alias = (
                    _scenario.alias
                    if _scenario.alias
                    else ExperimentScenarioData.alias_factory(index)
                )
                scenarios.append(validate_scenario(_scenario, _scenario_alias))
        elif isinstance(raw_scenarios, dict):
            raw_dict_scenarios: dict[str, ExperimentScenarioData] = raw_scenarios
            for alias, _scenario in raw_dict_scenarios.items():
                if _scenario.alias is not None and _scenario.alias != alias:
                    assert False, (
                        f"Scenario defines alias as dict-key: {alias} but "
                        f"also in the scenario object: {_scenario.alias}"
                    )
                _scenario.alias = alias
                scenarios.append(validate_scenario(_scenario, alias))
        # undefined scenarios. just one default scenario
        elif not raw_scenarios:
            default_scenario = ExperimentScenarioData()
            scenarios.append(
                validate_scenario(default_scenario, Experiment.DEFAULT_SCENARIO_ALIAS)
            )

        for scenario in scenarios:
            scenario.prepare_tree()
        return scenarios

    @staticmethod
    def validate_hierarchy(
            hierarchy: ExperimentHierarchyNodeData
    ) -> BasicTreeNode[TechTreeNodeData]:

        tech_tree: BasicTreeNode[TechTreeNodeData] = BasicTreeNode.from_dict(
            asdict(hierarchy), dataclass=TechTreeNodeData
        )

        def validate_node_data(node: BasicTreeNode[TechTreeNodeData]):
            good_leaf = node.is_leaf and node.data.adapter
            good_internal = not node.is_leaf and node.data.aggregator
            assert good_leaf or good_internal, (f"Node should have the leaf properties (id, adapter) "
                                                f"or non-leaf properties (children, aggregator): "
                                                f"{node.location_names()})")

        tech_tree.recursive_apply(validate_node_data, depth_first=True)
        return tech_tree

    def _validate_node_calculations(self, tech_tree: BasicTreeNode[TechTreeNodeData]):

        def validate_node(node: BasicTreeNode[TechTreeNodeData]):

            if node.is_leaf:
                # todo we validate that before right?
                # self.get_activity(node.name).adapter
                # if not node.data.adapter:
                #     raise ValueError(
                #         f"Node '{node.name}' is a leaf and does not have an adapter"
                #     )
                # return
                pass
            if not node.data.aggregator:
                if self.config.auto_aggregate:
                    all_aggregators = [child.data.aggregator for child in node.children]
                    if len(set(all_aggregators)) == 1:
                        node.data.aggregator = all_aggregators[0]
                    raise ValueError(
                        f"Node '{node.name}' is not a leaf and does not have an aggregator. "
                        f"Auto-aggregate is on, but the children have different aggregators: {all_aggregators}"
                    )

                raise ValueError(
                    f"Node '{node.name}' is not a leaf and does not have an aggregator"
                )

        tech_tree.recursive_apply(
            validate_node,
            depth_first=True
        )

    def get_scenario(self, scenario_alias: str) -> Scenario:
        """
        Get a scenario by its alias
        :param scenario_alias:
        :return:
        """
        for scenario in self.scenarios:
            if scenario.alias == scenario_alias:
                return scenario
        raise ValueError(f"Scenario '{scenario_alias}' not found")

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
        Run all scenarios. Returns a dict with the scenario alias as key
        and the result_tree as value
        :return: dictionary scenario-alias : result_tree
        """
        methods = [m.id for m in self.methods.values()]
        inventories: list[list[dict[Activity, float]]] = []

        if self.config.run_scenarios:
            run_scenarios = [self.get_scenario(s) for s in self.config.run_scenarios]
            logger.info(f"Running selected scenarios: {[s.alias for s in run_scenarios]}")
        else:
            run_scenarios = self.scenarios

        # TODO JUST PREPARE SCENARIOS...
        # for scenario in run_scenarios:
        #     scenario.reset_execution_time()
        #     if scenario.methods:
        #         raise ValueError(
        #             f"Scenario cannot have individual methods. '{scenario.alias}'"
        #         )
        #     inventory: list[dict[Activity, float]] = []
        #     for activity_alias, act_out in scenario.activities_outputs.items():
        #         bw_activity = scenario.experiment.get_activity(
        #             activity_alias.alias
        #         ).bw_activity
        #         inventory.append({bw_activity: act_out})
        #     inventories.append(inventory)

        # run experiment
        start_time = time.time()
        calculation_setup = BWCalculationSetup(
            "experiment", list(itertools.chain(*inventories)), methods
        )

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
                    scenario_results += f"{scenario.alias}: {scenario.execution_time}\n"
            if any_scenario_run:
                return scenario_results
            else:
                return "not run"

    def results_to_csv(
            self,
            file_path: PathLike,
            scenario_alias: Optional[str] = None,
            level_names: Optional[list[str]] = None,
            include_method_units: bool = True,
    ):
        """
        Turn the results into a csv file. If no scenario name is given,
        it will export all scenarios to the same file,
        :param file_path:
        :param scenario_alias: If no scenario name is given, it will
        export all scenarios to the same file,
            with an additional column for the scenario alias
        :param level_names: (list of strings) If given, the results will be
        exported with the given level names
        :param include_method_units:  (Include the units of the methods in the header)
        :return:
        """
        if scenario_alias:
            scenario = self.get_scenario(scenario_alias)
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
                temp_file_name = gettempdir() + f"/temp_scenario_{scenario.alias}.csv"
                scenario.results_to_csv(
                    temp_file_name,
                    level_names=level_names,
                    include_method_units=include_method_units,
                )
                rows = ReadPath(temp_file_name).read_data()
                rows[0]["scenario"] = scenario.alias
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
    def activities_aliases(self) -> list[str]:
        return list(self._activities.keys())

    @property
    def method_aliases(self) -> list[str]:
        return list(self.methods.keys())

    @property
    def scenario_aliases(self) -> list[str]:
        return list([s.alias for s in self.scenarios])

    def info(self) -> str:
        """
        Information about the experiment
        :return:
        """
        activity_rows: list[str] = []
        for activity_alias, activity in self._activities.items():
            activity_rows.append(f"  {activity.name} - {activity.id.name}")
        activity_rows_str = "\n".join(activity_rows)
        methods_str = "\n".join([f" {m.id}" for m in self.methods.values()])
        return (
            f"Experiment: \n"
            f"Activities: {len(self._activities)}\n"
            f"{activity_rows_str}\n"
            f"Methods: {len(self.methods)}\n"
            f"{methods_str}\n"
            f"Hierarchy (depth): {self.hierarchy_root.depth}\n"
            f"Scenarios: {len(self.scenarios)}\n"
        )
