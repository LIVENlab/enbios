from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Union

import bw2data as bd

from bw2calc import MultiLCA
from bw2data.backends import Activity
from numpy import ndarray
from pint import DimensionalityError, UndefinedUnitError, Quantity

from enbios2.base.db_models import BWProjectIndex
from enbios2.base.unit_registry import ureg
from enbios2.ecoinvent.ecoinvent_index import get_ecoinvent_dataset_index
from enbios2.generic.enbios2_logging import get_logger
from enbios2.generic.tree.basic_tree import BasicTreeNode
from enbios2.models.experiment_models import (ExperimentActivityId,
                                              ExtendedExperimentActivityData,
                                              BWMethod, ExperimentMethodData,
                                              ExperimentScenarioData, ExperimentData,
                                              ExtendedExperimentActivityOutput,
                                              EcoInventSimpleIndex, MethodsDataTypes, ExperimentActivityOutput,
                                              ActivitiesDataTypes, BWCalculationSetup)

logger = get_logger(__file__)

# map from ExtendedExperimentActivityData.alias = (ExtendedExperimentActivityData.id.alias) to outputs
Activity_Outputs = dict[str, float]

"""
dict of Method tuple, to result-value 
"""


@dataclass
class ScenarioResultNodeData:
    output: tuple[str, Optional[float]] = ("", None)
    results: dict[Union[tuple[str], str], float] = field(default_factory=dict)


@dataclass
class Scenario:
    experiment: "Experiment"
    alias: Optional[str] = None
    # this should be a simpler type - just str: float
    # todo not used yet
    orig_outputs: Optional[dict[str, float]] = field(default_factory=dict)  # output declaration before conversion
    activities_outputs: Activity_Outputs = field(default_factory=dict)
    result_tree: Optional[BasicTreeNode[ScenarioResultNodeData]] = None
    methods: Optional[dict[str, ExperimentMethodData]] = None

    def create_bw_calculation_setup(self, register: bool = True) -> BWCalculationSetup:
        inventory: list[dict[Activity, float]] = []
        for activity_alias, act_out in self.activities_outputs.items():
            bw_activity = self.experiment.get_activity(activity_alias).bw_activity
            inventory.append({bw_activity: act_out})

        methods = [m.id for m in self.get_methods().values()]
        calculation_setup = BWCalculationSetup(self.alias, inventory, methods)
        if register:
            calculation_setup.register()
        return calculation_setup

    def create_results_to_technology_tree(self, results: ndarray) -> BasicTreeNode[ScenarioResultNodeData]:
        """
        Add results to the technology tree, for each method
        """

        def recursive_resolve_node(node: BasicTreeNode[ScenarioResultNodeData]):
            for child in node.children:
                for key, value in child.data.results.items():
                    if node.data.results.get(key) is None:
                        node.data.results[key] = 0
                    node.data.results[key] += value

        activity_nodes = list(self.result_tree.get_leaves())
        # todo should be the same set of activities
        activities_aliases = list(self.activities_outputs.keys())

        methods_aliases: list[str] = list(self.get_methods().keys())
        for result_index, alias in enumerate(activities_aliases):
            bw_activity = self.experiment.get_activity(alias).bw_activity
            try:
                activity_node = next(filter(lambda node: node.temp_data().bw_activity == bw_activity, activity_nodes))
            except StopIteration:
                raise ValueError(f"Activity {alias} not found in the technology tree [TODO: this should not happen]")

            for method_index, method in enumerate(methods_aliases):
                activity_node.data.results[method] = results[result_index][method_index]

        self.result_tree.recursive_apply(recursive_resolve_node, depth_first=True)
        return self.result_tree

    def get_methods(self) -> Optional[dict[str, ExperimentMethodData]]:
        if self.methods:
            return self.methods
        else:
            return self.experiment.methods

    def run(self) -> BasicTreeNode[ScenarioResultNodeData]:
        if not self.get_methods():
            raise ValueError(f"Scenario '{self.alias}' has no methods")
        logger.info(f"Running scenario '{self.alias}'")
        bw_calc_setup = self.create_bw_calculation_setup()

        self.result_tree = self.experiment.technology_root_node.copy()
        activity_nodes = list(self.result_tree.get_leaves())
        activities_aliases = list(self.activities_outputs.keys())
        for result_index, alias in enumerate(activities_aliases):
            bw_activity = self.experiment.get_activity(alias).bw_activity
            try:
                activity_node = next(filter(lambda node: node.temp_data().bw_activity == bw_activity, activity_nodes))
            except StopIteration:
                raise ValueError(f"Activity {alias} not found in the technology tree [TODO: this should not happen]")

            # todo this does not consider magnitude...
            activity_node.data.output = (bw_activity['unit'].replace(" ", "_"), self.activities_outputs[alias])

        results: ndarray = MultiLCA(bw_calc_setup.name).results

        def recursive_resolve_outputs(node: BasicTreeNode[ScenarioResultNodeData]):
            if node.is_leaf:
                return
            node_output = None
            for child in node.children:
                activity_output = child.data.output[0]
                units = {[activity_output, activity_output.replace("_", " ")]}
                output: Optional[Quantity] = None
                for ao in units:
                    try:
                        output = ureg.parse_expression(ao) * child.data.output[1]
                        break
                    except UndefinedUnitError as err:
                        logger.error(
                            f"Cannot parse output unit '{ao}'- {activity_output} of activity {child.name}. {err}. "
                            f"Consider the unit definition to 'enbios2/base/unit_registry.py'")
                        pass
                if not output:
                    raise ValueError(f"Cannot parse output unit {activity_output} of activity {child.name}. {err}")
                try:
                    if not node_output:
                        node_output = output
                    else:
                        node_output += output
                except DimensionalityError as err:
                    logger.warning(f"Cannot aggregate output to parent: {node.name}. "
                                   f"From earlier children the base unit is {node_output.to_base_units()} "
                                   f"and from {child.name} it is {output.units}."
                                   f" {err}")
            node_output = node_output.to_compact()
            node.data.output = (str(node_output.units), node_output.magnitude)

        self.result_tree.recursive_apply(recursive_resolve_outputs, depth_first=True)

        return self.create_results_to_technology_tree(results)

    def results_to_csv(self, file_path: Path, include_method_units: bool = False):
        """
        Save the results (as tree) to a csv file
         :param file_path:  path to save the results to
         :param include_method_units:
        """
        if not self.result_tree:
            raise ValueError(f"Scenario '{self.alias}' has no results")

        def data_serializer(data: ScenarioResultNodeData) -> dict:
            result = {}
            if data.output:
                result["unit"] = data.output[0]
                result['amount'] = data.output[1]
            if not include_method_units:
                return result | data.results
            else:
                for method_alias, value in data.results.items():
                    final_name = f"{method_alias} ({self.experiment.methods[str(method_alias)].bw_method.unit})"
                    result[final_name] = value
                return result

        self.result_tree.to_csv(file_path, include_data=True, data_serializer=data_serializer)


class Experiment:
    DEFAULT_SCENARIO_ALIAS = "default scenario"

    def __init__(self, raw_data: Union[ExperimentData, dict]):
        if isinstance(raw_data, dict):
            input_data = ExperimentData(**raw_data)
        else:
            input_data = raw_data
        self.raw_data = input_data
        # alias to activity
        self.activities: dict[str, ExtendedExperimentActivityData] = {}

        self.validate_bw_config()
        self.validate_activities(self.raw_data.activities)
        self.technology_root_node: BasicTreeNode[ScenarioResultNodeData] = self.create_technology_tree()

        self.methods: dict[str, ExperimentMethodData] = Experiment.validate_methods(self.prepare_methods())
        self.scenarios: list[Scenario] = self.validate_scenarios()

    @staticmethod
    def create(bw_project: str):
        return Experiment(ExperimentData(bw_project=bw_project, activities=[], methods=[]))

    def add_activity(self, activity: Activity, default_demand: Optional[ExperimentActivityOutput] = None):
        if len(self.scenarios) == 1 and not default_demand:
            raise ValueError("No default demand specified / and no scenarios added yet")
        alias = activity["name"]
        if alias in self.activities:
            raise ValueError(f"Activity with alias {alias} already exists")
        self.activities[alias] = ExtendedExperimentActivityData(
            id=ExperimentActivityId(
                alias=alias,
                code=activity["code"],
                database=activity["database"]
            ),
            output=default_demand,
            bw_activity=activity
        )

    def add_method(self, method: tuple[str, ...], alias: Optional[str] = None):
        if not alias:
            alias = "_".join(method)
        m_data = ExperimentMethodData(id=method, alias=alias)
        Experiment.validate_method(m_data)
        self.methods[m_data.alias] = m_data

    def validate_bw_config(self):

        def validate_bw_project_bw_database(bw_project: str, bw_default_database: Optional[str] = None):
            if bw_project not in bd.projects:
                raise ValueError(f"Project {bw_project} not found")
            if bw_project in bd.projects:
                bd.projects.set_current(bw_project)

            if bw_default_database:
                if bw_default_database not in bd.databases:
                    raise ValueError(f"Database {bw_default_database} "
                                     f"not found. Options are: {list(bd.databases)}")

        # print("validate_bw_config***************", self.raw_data.bw_project)
        if isinstance(self.raw_data.bw_project, str):
            validate_bw_project_bw_database(self.raw_data.bw_project, self.raw_data.bw_default_database)

        else:
            simple_index: EcoInventSimpleIndex = self.raw_data.bw_project
            ecoinvent_index = get_ecoinvent_dataset_index(version=simple_index.version,
                                                          system_model=simple_index.system_model,
                                                          type_="default")
            if ecoinvent_index:
                ecoinvent_index = ecoinvent_index[0]
            else:
                raise ValueError(f"Ecoinvent index {self.raw_data.bw_project} not found")

            bw_project_index: BWProjectIndex = ecoinvent_index.bw_project_index
            if not bw_project_index:
                raise ValueError(f"Ecoinvent index {ecoinvent_index}, has not BWProject index")
            validate_bw_project_bw_database(bw_project_index.project_name, bw_project_index.database_name)

    def validate_activities(self, activities: ActivitiesDataTypes, output_required: bool = False):
        """
        Check if all activities exist in the bw database, and check if the given activities are unique
        In case there is only one scenario, all activities are required to have outputs
        """
        # if activities is a list, convert validate and convert to dict
        default_id_data = ExperimentActivityId(database=self.raw_data.bw_default_database)
        if isinstance(activities, list):
            logger.debug("activity list")
            for activity in activities:
                ext_activity: ExtendedExperimentActivityData = activity.check_exist(default_id_data, output_required)
                self.activities[ext_activity.alias] = ext_activity
        elif isinstance(activities, dict):
            logger.debug("activity dict")
            for activity_alias, activity in activities.items():
                default_id_data.alias = activity_alias
                ext_activity: ExtendedExperimentActivityData = activity.check_exist(default_id_data, output_required)
                self.activities[ext_activity.alias] = ext_activity

        # all codes should only appear once
        unique_activities = set()
        for ext_activity in self.activities.values():
            ext_activity: ExtendedExperimentActivityData = ext_activity
            unique_activities.add((ext_activity.id.database, ext_activity.id.code))

            if ext_activity.output:
                ext_activity.output = Experiment.validate_output(ext_activity.output, ext_activity)
        assert len(unique_activities) == len(activities), "Not all activities are unique"

    @staticmethod
    def validate_output(target_output: ExtendedExperimentActivityOutput,
                        activity: ExtendedExperimentActivityData) -> float:
        try:
            target_quantity = ureg.parse_expression(
                target_output.unit, case_sensitive=False) * target_output.magnitude
            return target_quantity.to(activity.bw_activity['unit']).magnitude
        except Exception as err:
            logger.error(
                f"Cannot parse output unit '{target_output.unit}'- of activity {activity}. {err}. "
                f"Consider the unit definition to 'enbios2/base/unit_registry.py'")
            raise Exception(f"Unit error, {err}; For activity: {activity.id}")

    def prepare_methods(self, methods: Optional[MethodsDataTypes] = None) -> dict[str, ExperimentMethodData]:
        if not methods:
            methods = self.raw_data.methods
        method_dict: dict[str, ExperimentMethodData] = {}
        if isinstance(methods, dict):
            for method_alias in methods:
                method_dict[method_alias] = ExperimentMethodData(alias=method_alias, id=methods[method_alias])
        elif isinstance(self.raw_data.methods, list):
            method_list: list[ExperimentMethodData] = self.raw_data.methods
            for method_ in method_list:
                if not method_.alias:
                    method_.alias = "_".join(method_.id)
                method_dict[method_.alias] = method_
        return method_dict

    @staticmethod
    def validate_method(method: ExperimentMethodData):
        method.id = tuple(method.id)
        bw_method = bd.methods.get(method.id)
        if not bw_method:
            raise Exception(f"Method with id: {method.id} does not exist")
        method.bw_method = BWMethod(**bw_method)

    @staticmethod
    def validate_methods(method_dict: dict[str, ExperimentMethodData]) -> dict[str, ExperimentMethodData]:
        # all methods must exist
        for method in method_dict.values():
            Experiment.validate_method(method)
        return method_dict

    def get_activity(self,
                     alias_or_id: Union[str, ExperimentActivityId],
                     raise_error_if_missing: bool = True) -> Optional[ExtendedExperimentActivityData]:
        if isinstance(alias_or_id, str):
            activity = self.activities.get(alias_or_id, None)
            if not activity and raise_error_if_missing:
                raise ValueError(f"Activity with alias {alias_or_id} not found")
            return activity
        else:  # isinstance(alias_or_id, ExperimentActivityId):
            for activity in self.activities.values():
                if activity.orig_id == alias_or_id:
                    return activity
            if raise_error_if_missing:
                raise ValueError(f"Activity with id {alias_or_id} not found")

    def validate_scenarios(self) -> list[Scenario]:
        """
        :return:
        """

        def validate_activities(scenario: ExperimentScenarioData) -> Activity_Outputs:
            activities = scenario.activities
            # two Union types,
            # 1. list of tuple: alias | ActivityIds -> unit
            # 2. dict: alias -> unit
            # turn to alias dict
            activity_outputs: Activity_Outputs = {}
            if isinstance(activities, list):
                for activity in activities:
                    activity_id, activity_output = activity
                    activity = self.get_activity(activity_id)
                    output: float = Experiment.validate_output(activity_output, activity)
                    activity_outputs[activity.alias] = output
            elif isinstance(activities, dict):
                for activity_alias, activity_output in activities.items():
                    activity = self.get_activity(activity_alias)
                    output: float = Experiment.validate_output(activity_output, activity)
                    activity_outputs[activity.alias] = output
            return activity_outputs

        def validate_scenario(scenario: ExperimentScenarioData) -> Scenario:
            """
            Validate one scenario
            :param scenario:
            :return:
            """
            scenario_activities_outputs: Activity_Outputs = validate_activities(scenario)
            # fill up the missing activities with default values
            for activity in self.activities.values():
                activity_alias = activity.alias
                if activity_alias not in scenario_activities_outputs:
                    scenario_activities_outputs[activity.alias] = 1
                    if self.raw_data.config.warn_default_demand:
                        logger.warning(
                            f"Activity: '{activity_alias}' has no output for scenario {scenario.alias} "
                            f"and will be set to 1")

            if scenario.methods:
                if isinstance(scenario.methods, list):
                    for index_, method_ in enumerate(scenario.methods):
                        if isinstance(method_, str):
                            global_method = self.methods.get(method_)
                            scenario.methods[index_] = global_method
                scenario.methods = self.prepare_methods(scenario.methods)

            return Scenario(experiment=self,
                            alias=scenario.alias,
                            activities_outputs=scenario_activities_outputs,
                            methods=scenario.methods)

        raw_scenarios = self.raw_data.scenarios
        scenarios: list[Scenario] = []

        # from Union, list or dict
        if isinstance(raw_scenarios, list):
            raw_scenarios: list[ExperimentScenarioData] = self.raw_data.scenarios
            for index, _scenario in enumerate(raw_scenarios):
                if not _scenario.alias:
                    _scenario.alias = ExperimentScenarioData.alias_factory(index)
                scenarios.append(validate_scenario(_scenario))
        elif isinstance(self.raw_data.scenarios, dict):
            raw_scenarios: dict[str, ExperimentScenarioData] = self.raw_data.scenarios
            for alias, _scenario in raw_scenarios.items():
                if _scenario.alias is not None and _scenario.alias != alias:
                    assert False, (f"Scenario defines alias as dict-key: {alias} but "
                                   f"also in the scenario object: {_scenario.alias}")
                _scenario.alias = alias
                scenarios.append(validate_scenario(_scenario))
        elif not raw_scenarios:
            default_scenario = ExperimentScenarioData(alias=Experiment.DEFAULT_SCENARIO_ALIAS)
            scenarios.append(validate_scenario(default_scenario))

        return scenarios

    def create_technology_tree(self) -> BasicTreeNode[ScenarioResultNodeData]:
        if not self.raw_data.hierarchy:
            self.raw_data.hierarchy = list(self.activities.keys())

        tech_tree: BasicTreeNode[ScenarioResultNodeData] = (BasicTreeNode.from_dict(self.raw_data.hierarchy,
                                                                                    compact=True,
                                                                                    data_factory=lambda
                                                                                        _: ScenarioResultNodeData()))
        for leaf in tech_tree.get_leaves():
            leaf._data = self.get_activity(leaf.name, True)
        return tech_tree

    def get_scenario(self, scenario_name: str) -> Scenario:
        for scenario in self.scenarios:
            if scenario.alias == scenario_name:
                return scenario
        raise f"Scenario '{scenario_name}' not found"

    def run_scenario(self, scenario_name: str) -> dict:
        return self.get_scenario(scenario_name).run().as_dict(include_data=True)

    def run(self) -> dict[str, dict]:
        results = {}
        for scenario in self.scenarios:
            results[scenario.alias] = self.run_scenario(scenario.alias)
        return results

    def results_to_csv(self, file_path: Path, scenario_name: Optional[str] = None, include_method_units: bool = True):
        """
        :param file_path:
        :param scenario_name:
        :param include_method_units:  (Include the units of the methods in the header)
        :return:
        """
        if scenario_name:
            scenario = self.get_scenario(scenario_name)
        else:
            scenario = self.scenarios[0]
        scenario.results_to_csv(file_path, include_method_units)

    def __repr__(self):
        pass
