from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Union

import bw2data as bd
from bw2calc import MultiLCA
from bw2data.backends import Activity
from numpy import ndarray
from pint import UnitRegistry

from enbios2.generic.enbios2_logging import get_logger
from enbios2.generic.tree.basic_tree import BasicTreeNode
from enbios2.models.experiment_models import (ExperimentActivitiesGlobalConf, ExperimentActivityId,
                                              ExtendedExperimentActivityData,
                                              ExperimentActivityData, BWMethod, ExperimentMethodData,
                                              ExperimentScenarioData, ExperimentData,
                                              ExtendedExperimentActivityOutput)
from enbios2.technology_tree import BW_CalculationSetup

logger = get_logger(__file__)

# todo there is another type for this, with units...
Activity_Outputs = dict[str, tuple[Activity, float]]

"""
dict of Method tuple, to result-value 
"""
ScenarioResultNodeData = dict[tuple[str], float]


@dataclass
class Scenario:
    alias: Optional[str] = None
    activities_outputs: Activity_Outputs = field(default_factory=dict)
    results: Optional[ndarray] = None
    result_tree: Optional[BasicTreeNode[ScenarioResultNodeData]] = None

    def add_results_to_technology_tree(self, methods_ids: list[tuple[str]]):
        activity_nodes = self.result_tree.get_leaves()
        activities_outputs = list(self.activities_outputs.values())
        for result_index, activity_out in enumerate(activities_outputs):
            activity_node = next(filter(lambda node: node._data.bw_activity == activity_out[0], activity_nodes))
            for method_index, method in enumerate(methods_ids):
                activity_node.data[method] = self.results[result_index][method_index]

    def resolve_result_tree(self):

        def recursive_resolve_node(node: BasicTreeNode[ScenarioResultNodeData]) -> ScenarioResultNodeData:
            if node.is_leaf:
                return node.data
            for child in node.children:
                recursive_resolve_node(child)
            for child in node.children:
                for key, value in child.data.items():
                    if node.data.get(key) is None:
                        node.data[key] = 0
                    node.data[key] += value

        recursive_resolve_node(self.result_tree)


class Experiment:
    ureg = UnitRegistry()

    def __init__(self, raw_data: ExperimentData):
        if raw_data.bw_project in bd.projects:
            bd.projects.set_current(raw_data.bw_project)
        self.raw_data = raw_data
        Experiment.ureg = UnitRegistry()

        self.validate_bw_config()
        # alias to activity
        self.activitiesMap: dict[str, ExtendedExperimentActivityData] = {}
        self.default_activities_outputs: Activity_Outputs = {}

        output_required = not raw_data.scenarios
        self.validate_activities(output_required)

        self.methods: dict[str, ExperimentMethodData] = self.prepare_methods()
        self.validate_methods(self.methods)

        self.scenarios: list[Scenario] = self.validate_scenarios(list(self.activitiesMap.values()))

        self.technology_root_node: BasicTreeNode[ScenarioResultNodeData] = self.create_technology_tree()

    def validate_bw_config(self):
        if self.raw_data.bw_project not in bd.projects:
            raise Exception(f"Project {self.raw_data.bw_project} not found")
        if self.raw_data.bw_project in bd.projects:
            bd.projects.set_current(self.raw_data.bw_project)

        if self.raw_data.activities_config.default_database:
            if self.raw_data.activities_config.default_database not in bd.databases:
                raise Exception(f"Database {self.raw_data.activities_config.default_database} not found")

    def validate_activities(self, required_output: bool = False):
        """

        :param required_output:
        :return:
        """
        # check if all activities exist
        activities = self.raw_data.activities
        config: ExperimentActivitiesGlobalConf = self.raw_data.activities_config

        logger.debug(f"activity-configuration, {config}")
        # if activities is a list, convert validate and convert to dict
        default_id_data = ExperimentActivityId(database=config.default_database)
        if isinstance(activities, list):
            logger.debug("activity list")
            activities_list: list[ExperimentActivityData] = activities

            for activity in activities_list:
                activity: ExperimentActivityData = activity
                ext_activity = activity.check_exist(default_id_data, required_output)
                self.activitiesMap[ext_activity.id.alias] = ext_activity
        elif isinstance(activities, dict):
            logger.debug("activity dict")
            # activities: dict[str, ExperimentActivity] = activities
            for activity_alias, activity in activities.items():
                default_id_data.alias = activity_alias
                ext_activity = activity.check_exist(default_id_data, required_output)
                self.activitiesMap[ext_activity.id.alias] = ext_activity

        # all codes should only appear once
        unique_activities = set()
        for activity in self.activitiesMap.values():
            activity: ExtendedExperimentActivityData = activity
            unique_activities.add((activity.id.database, activity.id.code))
            if activity.output:
                output = Experiment.validate_output(activity.output, activity)
                self.default_activities_outputs[activity.bw_activity._document.code] = (activity.bw_activity, output)

        assert len(unique_activities) == len(activities), "Not all activities are unique"

    def collect_orig_ids(self) -> list[tuple[ExperimentActivityId, ExtendedExperimentActivityData]]:
        return [(activity.orig_id, activity) for activity in self.activitiesMap.values()]

    @staticmethod
    def validate_output(target_output: ExtendedExperimentActivityOutput,
                        activity: ExtendedExperimentActivityData) -> float:
        try:
            pint_target_unit = Experiment.ureg(target_output.unit)
            pint_target_quantity = target_output.magnitude * pint_target_unit
            #
            pint_activity_unit = Experiment.ureg(activity.id.unit)
            #
            target_output.pint_quantity = pint_target_quantity.to(pint_activity_unit)
            return target_output.magnitude
        except Exception as err:
            # todo, change to Exception, and catch that in test too,
            # raise Exception(f"Unit error, {err}; For activity: {activity.id}")
            assert False, f"Unit error, {err}; For activity: {activity.id}"

    def prepare_methods(self) -> dict[str, ExperimentMethodData]:
        """
        give all methods some alias and turn them into a dict
        :return: map of alias -> method
        """
        if isinstance(self.raw_data.methods, dict):
            method_dict: dict[str, ExperimentMethodData] = self.raw_data.methods
            for method_alias, method in method_dict.items():
                assert method.alias is None or method_alias == method.alias, f"Method: {method} must either have NO alias or the same as the key"
                method.alias = method_alias
            return method_dict
        elif isinstance(self.raw_data.methods, list):
            method_list: list[ExperimentMethodData] = self.raw_data.methods
            method_dict: dict[str, ExperimentMethodData] = {}
            for method in method_list:
                if not method.alias:
                    method.alias = "_".join(method.id)
                method_dict[method.alias] = method
            return method_dict

    @staticmethod
    def validate_methods(methods: dict[str, ExperimentMethodData]):
        # all methods must exist
        all_methods = bd.methods
        method_tree: dict[str, dict] = {}

        def build_method_tree():
            """make a tree search (tuple-part=level)"""
            if not method_tree:
                for bw_method in all_methods.keys():
                    # iter through tuple
                    current = method_tree
                    for part in bw_method:
                        current = current.setdefault(part, {})

        def tree_search(search_method_tuple: tuple[str]) -> dict:
            """search for a method in the tree. Is used when not all parts of a method are given"""
            build_method_tree()
            current = method_tree
            # print(method_tree)

            result = list(search_method_tuple)
            for index, part in enumerate(search_method_tuple):
                _next = current.get(part)
                assert _next, f"Method not found. Part: '{part}' does not exist for {list(search_method_tuple)[index - 1]}"
                current = _next

            while True:
                assert len(
                    current) <= 1, f"There is not unique method for '{result}', but options are '{current}'"
                if len(current) == 0:
                    break
                elif len(current) == 1:
                    _next = list(current.keys())[0]
                    result.append(_next)
                    current = current[_next]

            return {**all_methods.get(tuple(result)), "full_id": result}

        for alias, method in methods.items():
            method_tuple = tuple(method.id)
            bw_method = all_methods.get(method_tuple)
            method.full_id = method_tuple
            if not bw_method:
                bw_method = tree_search(method_tuple)
                if bw_method:
                    method.full_id = tuple(bw_method["full_id"])

            assert bw_method, f"Method with id: {method_tuple} does not exist"
            method.bw_method = BWMethod(**bw_method)

    def _get_activity(self, alias_or_id: Union[str, ExperimentActivityId]) -> Optional[ExtendedExperimentActivityData]:
        if isinstance(alias_or_id, str):
            return self.activitiesMap.get(alias_or_id, None)
        elif isinstance(alias_or_id, ExperimentActivityId):
            for activity in self.activitiesMap.values():
                if activity.orig_id == alias_or_id:
                    return activity

    def validate_scenarios(self, defined_activities: list[ExtendedExperimentActivityData]) -> list[Scenario]:
        """

        :param defined_activities:
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
                    activity = self._get_activity(activity_id)
                    assert activity
                    output: float = Experiment.validate_output(activity_output, activity)
                    activity_outputs[activity.bw_activity._document.code] = (activity.bw_activity, output)
            elif isinstance(activities, dict):
                for activity_alias, activity_output in activities.items():
                    activity = self._get_activity(activity_alias)
                    assert activity
                    output: float = Experiment.validate_output(activity_output, activity)
                    activity_outputs[activity.bw_activity._document.code] = (activity.bw_activity, output)
            return activity_outputs

        def validate_scenario(scenario: ExperimentScenarioData) -> Scenario:
            """
            Validate one scenario
            :param scenario:
            :return:
            """
            scenario_activities_outputs: Activity_Outputs = validate_activities(scenario)
            for activity in self.activitiesMap.values():
                activity_code = activity.bw_activity._document.code
                if activity_code not in scenario_activities_outputs:
                    scenario_activities_outputs[activity_code] = self.default_activities_outputs[activity_code]
            return Scenario(alias=scenario.alias,
                            activities_outputs=scenario_activities_outputs)

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
            default_scenario = ExperimentScenarioData(alias="default scenario")
            scenarios.append(validate_scenario(default_scenario))

        return scenarios

    def create_technology_tree(self) -> BasicTreeNode[ScenarioResultNodeData]:
        tech_tree: BasicTreeNode = BasicTreeNode[ScenarioResultNodeData].from_dict(self.raw_data.hierarchy,
                                                                                   compact=True,
                                                                                   data_factory=lambda e: dict())
        for leaf in tech_tree.get_leaves():
            leaf._data = self._get_activity(leaf.name)
        return tech_tree

    def get_scenario(self, scenario_name: str) -> Optional[Scenario]:
        for scenario in self.scenarios:
            if scenario.alias == scenario_name:
                return scenario
        raise f"Scenario '{scenario_name}' not found"

    def create_bw_calculation_setup(self, scenario: Scenario) -> BW_CalculationSetup:
        inventory: list[dict[Activity, float]] = []
        for act_out in scenario.activities_outputs.values():
            inventory.append({act_out[0]: act_out[1]})
        methods = [m.full_id for m in self.methods.values()]

        return BW_CalculationSetup(scenario.alias, inventory, methods)

    def method_ids(self) -> list[tuple[str]]:
        return [m.full_id for m in self.methods.values()]

    def run_scenario(self, scenario_name: str) -> BasicTreeNode[ScenarioResultNodeData]:
        scenario = self.get_scenario(scenario_name)

        logger.info(f"Running scenario '{scenario.alias}'")
        bw_calc_setup = self.create_bw_calculation_setup(scenario)
        bw_calc_setup.register()
        scenario.results = MultiLCA(bw_calc_setup.name).results
        scenario.result_tree = self.technology_root_node.copy()

        scenario.add_results_to_technology_tree(self.method_ids())
        scenario.resolve_result_tree()
        return scenario.result_tree

    def run(self) -> dict[str, BasicTreeNode[ScenarioResultNodeData]]:
        results = {}
        for scenario in self.scenarios:
            results[scenario.alias] = self.run_scenario(scenario.alias)
        return results

    @staticmethod
    def result_tree_serializer(data: ScenarioResultNodeData):
        return {
            "_".join(method_tuple): value
            for method_tuple, value in data.items()
        }

    def results_to_csv(self, file_path: Path, scenario_name: Optional[str] = None):
        if not scenario_name:
            if len(self.scenarios) > 1:
                raise ValueError("More than one scenario defined, please specify scenario_name")
            scenario = self.scenarios[0]
        else:
            scenario = filter(lambda s: s.alias == scenario_name, self.scenarios)
            assert scenario, f"Scenario '{scenario_name}' not found"
            scenario = next(scenario)

        if not scenario.result_tree:
            raise ValueError(f"Scenario '{scenario_name}' has no results")
        scenario.result_tree.to_csv(file_path, include_data=True, data_serializer=Experiment.result_tree_serializer)


if __name__ == "__main__":
    scenario_data = {
        "bw_project": "uab_bw_ei39",
        "activities_config": {
            "default_database": "ei391"
        },
        "activities": {
            "single_activity": {
                "id": {
                    "name": "heat and power co-generation, wood chips, 6667 kW, state-of-the-art 2014",
                    "unit": "kilowatt hour",
                    "location": "DK"
                },
                "output": [
                    "MWh",
                    30
                ]
            }
        },
        "methods": [
            {
                "id": (
                    "Crustal Scarcity Indicator 2020",
                    "material resources: metals/minerals"
                )
            },
            {
                "id": ('EDIP 2003 no LT', 'non-renewable resources no LT', 'zinc no LT')
            }
        ],
        "hierarchy": {
            "energy": [
                "single_activity"
            ]
        }
    }
    exp_data = ExperimentData(**scenario_data)
    exp = Experiment(exp_data)
    result_tree = [(exp.run()).values()][0]
    # pickle.dump(exp, Path("test.pickle").open("wb"))

    # result_tree = pickle.load(Path("test.pickle").open("rb"))
    exp.results_to_csv(Path("test.csv"))
    print("done")
