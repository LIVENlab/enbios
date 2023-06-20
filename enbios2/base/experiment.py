from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Union

import bw2data as bd
from bw2calc import MultiLCA
from bw2data.backends import Activity
from deprecated.classic import deprecated
from numpy import ndarray
from pint import UnitRegistry

from enbios2.generic.enbios2_logging import get_logger
from enbios2.generic.tree.basic_tree import BasicTreeNode
from enbios2.models.experiment_models import (ExperimentActivitiesGlobalConf, ExperimentActivityId,
                                              ExtendedExperimentActivityData,
                                              BWMethod, ExperimentMethodData,
                                              ExperimentScenarioData, ExperimentData,
                                              ExtendedExperimentActivityOutput, BWCalculationSetup)

logger = get_logger(__file__)

ureg = UnitRegistry()

# map from ExtendedExperimentActivityData.alias = (ExtendedExperimentActivityData.id.alias) to outputs
Activity_Outputs = dict[str, float]

"""
dict of Method tuple, to result-value 
"""
ScenarioResultNodeData = dict[tuple[str], float]


@dataclass
class Scenario:
    experiment: "Experiment"
    alias: Optional[str] = None
    # this should be a simpler type - just str: float
    activities_outputs: Activity_Outputs = field(default_factory=dict)
    results: Optional[ndarray] = None
    result_tree: Optional[BasicTreeNode[ScenarioResultNodeData]] = None

    def add_results_to_technology_tree(self, methods_ids: list[tuple[str]]):
        """
        Add results to the technology tree, for each method
        :param methods_ids: tuple of method identifiers
        """
        activity_nodes = self.result_tree.get_leaves()
        activities_aliases = list(self.activities_outputs.keys())

        for result_index, alias in enumerate(activities_aliases):
            bw_activity = self.experiment._get_activity(alias).bw_activity
            activity_node = next(filter(lambda node: node._data.bw_activity == bw_activity, activity_nodes))
            for method_index, method in enumerate(methods_ids):
                activity_node.data[method] = self.results[result_index][method_index]

    def resolve_result_tree(self):
        """
        Sum up the results in the complete hierarchy
        """

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

        # TODO: WHY!?!?
        # def recursive_resolve_node(node: BasicTreeNode[ScenarioResultNodeData]):
        #     for child in node.children:
        #         for key, value in child.data.items():
        #             if node.data.get(key) is None:
        #                 node.data[key] = 0
        #             node.data[key] += value

        # self.result_tree.recursive_apply(recursive_resolve_node, depth_first=True)
        recursive_resolve_node(self.result_tree)

    @staticmethod
    def result_tree_serializer(data: ScenarioResultNodeData):
        """
        :Turn the method ids (tuples) into simple strings
        :param data:
        :return:
        """
        return {
            "_".join(method_tuple): value
            for method_tuple, value in data.items()
        }

    def results_to_csv(self, file_path: Path):
        """
        Save the results (as tree) to a csv file
         :param file_path:  path to save the results to
        """
        if not self.result_tree:
            raise ValueError(f"Scenario '{self.alias}' has no results")
        self.result_tree.to_csv(file_path, include_data=True, data_serializer=self.result_tree_serializer)


class Experiment:
    ureg = UnitRegistry()

    def __init__(self, raw_data: ExperimentData):
        if raw_data.bw_project in bd.projects:
            bd.projects.set_current(raw_data.bw_project)
        self.raw_data = raw_data
        # alias to activity
        self.activitiesMap: dict[str, ExtendedExperimentActivityData] = {}
        # todo, get this from the activitiesMap instead...
        # self.default_activities_outputs: Activity_Outputs = {}

        self.validate_bw_config()
        self.validate_activities()
        self.technology_root_node: BasicTreeNode[ScenarioResultNodeData] = self.create_technology_tree()

        self.methods: dict[str, ExperimentMethodData] = self.prepare_methods()
        self.validate_methods(self.methods)
        self.scenarios: list[Scenario] = self.validate_scenarios(list(self.activitiesMap.values()))

    def validate_bw_config(self):
        if self.raw_data.bw_project not in bd.projects:
            raise Exception(f"Project {self.raw_data.bw_project} not found")
        if self.raw_data.bw_project in bd.projects:
            bd.projects.set_current(self.raw_data.bw_project)

        if self.raw_data.activities_config.default_database:
            if self.raw_data.activities_config.default_database not in bd.databases:
                raise Exception(f"Database {self.raw_data.activities_config.default_database} not found")

    def validate_activities(self):
        """
        Check if all activities exist in the bw database, and check if the given activities are unique
        In case there is only one scenario, all activities are required to have outputs
        """
        output_required = not self.raw_data.scenarios
        # check if all activities exist
        activities = self.raw_data.activities
        config: ExperimentActivitiesGlobalConf = self.raw_data.activities_config

        logger.debug(f"activity-configuration, {config}")
        # if activities is a list, convert validate and convert to dict
        default_id_data = ExperimentActivityId(database=config.default_database)
        if isinstance(activities, list):
            logger.debug("activity list")

            for activity in activities:
                ext_activity: ExtendedExperimentActivityData = activity.check_exist(default_id_data, output_required)
                self.activitiesMap[ext_activity.alias] = ext_activity
        elif isinstance(activities, dict):
            logger.debug("activity dict")
            for activity_alias, activity in activities.items():
                default_id_data.alias = activity_alias
                ext_activity: ExtendedExperimentActivityData = activity.check_exist(default_id_data, output_required)
                self.activitiesMap[ext_activity.alias] = ext_activity

        # all codes should only appear once
        unique_activities = set()
        for ext_activity in self.activitiesMap.values():
            ext_activity: ExtendedExperimentActivityData = ext_activity
            unique_activities.add((ext_activity.id.database, ext_activity.id.code))

            if ext_activity.output:
                ext_activity.default_output_value = Experiment.validate_output(ext_activity.output, ext_activity)

        assert len(unique_activities) == len(activities), "Not all activities are unique"

    @deprecated()
    def collect_orig_ids(self) -> list[tuple[ExperimentActivityId, ExtendedExperimentActivityData]]:
        return [(activity.orig_id, activity) for activity in self.activitiesMap.values()]

    @staticmethod
    def validate_output(target_output: ExtendedExperimentActivityOutput,
                        activity: ExtendedExperimentActivityData) -> float:
        try:
            target_quantity = Experiment.ureg(target_output.unit) * target_output.magnitude
            return target_quantity.to(activity.bw_activity['unit']).magnitude
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
                    activity_outputs[activity.alias] = output
            elif isinstance(activities, dict):
                for activity_alias, activity_output in activities.items():
                    activity = self._get_activity(activity_alias)
                    assert activity
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
            for activity in self.activitiesMap.values():
                activity_alias = activity.alias
                if activity_alias not in scenario_activities_outputs:
                    scenario_activities_outputs[activity.alias] = activity.default_output_value
            return Scenario(experiment=self,
                            alias=scenario.alias,
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

    def create_bw_calculation_setup(self, scenario: Scenario, register: bool = True) -> BWCalculationSetup:
        inventory: list[dict[Activity, float]] = []
        for activity_alias, act_out in scenario.activities_outputs.items():
            bw_activity = self._get_activity(activity_alias).bw_activity
            inventory.append({bw_activity: act_out})
        methods = [m.full_id for m in self.methods.values()]
        calculation_setup = BWCalculationSetup(scenario.alias, inventory, methods)
        if register:
            calculation_setup.register()
        return calculation_setup

    def method_ids(self) -> list[tuple[str]]:
        return [m.full_id for m in self.methods.values()]

    def run_scenario(self, scenario_name: str) -> dict:
        scenario = self.get_scenario(scenario_name)

        logger.info(f"Running scenario '{scenario.alias}'")
        bw_calc_setup = self.create_bw_calculation_setup(scenario)
        scenario.results = MultiLCA(bw_calc_setup.name).results
        scenario.result_tree = self.technology_root_node.copy()

        scenario.add_results_to_technology_tree(self.method_ids())
        scenario.resolve_result_tree()

        return scenario.result_tree.as_dict(include_data=True)

    def run(self) -> dict[str, dict]:
        results = {}
        for scenario in self.scenarios:
            results[scenario.alias] = self.run_scenario(scenario.alias)
        return results

    def results_to_csv(self, file_path: Path, scenario_name: Optional[str] = None):
        if not scenario_name:
            if len(self.scenarios) > 1:
                raise ValueError("More than one scenario defined, please specify scenario_name")
            scenario = self.scenarios[0]
        else:
            scenario = filter(lambda s: s.alias == scenario_name, self.scenarios)
            assert scenario, f"Scenario '{scenario_name}' not found"
            scenario = next(scenario)

        scenario.results_to_csv(file_path)


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
                    1
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
