from dataclasses import dataclass, field
from typing import Optional, Union, Any

import bw2data as bd
from bw2calc import MultiLCA
from bw2data.backends import Activity
from pint import UnitRegistry

from enbios2.generic.enbios2_logging import get_logger
from enbios2.generic.tree.basic_tree import BasicTreeNode
from enbios2.models.experiment_models import ExperimentActivitiesGlobalConf, ExperimentActivityId, \
    ExtendedExperimentActivityData, \
    ExperimentActivityData, BWMethod, ExperimentMethodData, ExperimentHierarchyNode, \
    ExperimentHierarchyData, ExperimentScenarioData, ExperimentData, ExtendedExperimentActivityOutput
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
    results: Optional[Any] = None
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

        # alias to activity
        self.activitiesMap: dict[str, ExtendedExperimentActivityData] = {}
        self.default_activities_outputs: Activity_Outputs = {}

        output_required = not raw_data.scenarios
        self.validate_activities(output_required)

        self.methods: dict[str, ExperimentMethodData] = self.prepare_methods()
        self.validate_methods(self.methods)

        self.validate_hierarchies()
        self.scenarios: list[Scenario] = self.validate_scenarios(list(self.activitiesMap.values()))
        self.next_scenario_index = 0

        self.technology_root_node: BasicTreeNode[ScenarioResultNodeData] = self.create_technology_tree()

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

    def validate_hierarchies(self):

        hierarchies = self.raw_data.hierarchy

        if not hierarchies:
            return

        orig_activities_ids: list[tuple[ExperimentActivityId, ExtendedExperimentActivityData]] = self.collect_orig_ids()

        def check_hierarchy(hierarchy: ExperimentHierarchyData):
            def rec_find_leaf(node: ExperimentHierarchyNode) -> list[ExperimentActivityId]:
                """
                find all leafs of  a node
                :param node:
                :return:
                """
                # print(node)
                if isinstance(node.children, dict):
                    # merge all leafs of children
                    leafs = []
                    for child in node.children.values():
                        # todo, if only key, and value: None, it shouold be a leaf activity-id
                        leafs.extend(rec_find_leaf(child))
                    return leafs
                elif isinstance(node.children, list):
                    result: list[ExperimentActivityId] = []
                    for child in node.children:
                        assert isinstance(child, str)
                        assert (activity_ := self._get_activity(child))
                        result.append(activity_.orig_id)
                    return result

            rec_find_leaf(hierarchy.root)
            # all_aliases = [leaf.alias for leaf in leafs]

        if isinstance(hierarchies, list):
            # all activities must be in the hierarchy
            assert len(set(h.name for h in hierarchies)) == len(hierarchies), "Hierarchy names must be unique"
            for _hierarchy in hierarchies:
                check_hierarchy(_hierarchy)
        else:
            check_hierarchy(hierarchies)

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
                scenarios.append(validate_scenario(_scenario, index))
        elif isinstance(self.raw_data.scenarios, dict):
            raw_scenarios: dict[str, ExperimentScenarioData] = self.raw_data.scenarios
            for alias, _scenario in raw_scenarios.items():
                if _scenario.alias is not None and _scenario.alias != alias:
                    assert False, f"Scenario defines alias as dict-key: {alias} but also in the scenario object: {_scenario.alias}"
                _scenario.alias = alias
                scenarios.append(validate_scenario(_scenario))
        elif not raw_scenarios:
            default_scenario = ExperimentScenarioData(alias="default scenario")
            scenarios.append(validate_scenario(default_scenario))

        return scenarios

    def create_technology_tree(self) -> BasicTreeNode[ScenarioResultNodeData]:
        """
        Build a tree of all technologies
        :return:
        """
        hierarchy = self.raw_data.hierarchy
        if isinstance(hierarchy, list):
            raise "Currently only one hierarchy is supported"

        def recursive_get_children(
                children_data: Union[ExperimentHierarchyNode, list, dict[str, ExperimentHierarchyNode]]) -> list[
            BasicTreeNode[ScenarioResultNodeData]]:
            child_nodes: list[BasicTreeNode] = []
            if isinstance(children_data, dict):
                for child_name, child_data in children_data.items():
                    child_node = BasicTreeNode(name=child_name, data={})
                    child_nodes.append(child_node)
                    child_node.add_children(recursive_get_children(child_data))
            else:
                # this could be str, or ExperimentActivityId
                children_data: ExperimentHierarchyNode = children_data
                for activity_id in children_data.children:
                    # todo, this could be also of type "ExperimentActivityId" (not just str)
                    activity_node = BasicTreeNode(name=activity_id, data={})
                    activity_node._data = self._get_activity(activity_id)
                    child_nodes.append(activity_node)
            return child_nodes

        root_node = BasicTreeNode[ScenarioResultNodeData](name="root", data={})
        root_node.add_children(recursive_get_children(hierarchy.root.children))
        return root_node

    def get_next_scenario(self) -> Scenario:
        return self.scenarios[self.next_scenario_index]

    def create_bw_calculation_setup(self, scenario: Scenario) -> BW_CalculationSetup:
        inventory: list[dict[Activity, float]] = []
        for act_out in scenario.activities_outputs.values():
            inventory.append({act_out[0]: act_out[1]})
        methods = [m.full_id for m in self.methods.values()]

        return BW_CalculationSetup(scenario.alias, inventory, methods)

    def method_ids(self) -> list[tuple[str]]:
        return [m.full_id for m in self.methods.values()]

    def run_next_scenario(self):
        scenario = self.get_next_scenario()
        bw_calc_setup = self.create_bw_calculation_setup(scenario)
        bw_calc_setup.register()
        scenario.results = MultiLCA(bw_calc_setup.name).results
        scenario.result_tree = self.technology_root_node.copy()

        scenario.add_results_to_technology_tree(self.method_ids())
        scenario.resolve_result_tree()
        self.next_scenario_index += 1


if __name__ == "__main__":
    scenario_data = {
        "bw_project": "ecoi_dbs",
        "activities_config": {
            "default_database": "cutoff391"
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
            "root": {
                "children": {
                    "energy": {
                        "children": [
                            "single_activity"
                        ]
                    }
                }
            }
        }
    }
    exp_data = ExperimentData(**scenario_data)
    exp = Experiment(exp_data)
    exp.run_next_scenario()
