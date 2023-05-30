import logging
import sys
from typing import Optional, Union

import bw2data as bd
from pint import UnitRegistry

from enbios2.models.experiment_models import ExperimentActivitiesGlobalConf, ExperimentActivityId, \
    ExtendedExperimentActivity, \
    ExperimentActivity, BWMethod, ExperimentMethod, ExperimentHierarchyNode, \
    ExperimentHierarchy, ExperimentScenario, ExperimentData, ExtendedExperimentActivityOutput


logger = logging.Logger(__name__)
# add console.stream handler
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel("INFO")


class Experiment:
    ureg = UnitRegistry()

    def __init__(self, raw_data: ExperimentData):
        if raw_data.bw_project in bd.projects:
            bd.projects.set_current(raw_data.bw_project)
        self.raw_data = raw_data
        self.ureg = UnitRegistry()

        # alias to activity
        self.activitiesMap: dict[str, ExtendedExperimentActivity] = {}

        output_required = not raw_data.scenarios
        self.validate_activities(output_required)

        self.methods: dict[str, ExperimentMethod] = self.prepare_methods()
        self.validate_methods(self.methods)

        self.validate_hierarchies()
        self.validate_scenarios(list(self.activitiesMap.values()))

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
            activities_list: list[ExperimentActivity] = activities

            for activity in activities_list:
                ext_activity = activity.check_exist(default_id_data, required_output)
                self.activitiesMap[ext_activity.id.alias] = ext_activity
                # assert activity.id.alias not in self.activitiesMap, f"Duplicate activity. {activity.id.alias} exists already. Try giving it a specific alias"
                # self.activitiesMap[activity.id.alias] = ext_activity
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
            activity: ExtendedExperimentActivity = activity
            unique_activities.add((activity.id.database, activity.id.code))
            if activity.output:
                Experiment.validate_output(activity.output, activity)

        assert len(unique_activities) == len(activities), "Not all activities are unique"

    def collect_orig_ids(self) -> list[tuple[ExperimentActivityId, ExtendedExperimentActivity]]:
        return [(activity.orig_id, activity) for activity in self.activitiesMap.values()]

    @staticmethod
    def validate_output(target_output: ExtendedExperimentActivityOutput, activity: ExtendedExperimentActivity) -> None:
        try:
            pint_target_unit = Experiment.ureg[target_output.unit]
            pint_target_quantity = target_output.magnitude * pint_target_unit
            #
            pint_activity_unit = Experiment.ureg[activity.id.unit]
            #
            target_output.pint_quantity = pint_target_quantity.to(pint_activity_unit)
        except Exception as err:
            # todo, change to Exception, and catch that in test too,
            # raise Exception(f"Unit error, {err}; For activity: {activity.id}")
            assert False, f"Unit error, {err}; For activity: {activity.id}"

    def prepare_methods(self) -> dict[str, ExperimentMethod]:
        """
        give all methods some alias and turn them into a dict
        :return: map of alias -> method
        """
        if isinstance(self.raw_data.methods, dict):
            method_dict: dict[str, ExperimentMethod] = self.raw_data.methods
            for method_alias, method in method_dict.items():
                assert method.alias is None or method_alias == method.alias, f"Method: {method} must either have NO alias or the same as the key"
                method.alias = method_alias
            return method_dict
        elif isinstance(self.raw_data.methods, list):
            method_list: list[ExperimentMethod] = self.raw_data.methods
            method_dict: dict[str, ExperimentMethod] = {}
            for method in method_list:
                if not method.alias:
                    method.alias = "_".join(method.id)
                method_dict[method.alias] = method
            return method_dict

    def validate_methods(self, methods: dict[str, ExperimentMethod]):
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

            return all_methods.get(tuple(result))

        for alias, method in methods.items():
            method_tuple = tuple(method.id)
            bw_method = all_methods.get(method_tuple)
            if not bw_method and len(method_tuple) < 3:
                bw_method = tree_search(method_tuple)

            assert bw_method, f"Method with id: {method_tuple} does not exist"
            method.bw_method = BWMethod(**bw_method)

    def validate_hierarchies(self):

        hierarchies = self.raw_data.hierarchy

        if not hierarchies:
            return

        orig_activities_ids: list[tuple[ExperimentActivityId, ExtendedExperimentActivity]] = self.collect_orig_ids()

        def check_hierarchy(hierarchy: ExperimentHierarchy):
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

    def _get_activity(self, alias_or_id: Union[str, ExperimentActivityId]) -> Optional[ExtendedExperimentActivity]:
        if isinstance(alias_or_id, str):
            return self.activitiesMap.get(alias_or_id, None)
        elif isinstance(alias_or_id, ExperimentActivityId):
            for activity in self.activitiesMap.values():
                if activity.orig_id == alias_or_id:
                    return activity

    def validate_scenarios(self, defined_activities: list[ExtendedExperimentActivity]):
        """

        :param defined_activities:
        :return:
        """

        def validate_activities(scenario: ExperimentScenario):
            activities = scenario.activities
            # two Union types,
            # 1. list of tuple: alias | ActivityIds -> unit
            # 2. dict: alias -> unit
            # turn to alias dict
            if isinstance(activities, list):
                for activity in activities:
                    activity_id, activity_output = activity
                    activity = self._get_activity(activity_id)
                    assert activity
                    Experiment.validate_output(activity_output, activity)
            elif isinstance(activities, dict):
                for activity_alias, activity_output in activities.items():
                    activity = self._get_activity(activity_alias)
                    assert activity
                    Experiment.validate_output(activity_output, activity)

        def validate_scenario(scenario: ExperimentScenario, generate_name_index: Optional[int] = None):
            """
            Validate one scenario
            :param scenario:
            :return:
            """
            validate_activities(scenario)

        scenarios = self.raw_data.scenarios

        # from Union, list or dict
        if isinstance(scenarios, list):
            scenarios: list[ExperimentScenario] = self.raw_data.scenarios
            for index, _scenario in enumerate(scenarios):
                if not _scenario.alias:
                    _scenario.alias = ExperimentScenario.alias_factory(index)
                validate_scenario(_scenario, index)
            pass
        elif isinstance(self.raw_data.scenarios, dict):
            scenarios: dict[str, ExperimentScenario] = self.raw_data.scenarios
            for alias, _scenario in scenarios.items():
                if _scenario.alias is not None and _scenario.alias != alias:
                    assert False, f"Scenario defines alias as dict-key: {alias} but also in the scenario object: {_scenario.alias}"
                _scenario.alias = alias
                validate_scenario(_scenario)
        elif not scenarios:
            # todo. one scenario
            return
