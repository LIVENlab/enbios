from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Union

import bw2data as bd
from bw2calc import MultiLCA
from bw2data.backends import Activity
from deprecated.classic import deprecated
from numpy import ndarray
from pint import UnitRegistry

import plotly.graph_objects as go

from enbios2.bw2.util import method_search
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

    def add_results_to_technology_tree(self, methods_aliases: list[str]):
        """
        Add results to the technology tree, for each method
        :param methods_aliases: tuple of method identifiers
        """
        activity_nodes = self.result_tree.get_leaves()
        activities_aliases = list(self.activities_outputs.keys())

        for result_index, alias in enumerate(activities_aliases):
            bw_activity = self.experiment._get_activity(alias).bw_activity
            activity_node = next(filter(lambda node: node._data.bw_activity == bw_activity, activity_nodes))
            for method_index, method in enumerate(methods_aliases):
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

    def results_to_csv(self, file_path: Path, include_method_units: bool = False):
        """
        Save the results (as tree) to a csv file
         :param file_path:  path to save the results to
         :param include_method_units:
        """
        if not self.result_tree:
            raise ValueError(f"Scenario '{self.alias}' has no results")

        def data_serializer(data: ScenarioResultNodeData) -> dict:
            if not include_method_units:
                return data
            else:
                result = {}
                for method_alias, value in data.items():
                    final_name = f"{method_alias} ({self.experiment.methods[method_alias].bw_method.unit})"
                    result[final_name] = value
                return result

        self.result_tree.to_csv(file_path, include_data=True, data_serializer=data_serializer)


class Experiment:
    ureg = UnitRegistry()

    def __init__(self, raw_data: ExperimentData):
        if raw_data.bw_project in bd.projects:
            bd.projects.set_current(raw_data.bw_project)
        self.raw_data = raw_data
        # alias to activity
        self.activitiesMap: dict[str, ExtendedExperimentActivityData] = {}

        self.validate_bw_config()
        self.validate_activities()
        self.technology_root_node: BasicTreeNode[ScenarioResultNodeData] = self.create_technology_tree()

        self.methods = self.validate_methods()
        self.scenarios: list[Scenario] = self.validate_scenarios(list(self.activitiesMap.values()))

    def validate_bw_config(self):
        if self.raw_data.bw_project not in bd.projects:
            raise Exception(f"Project {self.raw_data.bw_project} not found")
        if self.raw_data.bw_project in bd.projects:
            bd.projects.set_current(self.raw_data.bw_project)

        if self.raw_data.activities_config.default_database:
            if self.raw_data.activities_config.default_database not in bd.databases:
                raise Exception(f"Database {self.raw_data.activities_config.default_database} "
                                f"not found. Options are: {list(bd.databases)}")

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

    def validate_methods(self) -> dict[str, ExperimentMethodData]:
        method_dict: dict[str, ExperimentMethodData] = {}
        if isinstance(self.raw_data.methods, dict):
            method_dict = self.raw_data.methods
            for method_alias, method_ in method_dict.items():
                assert method_.alias is None or method_alias == method_.alias, f"Method: {method_} must either have NO alias or the same as the key"
                method_.alias = method_alias
        elif isinstance(self.raw_data.methods, list):
            method_list: list[ExperimentMethodData] = self.raw_data.methods
            for method_ in method_list:
                if not method_.alias:
                    method_.alias = "_".join(method_.id)
                method_dict[method_.alias] = method_

        # all methods must exist
        all_methods = bd.methods
        for alias, method in method_dict.items():
            method.id = tuple(method.id)
            bw_method = all_methods.get(method.id)
            if not bw_method:
                raise Exception(f"Method with id: {method.id} does not exist")
            method.bw_method = BWMethod(**bw_method)
        return method_dict

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
        methods = [m.id for m in self.methods.values()]
        calculation_setup = BWCalculationSetup(scenario.alias, inventory, methods)
        if register:
            calculation_setup.register()
        return calculation_setup

    def run_scenario(self, scenario_name: str) -> dict:
        scenario = self.get_scenario(scenario_name)

        logger.info(f"Running scenario '{scenario.alias}'")
        bw_calc_setup = self.create_bw_calculation_setup(scenario)
        scenario.results = MultiLCA(bw_calc_setup.name).results
        scenario.result_tree = self.technology_root_node.copy()

        method_aliases = [m.alias for m in self.methods.values()]
        scenario.add_results_to_technology_tree(method_aliases)
        scenario.resolve_result_tree()

        return scenario.result_tree.as_dict(include_data=True)

    def run(self) -> dict[str, dict]:
        results = {}
        for scenario in self.scenarios:
            results[scenario.alias] = self.run_scenario(scenario.alias)
        return results

    def select_scenario(self, scenario_name: Optional[str] = None) -> Scenario:
        if not scenario_name:
            if len(self.scenarios) > 1:
                raise ValueError("More than one scenario defined, please specify scenario_name. taking first one")
            return self.scenarios[0]
        else:
            scenario = filter(lambda s: s.alias == scenario_name, self.scenarios)
            assert scenario, f"Scenario '{scenario_name}' not found"
            return next(scenario)

    def results_to_csv(self, file_path: Path, scenario_name: Optional[str] = None, include_method_units: bool = True):
        scenario = self.select_scenario(scenario_name)
        scenario.results_to_csv(file_path, include_method_units)

    def results_to_plot(self,
                        method: str,
                        *,
                        scenario_name: Optional[str] = None,
                        image_file: Optional[Path] = None,
                        show: bool = False):

        scenario = self.select_scenario(scenario_name)
        # todo refactor that part out...
        all_nodes = list(scenario.result_tree.iter_all_nodes())
        node_labels = [node.name for node in all_nodes]

        source = []
        target = []
        value = []
        for index_, node in enumerate(all_nodes):
            for child in node.children:
                source.append(index_)
                target.append(all_nodes.index(child))
                value.append(child.data[method])

        fig = go.Figure(data=[go.Sankey(
            node=dict(
                pad=15,
                thickness=20,
                line=dict(color="black", width=0.5),
                label=node_labels,
                color="blue"
            ),
            link=dict(
                source=source,
                target=target,
                value=value
            ))])

        fig.update_layout(title_text=f"{scenario.alias} / {'_'.join(method)}", font_size=10)
        if show:
            fig.show()
        if image_file:
            fig.write_image(image_file.as_posix(), width=1800, height=1600)


if __name__ == "__main__":
    scenario_data = {
        "bw_project": "uab_bw_ei39",
        "activities_config": {
            "default_database": "ei39"
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
                "alias": "AX",
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

    for index, method in enumerate(scenario_data["methods"]):
        scenario_data["methods"][index]["id"] = method_search("uab_bw_ei39", method["id"])[0]

    exp_data = ExperimentData(**scenario_data)
    exp = Experiment(exp_data)
    result_tree = [(exp.run()).values()][0]

    exp.results_to_csv(Path("test.csv"))
    # exp.results_to_plot(("Crustal Scarcity Indicator 2020", "material resources: metals/minerals"),
    #                     image_file= Path("plot.png"))
    print("done")
