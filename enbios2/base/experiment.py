from dataclasses import asdict
from pathlib import Path
from typing import Optional, Union, Any

import bw2data as bd
import plotly.graph_objects as go
from pint import Quantity

from enbios2.base.db_models import BWProjectIndex
from enbios2.base.scenario import Scenario
from enbios2.base.stacked_MultiLCA import StackedMultiLCA
from enbios2.base.unit_registry import ureg
from enbios2.ecoinvent.ecoinvent_index import get_ecoinvent_dataset_index
from enbios2.generic.enbios2_logging import get_logger
from enbios2.generic.tree.basic_tree import BasicTreeNode
from enbios2.models.experiment_models import (ExperimentActivityId,
                                              ExtendedExperimentActivityData,
                                              BWMethod, ExperimentMethodData,
                                              ExperimentScenarioData, ExperimentData,
                                              EcoInventSimpleIndex, MethodsDataTypes, ActivitiesDataTypes,
                                              ExtendedExperimentActivityPrepData, ScenarioResultNodeData,
                                              ExperimentMethodPrepData, ActivityOutput, SimpleScenarioActivityId,
                                              Activity_Outputs, ExtendedExperimentMethodData)

logger = get_logger(__file__)


class Experiment:
    DEFAULT_SCENARIO_ALIAS = "default scenario"

    def __init__(self, raw_data: ExperimentData):
        self.raw_data = raw_data
        # alias to activity

        self.validate_bw_config()
        self.activitiesMap: dict[str, ExtendedExperimentActivityPrepData] = self.validate_activities(
            self.raw_data.activities)
        self.technology_root_node: BasicTreeNode[ScenarioResultNodeData] = self.create_technology_tree()

        self.methods: dict[str, ExperimentMethodPrepData] = Experiment.validate_methods(self.prepare_methods())
        self.scenarios: list[Scenario] = self.validate_scenarios()
        self.lca: Optional[StackedMultiLCA] = None

    # @staticmethod
    # def create(bw_project: str):
    #     return Experiment(ExperimentData(bw_project=bw_project, activities=[], methods=[]))
    #
    # def add_activity(self, activity: Activity, default_demand: Optional[ExperimentActivityOutput] = None):
    #     if len(self.scenarios) == 1 and not default_demand:
    #         raise ValueError("No default demand specified / and no scenarios added yet")
    #     alias = activity["name"]
    #     if alias in self.activitiesMap:
    #         raise ValueError(f"Activity with alias {alias} already exists")
    #     self.activitiesMap[alias] = ExtendedExperimentActivityData(
    #         id=ExperimentActivityId(
    #             alias=alias,
    #             code=activity["code"],
    #             database=activity["database"]
    #         ),
    #         output=default_demand,
    #         bw_activity=activity
    #     )
    #
    # def add_method(self, method: tuple[str, ...], alias: Optional[str] = None):
    #     if not alias:
    #         alias = "_".join(method)
    #     m_data = ExperimentMethodData(id=method, alias=alias)
    #     bw_method = Experiment.validate_method(m_data)
    #     self.methods[alias] = ExperimentMethodPrepData(id=method, alias=alias, bw_method=bw_method)

    def validate_bw_config(self) -> None:

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

            if not ecoinvent_index.bw_project_index:
                raise ValueError(f"Ecoinvent index {ecoinvent_index}, has not BWProject index")
            bw_project_index: BWProjectIndex = ecoinvent_index.bw_project_index
            validate_bw_project_bw_database(bw_project_index.project_name, bw_project_index.database_name)

    def validate_activities(self, activities: ActivitiesDataTypes, output_required: bool = False) -> dict[
        str, ExtendedExperimentActivityPrepData]:
        """
        Check if all activities exist in the bw database, and check if the given activities are unique
        In case there is only one scenario, all activities are required to have outputs
        """
        # if activities is a list, convert validate and convert to dict
        default_id_data = ExperimentActivityId(database=self.raw_data.bw_default_database)

        identified_activities: dict[str, ExtendedExperimentActivityData] = {}

        if isinstance(activities, list):
            logger.debug("activity list")
            for activity in activities:
                ext_activity: ExtendedExperimentActivityData = activity.check_exist(default_id_data, output_required)
                identified_activities[ext_activity.alias] = ext_activity
        elif isinstance(activities, dict):
            logger.debug("activity dict")
            for activity_alias, activity in activities.items():
                default_id_data.alias = activity_alias
                ext_activity: ExtendedExperimentActivityData = activity.check_exist(default_id_data,  # type: ignore
                                                                                    output_required)
                identified_activities[ext_activity.alias] = ext_activity

        # all codes should only appear once
        unique_activities = set()
        for ext_activity_ in identified_activities.values():
            unique_activities.add((ext_activity_.id.database, ext_activity_.id.code))
            if ext_activity_.output:
                ext_activity_.default_output_value = Experiment.validate_output(ext_activity_.output,
                                                                                # type: ignore
                                                                                ext_activity_)
        assert len(unique_activities) == len(activities), "Not all activities are unique"

        return {
            alias: ExtendedExperimentActivityPrepData(**asdict(act))
            for alias, act in identified_activities.items()
        }

    @staticmethod
    def validate_output(target_output: ActivityOutput,
                        activity: Union[ExtendedExperimentActivityData,
                        ExtendedExperimentActivityPrepData]) -> float:
        """
        validate and convert to the bw-activity unit
        :param target_output:
        :param activity:
        :return:
        """
        try:
            target_quantity: Quantity = ureg.parse_expression(
                target_output.unit, case_sensitive=False) * target_output.magnitude
            bw_activity_unit = activity.bw_activity['unit']
            return target_quantity.to(bw_activity_unit).magnitude
        except Exception as err:
            raise Exception(f"Unit error, {err}; For activity: {activity.id}")

    def prepare_methods(self, methods: Optional[MethodsDataTypes] = None) -> dict[str, ExtendedExperimentMethodData]:
        if not methods:
            methods = self.raw_data.methods
        method_dict: dict[str, ExtendedExperimentMethodData] = {}
        if isinstance(methods, dict):
            for method_alias in methods:
                method_dict[method_alias] = ExtendedExperimentMethodData(methods[method_alias], method_alias)
        elif isinstance(self.raw_data.methods, list):
            method_list: list[ExperimentMethodData] = self.raw_data.methods
            for method_ in method_list:
                method__ = ExtendedExperimentMethodData(method_.id, method_.alias)
                method_dict[method__.alias] = method__
        return method_dict

    @staticmethod
    def validate_method(method: Union[ExperimentMethodData, ExtendedExperimentMethodData]) -> BWMethod:
        method.id = tuple(method.id)
        bw_method = bd.methods.get(method.id)
        if not bw_method:
            raise Exception(f"Method with id: {method.id} does not exist")
        return BWMethod(**bw_method)

    @staticmethod
    def validate_methods(method_dict: dict[str, ExtendedExperimentMethodData]) -> dict[
        str, ExperimentMethodPrepData]:
        # all methods must exist
        return {
            alias: ExperimentMethodPrepData(id=method.id, alias=alias, bw_method=Experiment.validate_method(method))
            for alias, method in method_dict.items()
        }

    def has_activity(self,
                     alias_or_id: Union[str, ExperimentActivityId]) -> Optional[ExtendedExperimentActivityPrepData]:
        if isinstance(alias_or_id, str):
            activity = self.activitiesMap.get(alias_or_id, None)
            return activity
        elif isinstance(alias_or_id, ExperimentActivityId):
            for activity in self.activitiesMap.values():
                if activity.orig_id == alias_or_id:
                    return activity
            return None

    def get_activity(self,
                     alias_or_id: Union[str, ExperimentActivityId]) -> ExtendedExperimentActivityPrepData:
        activity = self.has_activity(alias_or_id)
        if not activity:
            raise ValueError(f"Activity with id {alias_or_id} not found")
        return activity

    def validate_scenarios(self) -> list[Scenario]:
        """
        :return:
        """

        def validate_activity_id(activity_id: Union[str, ExperimentActivityId]) -> SimpleScenarioActivityId:
            activity = self.get_activity(activity_id)
            id_ = activity.id
            assert id_.name and id_.code and id_.alias
            return SimpleScenarioActivityId(name=id_.name,
                                            alias=id_.alias,
                                            code=id_.code)

        def validate_activities(scenario_: ExperimentScenarioData) -> Activity_Outputs:
            activities = scenario_.activities
            result: dict[SimpleScenarioActivityId, float] = {}

            def convert_output(output) -> ActivityOutput:
                if isinstance(output, tuple):
                    return ActivityOutput(unit=activity.bw_activity['unit'],
                                          magnitude=output[1])
                else:
                    return output  # type: ignore

            if not activities:
                return result

            scenarios_activities = activities if isinstance(activities, list) else activities.items()
            for activity_id, activity_output in scenarios_activities:
                activity = self.get_activity(activity_id)
                simple_id = validate_activity_id(activity_id)
                output_ = convert_output(activity_output)
                scenario_output = Experiment.validate_output(output_, activity)
                result[simple_id] = scenario_output
            return result

        def validate_scenario(scenario: ExperimentScenarioData, _scenario_alias: str) -> Scenario:
            """
            Validate one scenario
            :param scenario:
            :param _scenario_alias:
            :return:
            """
            scenario_activities_outputs: Activity_Outputs = validate_activities(scenario)
            # prepared_methods: dict[str, ExperimentMethodData] = {}
            # fill up the missing activities with default values
            for activity in self.activitiesMap.values():
                activity_alias = activity.alias
                if activity_alias not in scenario_activities_outputs:
                    # print(activity)
                    id = SimpleScenarioActivityId(
                        name=str(activity.id.name),
                        code=str(activity.id.code),
                        alias=activity.alias)
                    scenario_activities_outputs[id] = activity.default_output_value

            resolved_methods: dict[str, ExperimentMethodPrepData] = {}
            if scenario.methods:
                if isinstance(scenario.methods, list):
                    for index_, method_ in enumerate(scenario.methods):
                        if isinstance(method_, str):
                            global_method = self.methods.get(method_)
                            assert global_method
                            resolved_methods[global_method.alias] = global_method
                else:
                    method_dict: dict[str, tuple[str, ...]] = scenario.methods
                    for method_alias, method_ in method_dict.items():
                        md = ExperimentMethodData(method_)
                        resolved_methods[md.alias] = ExperimentMethodPrepData(**asdict(md),
                                                                              bw_method=self.validate_method(md))

            return Scenario(experiment=self,  # type: ignore
                            alias=_scenario_alias,
                            activities_outputs=scenario_activities_outputs,
                            methods=resolved_methods,
                            result_tree=self.technology_root_node.copy())

        raw_scenarios = self.raw_data.scenarios
        scenarios: list[Scenario] = []

        # from Union, list or dict
        if isinstance(raw_scenarios, list):
            raw_list_scenarios: list[ExperimentScenarioData] = raw_scenarios
            for index, _scenario in enumerate(raw_list_scenarios):
                _scenario_alias = _scenario.alias if _scenario.alias else ExperimentScenarioData.alias_factory(index)
                scenarios.append(validate_scenario(_scenario, _scenario_alias))
        elif isinstance(raw_scenarios, dict):
            raw_dict_scenarios: dict[str, ExperimentScenarioData] = raw_scenarios
            for alias, _scenario in raw_dict_scenarios.items():
                if _scenario.alias is not None and _scenario.alias != alias:
                    assert False, (f"Scenario defines alias as dict-key: {alias} but "
                                   f"also in the scenario object: {_scenario.alias}")
                _scenario.alias = alias
                scenarios.append(validate_scenario(_scenario, alias))
        elif not raw_scenarios:
            default_scenario = ExperimentScenarioData()
            scenarios.append(validate_scenario(default_scenario, Experiment.DEFAULT_SCENARIO_ALIAS))

        for scenario in scenarios:
            scenario.prepare_tree()
        return scenarios

    def create_technology_tree(self) -> BasicTreeNode[ScenarioResultNodeData]:
        if not self.raw_data.hierarchy:
            self.raw_data.hierarchy = list(self.activitiesMap.keys())

        tech_tree: BasicTreeNode[ScenarioResultNodeData] = (BasicTreeNode.from_dict(self.raw_data.hierarchy,
                                                                                    compact=True))
        for leaf in tech_tree.get_leaves():
            leaf._data = {"activity": self.get_activity(leaf.name)}
        return tech_tree

    def get_scenario(self, scenario_name: str) -> Scenario:
        for scenario in self.scenarios:
            if scenario.alias == scenario_name:
                return scenario
        raise ValueError(f"Scenario '{scenario_name}' not found")

    def run_scenario(self, scenario_name: str) -> dict[str, Any]:
        return self.get_scenario(scenario_name).run().as_dict(include_data=True)

    def run(self) -> dict[str, dict[str, Any]]:
        results: dict[str, dict[str, Any]] = {}
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

    def results_to_plot(self,
                        method_: tuple[str, ...],
                        *,
                        scenario_name: Optional[str] = None,
                        image_file: Optional[Path] = None,
                        show: bool = False):

        scenario = self.get_scenario(scenario_name) if scenario_name else self.scenarios[0]
        if not scenario.result_tree:
            logger.info(f"Scenario '{scenario.alias}' has no results to plot")
            return
        all_nodes = list(scenario.result_tree.iter_all_nodes())
        node_labels = [node.name for node in all_nodes]

        source = []
        target = []
        value = []
        for index_, node in enumerate(all_nodes):
            for child in node.children:
                source.append(index_)
                target.append(all_nodes.index(child))
                if child.data:
                    value.append(child.data.results[method_])
                else:
                    raise ValueError(f"Node '{child.name}' has no data")

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

        fig.update_layout(title_text=f"{scenario.alias} / {'_'.join(method_)}", font_size=10)
        if show:
            fig.show()
        if image_file:
            fig.write_image(image_file.as_posix(), width=1800, height=1600)
