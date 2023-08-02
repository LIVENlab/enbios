import itertools
from copy import copy
from dataclasses import asdict
from typing import Optional, Union, Any, cast

import bw2data as bd
import numpy as np
from bw2data.backends import Activity
from pint import Quantity, UndefinedUnitError, DimensionalityError
from pydantic import ValidationError

from enbios2.base.db_models import BWProjectIndex
from enbios2.base.scenario import Scenario
from enbios2.base.stacked_MultiLCA import StackedMultiLCA
from enbios2.base.unit_registry import ureg
from enbios2.bw2.util import get_activity
from enbios2.ecoinvent.ecoinvent_index import get_ecoinvent_dataset_index
from enbios2.generic.enbios2_logging import get_logger
from enbios2.generic.files import PathLike
from enbios2.generic.tree.basic_tree import BasicTreeNode
from enbios2.models.experiment_models import (ExperimentActivityId,
                                              ExtendedExperimentActivityData,
                                              BWMethod, ExperimentMethodData,
                                              ExperimentScenarioData, ExperimentData,
                                              EcoInventSimpleIndex, MethodsDataTypes, ActivitiesDataTypes,
                                              ExtendedExperimentActivityPrepData, ScenarioResultNodeData,
                                              ExperimentMethodPrepData, ActivityOutput, SimpleScenarioActivityId,
                                              Activity_Outputs, BWCalculationSetup, ExperimentActivityData,
                                              ScenarioConfig)

logger = get_logger(__file__)


class Experiment:
    DEFAULT_SCENARIO_ALIAS = "default scenario"

    def __init__(self, raw_data: Union[ExperimentData, dict]):
        if isinstance(raw_data, dict):
            input_data = ExperimentData(**raw_data)
        else:
            input_data = raw_data
        self.raw_data = input_data
        # alias to activity

        self._validate_bw_config()
        self.activitiesMap: dict[str, ExtendedExperimentActivityPrepData] = Experiment._validate_activities(
            self._prepare_activities(self.raw_data.activities), self.raw_data.bw_default_database)
        self._user_defined_hierarchy: bool = True
        self.technology_root_node: BasicTreeNode[ScenarioResultNodeData] = self._create_technology_tree()

        self.methods: dict[str, ExperimentMethodPrepData] = Experiment._validate_methods(self._prepare_methods())
        self.scenarios: list[Scenario] = self._validate_scenarios()
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

    def _validate_bw_config(self) -> None:

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

    @staticmethod
    def _prepare_activities(activities: ActivitiesDataTypes) -> list[ExperimentActivityData]:
        raw_activities_list: list[ExperimentActivityData] = []
        if isinstance(activities, list):
            raw_activities_list = activities
            for activity in raw_activities_list:
                activity.orig_id = copy(activity.id)

        elif isinstance(activities, dict):
            for activity_alias, activity in activities.items():
                if activity_alias == activity.alias:
                    raise (f"Activity in activities-dict declared with alias: '{activity_alias}', "
                           f"different than in the activity.id: '{activity.alias}'")
                activity.orig_id = copy(activity.id)
                activity.id.alias = activity_alias
                raw_activities_list.append(activity)
        return raw_activities_list

    @staticmethod
    def _validate_activities(activities: list[ExperimentActivityData], bw_default_database: str,
                             output_required: bool = False) -> [str,
                                                                ExtendedExperimentActivityData]:
        """
        Check if all activities exist in the bw database, and check if the given activities are unique
        In case there is only one scenario, all activities are required to have outputs
        """
        # if activities is a list, convert validate and convert to dict
        default_id_data = ExperimentActivityId(database=bw_default_database)
        activities_map: [str, ExtendedExperimentActivityData] = {}
        # validate
        for activity in activities:
            ext_activity: ExtendedExperimentActivityData = Experiment._validate_activity(activity,
                                                                                         default_id_data,
                                                                                         output_required)
            # check unique aliases
            if ext_activity.alias in activities_map:
                raise Exception(
                    f"Activity-alias '{ext_activity.alias}' passed more then once. "
                    f"Consider using a dictionary for activities or include 'alias' in the ids.")
            activities_map[ext_activity.alias] = ext_activity

        # all codes should only appear once
        unique_activities = set()
        for ext_activity in activities_map.values():
            unique_activities.add((ext_activity.id.database, ext_activity.id.code))
            if ext_activity.output:
                ext_activity.default_output_value = Experiment._validate_output(ext_activity.output, ext_activity)
        assert len(activities_map) > 0, "There are no activities in the experiment"
        return activities_map

    @staticmethod
    def _validate_activity(activity: ExperimentActivityData,
                           default_id_attr: Optional[ExperimentActivityId] = None,
                           required_output: bool = False) -> "ExtendedExperimentActivityData":
        """
        This method checks if the activity exists in the database by several ways.
        :param activity:
        :param default_id_attr:
        :param required_output:
        :return:
        """

        id_ = activity.id
        database = activity.id.database if activity.id.database else default_id_attr.database
        bw_activity: Optional[Activity] = None
        if id_.code:
            if id_.database:
                bw_activity = bd.Database(id_.database).get_node(id_.code)
            else:
                bw_activity = get_activity(id_.code)
        elif id_.name:
            filters = {}
            search_in_dbs = [id_.database] if id_.database else bd.databases
            for db in search_in_dbs:
                if id_.location:
                    filters["location"] = id_.location
                    search_results = bd.Database(db).search(id_.name, filter=filters)
                else:
                    search_results = bd.Database(db).search(id_.name)
                if id_.unit:
                    search_results = list(filter(lambda a: a["unit"] == id_.unit, search_results))
                if len(search_results) == 1:
                    bw_activity = search_results[0]
                    break
                elif len(search_results) > 1:
                    activities_str = "\n".join([f'{str(a)} - {a["code"]}' for a in search_results])
                    raise ValueError(
                        f"There are more than one activity with the same name, Try including  "
                        f"the code of the activity you want to use:\n{activities_str}")
        if not bw_activity:
            raise ValueError(f"No activity found for {activity.id}")

        if not activity.output:
            try:
                activity.output = ActivityOutput(unit=bw_activity["unit"])
            except ValidationError as err:
                raise ValueError(f"Activity {activity.id} has invalid output format: {err}")

        activity_dict = asdict(activity)
        if output := activity_dict.get("output"):
            if isinstance(output, tuple):
                activity_dict["output"] = asdict(ActivityOutput(unit=output[0], magnitude=output[1]))
        result: ExtendedExperimentActivityData = ExtendedExperimentActivityData(**activity_dict,
                                                                                database=database,
                                                                                bw_activity=bw_activity,
                                                                                default_output=activity.output)
        result.id.fill_empty_fields(["alias"], **asdict(default_id_attr))

        result.id.fill_empty_fields(["name", "code", "location", "unit", ("alias", "name")],
                                    **result.bw_activity.as_dict())
        if required_output:
            assert activity.output is not None, (
                f"Since there is no scenario, activity output is required: {activity.orig_id}")
        return result

    @staticmethod
    def _validate_output(target_output: ActivityOutput,
                         activity: Union[ExtendedExperimentActivityData, ExtendedExperimentActivityPrepData]) -> float:
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
        except UndefinedUnitError as err:
            logger.error(
                f"Cannot parse output unit '{target_output.unit}'- of activity {activity.id}. {err}. "
                f"Consider the unit definition to 'enbios2/base/unit_registry.py'")
            raise Exception(f"Unit error, {err}; For activity: {activity.id}")
        except DimensionalityError as err:
            logger.error(
                f"Cannot convert output of activity {activity.id}. -From- \n{target_output}\n-To-"
                f"\n{activity.bw_activity['unit']} (brightway unit)"
                f"\n{err}")
            raise Exception(f"Unit error for activity: {activity.id}")

    def _prepare_methods(self, methods: Optional[MethodsDataTypes] = None) -> dict[str, ExperimentMethodData]:
        if not methods:
            methods = self.raw_data.methods
        method_dict: dict[str, ExperimentMethodData] = {}
        if isinstance(methods, dict):
            for method_alias, method in methods.items():
                method_dict[method_alias] = ExperimentMethodData(method, method_alias)
        elif isinstance(self.raw_data.methods, list):
            method_list: list[ExperimentMethodData] = self.raw_data.methods
            for method_ in method_list:
                alias = method_.alias if method_.alias else "_".join(method_.id)
                method__ = ExperimentMethodData(method_.id, alias)
                method_dict[method__.alias_] = method__
        return method_dict

    @staticmethod
    def _validate_method(method: ExperimentMethodData,
                         alias: str) -> ExperimentMethodPrepData:
        method.id = tuple(method.id)
        bw_method = bd.methods.get(method.id)
        if not bw_method:
            raise Exception(f"Method with id: {method.id} does not exist")
        if method.alias:
            if method.alias != alias:
                raise Exception(f"Method alias: {method.alias} does not match with the given alias: {alias}")
        else:
            method.alias = alias
        return ExperimentMethodPrepData(id=method.id, alias=method.alias, bw_method=BWMethod(**bw_method))

    @staticmethod
    def _validate_methods(method_dict: dict[str, ExperimentMethodData]) -> dict[str, ExperimentMethodPrepData]:
        # all methods must exist
        return {
            alias: Experiment._validate_method(method, alias)
            for alias, method in method_dict.items()
        }

    def _has_activity(self,
                      alias_or_id: Union[str, ExperimentActivityId]) -> Optional[ExtendedExperimentActivityPrepData]:
        if isinstance(alias_or_id, str):
            activity = self.activitiesMap.get(alias_or_id, None)
            return activity
        else:  # isinstance(alias_or_id, ExperimentActivityId):
            for activity in self.activitiesMap.values():
                if activity.orig_id == alias_or_id:
                    return activity
            return None

    def get_activity(self,
                     alias_or_id: Union[str, ExperimentActivityId]) -> ExtendedExperimentActivityPrepData:
        activity = self._has_activity(alias_or_id)
        if not activity:
            raise ValueError(f"Activity with id {alias_or_id} not found")
        return activity

    def _validate_scenarios(self) -> list[Scenario]:
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
                scenario_output = Experiment._validate_output(output_, activity)
                result[simple_id] = scenario_output
            return result

        def validate_scenario(_scenario: ExperimentScenarioData, _scenario_alias: str) -> Scenario:
            """
            Validate one scenario
            :param _scenario:
            :param _scenario_alias:
            :return:
            """
            scenario_activities_outputs: Activity_Outputs = validate_activities(_scenario)
            defined_aliases = [output_id.alias for output_id in scenario_activities_outputs.keys()]
            # prepared_methods: dict[str, ExperimentMethodData] = {}
            # fill up the missing activities with default values
            for activity in self.activitiesMap.values():
                activity_alias = activity.alias
                if activity_alias not in defined_aliases:
                    # print(activity)
                    id_ = SimpleScenarioActivityId(
                        name=str(activity.id.name),
                        code=str(activity.id.code),
                        alias=activity.alias)
                    scenario_activities_outputs[id_] = activity.default_output_value

            resolved_methods: dict[str, ExperimentMethodPrepData] = {}
            if _scenario.methods:
                if isinstance(_scenario.methods, list):
                    for index_, method_ in enumerate(_scenario.methods):
                        if isinstance(method_, str):
                            global_method = self.methods.get(method_)
                            assert global_method
                            resolved_methods[global_method.alias] = global_method
                else:
                    method_dict: dict[str, tuple[str, ...]] = cast(dict[str, tuple[str, ...]], _scenario.methods)
                    for method_alias, method_ in method_dict.items():  # type: ignore
                        md = ExperimentMethodData(id=cast(tuple[str, ...], method_))
                        prep_method = self._validate_method(md, method_alias)
                        resolved_methods[prep_method.alias] = prep_method

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
        # undefined scenarios. just one default scenario
        elif not raw_scenarios:
            default_scenario = ExperimentScenarioData()
            scenarios.append(validate_scenario(default_scenario, Experiment.DEFAULT_SCENARIO_ALIAS))

        for scenario in scenarios:
            scenario.prepare_tree(self._config.include_bw_activity_in_nodes)
        return scenarios

    def _create_technology_tree(self) -> BasicTreeNode[ScenarioResultNodeData]:
        if not self.raw_data.hierarchy:
            self.raw_data.hierarchy = list(self.activitiesMap.keys())
            self._user_defined_hierarchy = False

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

    def run(self) -> dict[str, BasicTreeNode[ScenarioResultNodeData]]:
        methods = [m.id for m in self.methods.values()]
        inventories: list[list[dict[Activity, float]]] = []
        for scenario in self.scenarios:
            if scenario.methods:
                raise ValueError(f"Scenario cannot have individual methods. '{scenario.alias}'")
            inventory: list[dict[Activity, float]] = []
            for activity_alias, act_out in scenario.activities_outputs.items():
                bw_activity = scenario.experiment.get_activity(activity_alias.alias).bw_activity
                inventory.append({bw_activity: act_out})
            inventories.append(inventory)

        # run experiment
        calculation_setup = BWCalculationSetup("experiment", list(itertools.chain(*inventories)), methods)
        raw_results = StackedMultiLCA(calculation_setup).results
        scenario_results = np.split(raw_results, len(self.scenarios))
        results: dict[str, BasicTreeNode[ScenarioResultNodeData]] = {}
        for index, scenario in enumerate(self.scenarios):
            results[scenario.alias] = scenario.create_results_to_technology_tree(scenario_results[index])
        return results

    def results_to_csv(self,
                       file_path: PathLike,
                       scenario_name: Optional[str] = None,
                       include_method_units: bool = True):
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

    @property
    def _config(self) -> ScenarioConfig:
        return self.raw_data.config

    def __repr__(self):
        return (f"Experiment: (call info() for details)\n"
                f"Activities: {len(self.activitiesMap)}\n"
                f"Methods: {len(self.methods)}\n"
                f"Scenarios: {len(self.scenarios)}\n")

    def info(self):
        activity_rows: list[str] = []
        for activity_alias, activity in self.activitiesMap.items():
            activity_rows.append(f"  {activity.alias} - {activity.id.name}")
        activity_rows_str = "\n".join(activity_rows)
        methods_str = "\n".join([f" {m.id}" for m in self.methods.values()])
        return (f"Experiment: \n"
                f"Activities: {len(self.activitiesMap)}\n"
                f"{activity_rows_str}\n"
                f"Methods: {len(self.methods)}\n"
                f"{methods_str}\n"
                f"Scenarios: {len(self.scenarios)}\n")
