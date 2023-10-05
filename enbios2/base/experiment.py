import csv
import itertools
import math
import time
from copy import copy
from dataclasses import asdict
from datetime import timedelta
from pathlib import Path
from tempfile import gettempdir
from typing import Any, Optional, Union, cast

import bw2data as bd
import numpy as np
from bw2data.backends import Activity
from pint import DimensionalityError, Quantity, UndefinedUnitError
from pydantic import ValidationError

from enbios2.base.db_models import BWProjectIndex
from enbios2.base.experiment_io import resolve_input_files
from enbios2.base.scenario import Scenario
from enbios2.base.stacked_MultiLCA import StackedMultiLCA
from enbios2.base.unit_registry import ureg
from enbios2.bw2.util import bw_unit_fix, get_activity
from enbios2.ecoinvent.ecoinvent_index import get_ecoinvent_dataset_index
from enbios2.generic.enbios2_logging import get_logger
from enbios2.generic.files import PathLike, ReadPath
from enbios2.generic.tree.basic_tree import BasicTreeNode
from enbios2.models.experiment_models import (ActivitiesDataTypes, ActivityOutput,
                                              Activity_Outputs, BWCalculationSetup,
                                              EcoInventSimpleIndex,
                                              ExperimentActivityData,
                                              ExperimentActivityId, ExperimentConfig,
                                              ExperimentData, ExperimentMethodData,
                                              ExperimentMethodPrepData,
                                              ExperimentScenarioData,
                                              ExtendedExperimentActivityData,
                                              MethodsDataTypes, ScenarioResultNodeData,
                                              Settings, SimpleScenarioActivityId)

logger = get_logger(__file__)


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
            input_data = ExperimentData(**raw_data)
        else:
            input_data = raw_data
        # todo. look at how reading a hierarchy from a csv file or a json
        # creates an additional root node
        resolve_input_files(input_data)
        self.raw_data = ExperimentData(**asdict(input_data))

        # alias to activity
        self._validate_bw_config()
        self._activities: dict[
            str, ExtendedExperimentActivityData
        ] = Experiment._validate_activities(
            self._prepare_activities(self.raw_data.activities),
            self.raw_data.bw_default_database,
        )

        self.raw_data.hierarchy = self._prepare_hierarchy()
        self.hierarchy_root: BasicTreeNode[
            ScenarioResultNodeData
        ] = self.validate_hierarchy(self.raw_data.hierarchy)

        self.methods: dict[str, ExperimentMethodPrepData] = Experiment._validate_methods(
            self._prepare_methods()
        )
        self.scenarios: list[Scenario] = self._validate_scenarios()
        self._validate_run_scenario_setting()
        self._lca: Optional[StackedMultiLCA] = None
        self._execution_time: float = float("NaN")

    def _validate_bw_config(self) -> None:
        if self.config.use_k_bw_distributions < 1:
            raise ValueError(
                f"config.use_k_bw_distributions must be greater than 0, "
                f"but is {self.config.use_k_bw_distributions}"
            )

        def validate_bw_project_bw_database(
            bw_project: str, bw_default_database: Optional[str] = None
        ):
            if bw_project not in bd.projects:
                raise ValueError(f"Project {bw_project} not found")
            if bw_project in bd.projects:
                bd.projects.set_current(bw_project)

            if bw_default_database:
                if bw_default_database not in bd.databases:
                    raise ValueError(
                        f"Database {bw_default_database} "
                        f"not found. Options are: {list(bd.databases)}"
                    )

        # print("validate_bw_config***************", self.raw_data.bw_project)
        if isinstance(self.raw_data.bw_project, str):
            validate_bw_project_bw_database(
                self.raw_data.bw_project, self.raw_data.bw_default_database
            )

        else:
            simple_index: EcoInventSimpleIndex = self.raw_data.bw_project
            ecoinvent_index = get_ecoinvent_dataset_index(
                version=simple_index.version,
                system_model=simple_index.system_model,
                type_="default",
            )
            if ecoinvent_index:
                ecoinvent_index = ecoinvent_index[0]
            else:
                raise ValueError(f"Ecoinvent index {self.raw_data.bw_project} not found")

            if not ecoinvent_index.bw_project_index:
                raise ValueError(
                    f"Ecoinvent index {ecoinvent_index}, has not BWProject index"
                )
            bw_project_index: BWProjectIndex = ecoinvent_index.bw_project_index
            validate_bw_project_bw_database(
                bw_project_index.project_name, bw_project_index.database_name
            )

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

    @staticmethod
    def _prepare_activities(
        activities: ActivitiesDataTypes,
    ) -> list[ExperimentActivityData]:
        raw_activities_list: list[ExperimentActivityData] = []
        if isinstance(activities, list):
            raw_activities_list = activities
            for activity in raw_activities_list:
                activity.orig_id = copy(activity.id)

        elif isinstance(activities, dict):
            for activity_alias, activity in activities.items():
                if activity_alias == activity.alias:
                    raise ValueError(
                        f"Activity in activities-dict declared with alias: "
                        f"'{activity_alias}', "
                        f"different than in the activity.id: '{activity.alias}'"
                    )
                activity.orig_id = copy(activity.id)
                activity.id.alias = activity_alias
                raw_activities_list.append(activity)
        return raw_activities_list

    @staticmethod
    def _validate_activities(
        activities: list[ExperimentActivityData],
        bw_default_database: Optional[str] = None,
        output_required: bool = False,
    ) -> dict[str, ExtendedExperimentActivityData]:
        """
        Check if all activities exist in the bw database, and check if the
        given activities are unique
        In case there is only one scenario, all activities are required to have outputs
        """
        # if activities is a list, convert validate and convert to dict
        default_id_data = ExperimentActivityId(database=bw_default_database)
        activities_map: dict[str, ExtendedExperimentActivityData] = {}
        # validate
        for activity in activities:
            ext_activity: ExtendedExperimentActivityData = Experiment._validate_activity(
                activity, default_id_data, output_required
            )
            # check unique aliases
            if ext_activity.alias in activities_map:
                raise Exception(
                    f"Activity-alias '{ext_activity.alias}' passed more then once. "
                    f"Consider using a dictionary for activities or include "
                    f"'alias' in the ids."
                )
            activities_map[ext_activity.alias] = ext_activity

        # all codes should only appear once
        # unique_activities = set()
        # for ext_activity in activities_map.values():
        #     unique_activities.add((ext_activity.id.database, ext_activity.id.code))
        assert len(activities_map) > 0, "There are no activities in the experiment"
        return activities_map

    @staticmethod
    def _bw_activity_search(activity: ExperimentActivityData) -> Activity:
        """
        Search for the activity in the brightway project
        :param activity:
        :return: brightway activity
        """
        id_ = activity.id
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
                    search_results = list(
                        filter(lambda a: a["unit"] == id_.unit, search_results)
                    )
                if len(search_results) == 1:
                    bw_activity = search_results[0]
                    break
                elif len(search_results) > 1:
                    activities_str = "\n".join(
                        [f'{str(a)} - {a["code"]}' for a in search_results]
                    )
                    raise ValueError(
                        f"There are more than one activity with the same name, "
                        f"try including  "
                        f"the code of the activity you want to use:\n{activities_str}"
                    )
        if not bw_activity:
            raise ValueError(f"No activity found for {activity.id}")
        return bw_activity

    @staticmethod
    def _validate_activity(
        activity: ExperimentActivityData,
        default_id_attr: ExperimentActivityId,
        required_output: bool = False,
    ) -> "ExtendedExperimentActivityData":
        """
        This method checks if the activity exists in the database by several ways.
        :param activity:
        :param default_id_attr:
        :param required_output:
        :return:
        """

        # get the brightway activity
        bw_activity = Experiment._bw_activity_search(activity)

        # create output: ActivityOutput and default_output_value
        if activity.output:
            if isinstance(activity.output, tuple):
                output = ActivityOutput(
                    unit=activity.output[0], magnitude=activity.output[1]
                )
            else:  # if isinstance(activity.output, ActivityOutput):
                output = activity.output

        else:
            output = ActivityOutput(unit=bw_unit_fix(bw_activity["unit"]), magnitude=1.0)
        default_output_value = Experiment._validate_output(
            output, bw_activity, activity.id
        )

        if not activity.output:
            try:
                activity.output = ActivityOutput(unit=bw_activity["unit"])
            except ValidationError as err:
                raise ValueError(
                    f"Activity {activity.id} has invalid output format: {err}"
                )

        activity_dict = asdict(activity)
        activity_dict["output"] = asdict(output)
        result: ExtendedExperimentActivityData = ExtendedExperimentActivityData(
            **activity_dict,
            bw_activity=bw_activity,
            default_output_value=default_output_value,
        )
        result.id.fill_empty_fields(["alias"], **asdict(default_id_attr))

        result.id.fill_empty_fields(
            ["name", "code", "location", "unit", ("alias", "name")],
            **result.bw_activity.as_dict(),
        )
        if required_output:
            assert activity.output is not None, (
                f"Since there is no scenario, activity output is required: "
                f"{activity.orig_id}"
            )
        return result

    @staticmethod
    def _validate_output(
        target_output: ActivityOutput,
        bw_activity: Activity,
        activity_id: ExperimentActivityId,
    ) -> float:
        """
        validate and convert to the bw-activity unit
        :param target_output:
        :param bw_activity:
        :param activity_id:
        :return:
        """
        try:
            target_quantity: Quantity = (
                ureg.parse_expression(
                    bw_unit_fix(target_output.unit), case_sensitive=False
                )
                * target_output.magnitude
            )
            bw_activity_unit = bw_activity["unit"]
            return target_quantity.to(bw_unit_fix(bw_activity_unit)).magnitude
        except UndefinedUnitError as err:
            logger.error(
                f"Cannot parse output unit '{target_output.unit}'- "
                f"of activity {activity_id}. {err}. "
                f"Consider the unit definition to 'enbios2/base/unit_registry.py'"
            )
            raise Exception(f"Unit error, {err}; For activity: {activity_id}")
        except DimensionalityError as err:
            logger.error(
                f"Cannot convert output of activity {activity_id}. -"
                f"From- \n{target_output}\n-To-"
                f"\n{bw_activity['unit']} (brightway unit)"
                f"\n{err}"
            )
            raise Exception(f"Unit error for activity: {activity_id}")

    def _prepare_methods(
        self, methods: Optional[MethodsDataTypes] = None
    ) -> dict[str, ExperimentMethodData]:
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
    def _validate_method(
        method: ExperimentMethodData, alias: str
    ) -> ExperimentMethodPrepData:
        method.id = tuple(method.id)
        bw_method = bd.methods.get(method.id)
        if not bw_method:
            raise Exception(f"Method with id: {method.id} does not exist")
        if method.alias:
            if method.alias != alias:
                raise Exception(
                    f"Method alias: {method.alias} does not match with "
                    f"the given alias: {alias}"
                )
        else:
            method.alias = alias
        return ExperimentMethodPrepData(
            id=method.id, alias=method.alias, bw_method_unit=bw_method["unit"]
        )

    @staticmethod
    def _validate_methods(
        method_dict: dict[str, ExperimentMethodData]
    ) -> dict[str, ExperimentMethodPrepData]:
        # all methods must exist
        return {
            alias: Experiment._validate_method(method, alias)
            for alias, method in method_dict.items()
        }

    def _has_activity(
        self, alias_or_id: Union[str, ExperimentActivityId]
    ) -> Optional[ExtendedExperimentActivityData]:
        if isinstance(alias_or_id, str):
            activity = self._activities.get(alias_or_id, None)
            return activity
        else:  # isinstance(alias_or_id, ExperimentActivityId):
            for activity in self._activities.values():
                if activity.orig_id == alias_or_id:
                    return activity
            return None

    def get_activity(
        self, alias_or_id: Union[str, ExperimentActivityId]
    ) -> ExtendedExperimentActivityData:
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

    def _validate_scenarios(self) -> list[Scenario]:
        """
        :return:
        """

        def validate_activity_id(
            activity_id: Union[str, ExperimentActivityId]
        ) -> SimpleScenarioActivityId:
            activity = self.get_activity(activity_id)
            id_ = activity.id
            assert id_.name and id_.code and id_.alias
            return SimpleScenarioActivityId(name=id_.name, alias=id_.alias, code=id_.code)

        def validate_activities(scenario_: ExperimentScenarioData) -> Activity_Outputs:
            activities = scenario_.activities
            result: dict[SimpleScenarioActivityId, float] = {}

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
            for activity_id, activity_output in scenarios_activities:
                activity = self.get_activity(activity_id)
                simple_id = validate_activity_id(activity_id)
                output_ = convert_output(activity_output)
                scenario_output = Experiment._validate_output(
                    output_, activity.bw_activity, activity.id
                )
                result[simple_id] = scenario_output
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
            defined_aliases = [
                output_id.alias for output_id in scenario_activities_outputs.keys()
            ]
            # fill up the missing activities with default values
            for activity in self._activities.values():
                activity_alias = activity.alias
                if activity_alias not in defined_aliases:
                    # print(activity)
                    id_ = SimpleScenarioActivityId(
                        name=str(activity.id.name),
                        code=str(activity.id.code),
                        alias=activity.alias,
                    )
                    scenario_activities_outputs[
                        id_
                    ] = activity.default_output_value  # type: ignore

            resolved_methods: dict[str, ExperimentMethodPrepData] = {}
            if _scenario.methods:
                if isinstance(_scenario.methods, list):
                    for index_, method_ in enumerate(_scenario.methods):
                        if isinstance(method_, str):
                            global_method = self.methods.get(method_)
                            assert global_method
                            resolved_methods[global_method.alias] = global_method
                else:
                    method_dict: dict[str, tuple[str, ...]] = cast(
                        dict[str, tuple[str, ...]], _scenario.methods
                    )
                    for method_alias, method_ in method_dict.items():  # type: ignore
                        md = ExperimentMethodData(id=cast(tuple[str, ...], method_))
                        prep_method = self._validate_method(md, method_alias)
                        resolved_methods[prep_method.alias] = prep_method

            return Scenario(
                experiment=self,  # type: ignore
                alias=_scenario_alias,
                activities_outputs=scenario_activities_outputs,
                methods=resolved_methods,
                result_tree=self.hierarchy_root.copy(),
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

    def _prepare_hierarchy(self) -> Union[dict, list]:
        return (
            self.raw_data.hierarchy
            if self.raw_data.hierarchy
            else list(self._activities.keys())
        )

    def validate_hierarchy(
        self, hierarchy: Union[dict, list]
    ) -> BasicTreeNode[ScenarioResultNodeData]:
        tech_tree: BasicTreeNode[ScenarioResultNodeData] = BasicTreeNode.from_dict(
            hierarchy, compact=True
        )
        for leaf in tech_tree.iter_leaves():
            leaf.temp_data = {"activity": self.get_activity(leaf.name)}
        missing = set(self.activities_aliases) - set(
            n.name for n in tech_tree.iter_leaves()
        )
        if missing:
            raise ValueError(f"Activities {missing} not found in hierarchy")
        return tech_tree

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

    def run_scenario(self, scenario_alias: str) -> dict[str, Any]:
        """
        Run a specific scenario
        :param scenario_alias:
        :return: The result_tree converted into a dict
        """
        return self.get_scenario(scenario_alias).run().as_dict(include_data=True)

    def run(self) -> dict[str, BasicTreeNode[ScenarioResultNodeData]]:
        """
        Run all scenarios. Returns an dict with the scenario alias as key
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

        for scenario in run_scenarios:
            scenario.reset_execution_time()
            if scenario.methods:
                raise ValueError(
                    f"Scenario cannot have individual methods. '{scenario.alias}'"
                )
            inventory: list[dict[Activity, float]] = []
            for activity_alias, act_out in scenario.activities_outputs.items():
                bw_activity = scenario.experiment.get_activity(
                    activity_alias.alias
                ).bw_activity
                inventory.append({bw_activity: act_out})
            inventories.append(inventory)

        # run experiment
        start_time = time.time()
        calculation_setup = BWCalculationSetup(
            "experiment", list(itertools.chain(*inventories)), methods
        )

        distribution_results = self.config.use_k_bw_distributions > 1

        results: dict[str, BasicTreeNode[ScenarioResultNodeData]] = {}
        for i in range(self.config.use_k_bw_distributions):
            raw_results = StackedMultiLCA(calculation_setup, distribution_results).results
            scenario_results = np.split(raw_results, len(run_scenarios))
            for index, scenario in enumerate(run_scenarios):
                results[scenario.alias] = scenario.set_results(
                    scenario_results[index],
                    distribution_results,
                    i == self.config.use_k_bw_distributions - 1,
                )
            self._execution_time = time.time() - start_time
        return results

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
                if not math.isnan(scenario._execution_time):
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
            activity_rows.append(f"  {activity.alias} - {activity.id.name}")
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
