from dataclasses import dataclass
from logging import getLogger
from typing import Optional, Any, Union

import bw2data as bd
from bw2data.backends import Activity
from numpy import ndarray
from pint import DimensionalityError, Quantity, UndefinedUnitError

from enbios import get_enbios_ureg
from enbios.base.adapters import EnbiosAdapter, EnbiosAggregator
from enbios.base.scenario import Scenario
from enbios.base.stacked_MultiLCA import StackedMultiLCA
from enbios.bw2.util import bw_unit_fix, get_activity
from enbios.generic.tree.basic_tree import BasicTreeNode
from enbios.models.experiment_models import (
    ActivityOutput,
    ExperimentActivityData,
    ExperimentMethodData, ExperimentMethodPrepData,
    MethodsDataTypes, MethodsDataTypesExt, BWCalculationSetup, ScenarioResultNodeData,
)

logger = getLogger(__file__)

ureg = get_enbios_ureg()


@dataclass
class BWAdapterConfig:
    bw_project: str
    methods: MethodsDataTypesExt
    use_k_bw_distributions: Optional[int] = 1  # number of samples to use for monteCarlo
    bw_default_database: Optional[str] = None
    store_raw_results: Optional[bool] = False  # store numpy arrays of lca results


@dataclass
class BWAggregatorConfig:
    # bw_project: str
    # methods: MethodsDataTypesExt
    use_k_bw_distributions: Optional[int] = 1  # number of samples to use for monteCarlo


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


@dataclass
class BWActivityData:
    bw_activity: Activity
    default_output: ActivityOutput


class BrightwayAdapter(EnbiosAdapter):

    def __init__(self, config: dict):
        super(BrightwayAdapter, self).__init__()
        self.config = BWAdapterConfig(**config)
        self.activityMap: dict[str, BWActivityData] = {}
        self.methods: dict[str, ExperimentMethodPrepData] = {}
        self.scenario_calc_setups: dict[str, BWCalculationSetup] = {}  # scenario_alias to BWCalculationSetup
        self.raw_results: dict[str, list[ndarray]] = {}  # scenario_alias to results

    @property
    def name(self) -> str:
        return "brightway-adapter"

    @property
    def activity_indicator(self) -> str:
        return "bw"

    def validate_config(self):
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

        validate_bw_project_bw_database(
            self.config.bw_project, self.config.bw_default_database
        )

    def validate_methods(self):
        def validate_method(
                method: ExperimentMethodData, alias: str
        ) -> ExperimentMethodPrepData:
            # todo: should complain, if the same method is passed twice
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

        def prepare_methods(methods: MethodsDataTypes) -> dict[str, ExperimentMethodData]:
            # if not methods:
            #     methods = self.raw_data.methods
            method_dict: dict[str, ExperimentMethodData] = {}
            if isinstance(methods, dict):
                for method_alias, method in methods.items():
                    method_dict[method_alias] = ExperimentMethodData(method, method_alias)
            elif isinstance(methods, list):
                method_list: list[ExperimentMethodData] = [ExperimentMethodData(**m) for m in methods]
                for method_ in method_list:
                    alias = method_.alias if method_.alias else "_".join(method_.id)
                    method__ = ExperimentMethodData(method_.id, alias)
                    method_dict[method__.alias_] = method__
            return method_dict

        self.methods: dict[str, ExperimentMethodPrepData] = {
            alias: validate_method(method, alias)
            for alias, method in prepare_methods(self.config.methods).items()
        }
        return self.methods

    def validate_activity_output(
            self,
            activity: ExperimentActivityData,
            target_output: ActivityOutput,
    ) -> float:
        """
        validate and convert to the bw-activity unit
        :param activity:
        :param target_output:
        :return:
        """
        bw_activity_unit = "not yet set"
        try:
            target_quantity: Quantity = (
                    ureg.parse_expression(bw_unit_fix(target_output.unit), case_sensitive=False)
                    * target_output.magnitude
            )
            bw_activity_unit = self.activityMap[activity.alias].bw_activity["unit"]
            return target_quantity.to(bw_unit_fix(bw_activity_unit)).magnitude
        except UndefinedUnitError as err:
            logger.error(
                f"Cannot parse output unit '{target_output.unit}'- "
                f"of activity {activity.id}. {err}. "
                f"Consider the unit definition to 'enbios2/base/unit_registry.py'"
            )
            raise Exception(f"Unit error, {err}; For activity: {activity.id}")
        except DimensionalityError as err:
            logger.error(
                f"Cannot convert output of activity {activity.id}. -"
                f"From- \n{target_output}\n-To-"
                f"\n{bw_activity_unit} (brightway unit)"
                f"\n{err}"
            )
            raise Exception(f"Unit error for activity: {activity.id}")

    def validate_activity(
            self,
            activity: ExperimentActivityData,
            required_output: bool = False,
    ):
        # get the brightway activity
        bw_activity = _bw_activity_search(activity)
        self.activityMap[activity.alias] = BWActivityData(bw_activity=bw_activity,
                                                          default_output=ActivityOutput(
                                                              bw_unit_fix(bw_activity["unit"]), 1))
        # create output: ActivityOutput and default_output_value
        if activity.output:
            if isinstance(activity.output, tuple):
                output = ActivityOutput(unit=activity.output[0], magnitude=activity.output[1])
            else:  # if isinstance(activity.output, ActivityOutput):
                output = activity.output
            self.activityMap[activity.alias].default_output = output
            # todo do we need to use that value?
            default_output_value = self.validate_activity_output(activity, output)

    def get_default_output_value(self, activity_alias: str) -> float:
        return self.activityMap[activity_alias].default_output.magnitude

    def get_activity_unit(self, activity_alias: str) -> str:
        return bw_unit_fix(self.activityMap[activity_alias].bw_activity["unit"])

    def prepare_scenario(self, scenario: Scenario):
        inventory: list[dict[Activity, float]] = []
        # for activity_alias, act_out in scenario.activities_outputs.items():
        #     bw_activity = self.activityMap[activity_alias.alias].bw_activity
        #     inventory.append({bw_activity: act_out})
        # do the order we have in this map
        for act_alias, activity in self.activityMap.items():
            act_output = scenario.activities_outputs[act_alias]
            inventory.append({activity.bw_activity: act_output})

        methods = [m.id for m in self.methods.values()]
        calculation_setup = BWCalculationSetup(scenario.alias, inventory, methods)
        calculation_setup.register()
        self.scenario_calc_setups[scenario.alias] = calculation_setup

    def run_scenario(self, scenario: Scenario) -> dict[str, Any]:
        use_distributions = self.config.use_k_bw_distributions > 1
        raw_results: Union[list[ndarray], ndarray] = []
        for i in range(self.config.use_k_bw_distributions):
            _raw_results: ndarray = StackedMultiLCA(
                self.scenario_calc_setups[scenario.alias], use_distributions
            ).results
            raw_results.append(_raw_results)

        if self.config.store_raw_results:
            self.raw_results[scenario.alias] = raw_results

        result_data: dict[str, Any] = {}
        method_aliases = [m.alias for m in self.methods.values()]
        for act_idx, act_alias in enumerate(self.activityMap.keys()):
            result_data[act_alias] = {}
            for m_idx, method in enumerate(method_aliases):
                if use_distributions:
                    result_data[act_alias][method] = [res[act_idx, m_idx] for res in raw_results]
                else:
                    result_data[act_alias][method] = raw_results[0][act_idx, m_idx]

        return result_data

    def run(self):
        pass


class BrightwayAggregator(EnbiosAggregator):

    def __init__(self, config: dict):
        super(BrightwayAggregator, self).__init__()
        self.config = BWAggregatorConfig(**config)
        self.activityMap: dict[str, BWActivityData] = {}
        self.methods: dict[str, ExperimentMethodPrepData] = {}
        self.scenario_calc_setups: dict[str, BWCalculationSetup] = {}  # scenario_alias to BWCalculationSetup
        self.raw_results: dict[str, list[ndarray]] = {}  # scenario_alias to results

    @property
    def node_indicator(self) -> str:
        return "bw"

    def validate_config(self):
        pass

    def validate_node_output(self, node: BasicTreeNode[ScenarioResultNodeData]):
        pass

    def aggregate_results(self, node: BasicTreeNode[ScenarioResultNodeData]):
        pass
        # for child in node.children:
        #     if child.data:
        #         if add_to_distribution:
        #             for key, value in child.data.distribution_results.items():
        #                 if node.data.distribution_results.get(key) is None:
        #                     num_distribution = len(
        #                         list(child.data.distribution_results.values())[0]
        #                     )
        #                     node.data.distribution_results[key] = [0] * num_distribution
        #                 node.data.distribution_results[key] = list(
        #                     [
        #                         a + b
        #                         for a, b in zip(
        #                         node.data.distribution_results[key],
        #                         child.data.distribution_results[key],
        #                     )
        #                     ]
        #                 )
        #         else:
        #             for key, value in child.data.results.items():
        #                 if node.data.results.get(key) is None:
        #                     node.data.results[key] = 0
        #                 node.data.results[key] += value