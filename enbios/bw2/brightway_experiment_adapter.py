from dataclasses import dataclass
from logging import getLogger
from typing import Optional, Any, Union

import bw2data as bd
from bw2data.backends import Activity
from numpy import ndarray
from pint import DimensionalityError, Quantity, UndefinedUnitError

from enbios import get_enbios_ureg
from enbios.base.adapters_aggregators.adapter import EnbiosAdapter
from enbios.base.scenario import Scenario
from enbios.bw2.stacked_MultiLCA import StackedMultiLCA, BWCalculationSetup
from enbios.bw2.util import bw_unit_fix, get_activity
from enbios.models.experiment_models import (
    ActivityOutput,
    ExperimentActivityId,
    ResultValue,
)

logger = getLogger(__file__)

ureg = get_enbios_ureg()


@dataclass
class ExperimentMethodPrepData:
    id: tuple[str, ...]
    # alias: str
    # todo should go...
    bw_method_unit: str


@dataclass
class BWAdapterConfig:
    bw_project: str
    # methods: MethodsDataTypesExt
    use_k_bw_distributions: Optional[int] = 1  # number of samples to use for monteCarlo
    bw_default_database: Optional[str] = None
    store_raw_results: Optional[bool] = False  # store numpy arrays of lca results


@dataclass
class BWAggregatorConfig:
    # bw_project: str
    # methods: MethodsDataTypesExt
    use_k_bw_distributions: Optional[int] = 1  # number of samples to use for monteCarlo


def _bw_activity_search(activity_id: ExperimentActivityId) -> Activity:
    """
    Search for the activity in the brightway project
    :param activity_id:
    :return: brightway activity
    """
    id_ = activity_id
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
        raise ValueError(f"No activity found for {activity_id}")
    return bw_activity


@dataclass
class BWActivityData:
    bw_activity: Activity
    default_output: ActivityOutput


class BrightwayAdapter(EnbiosAdapter):
    def __init__(self):
        super(BrightwayAdapter, self).__init__()
        self.config = None
        self.activityMap: dict[str, BWActivityData] = {}
        self.methods: dict[str, ExperimentMethodPrepData] = {}
        self.scenario_calc_setups: dict[
            str, BWCalculationSetup
        ] = {}  # scenario_alias to BWCalculationSetup
        self.raw_results: dict[str, list[ndarray]] = {}  # scenario_alias to results

    @property
    def name(self) -> str:
        return "brightway-adapter"

    @property
    def activity_indicator(self) -> str:
        return "bw"

    def validate_config(self, config: dict[str, Any]):
        self.config = BWAdapterConfig(**config)
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

    def validate_methods(self, methods: dict[str, Any]) -> list[str]:
        def validate_method(method: dict) -> ExperimentMethodPrepData:
            # todo: should complain, if the same method is passed twice
            bw_method = bd.methods.get(method["id"])
            if not bw_method:
                raise Exception(f"Method with id: {method['id']} does not exist")
            return ExperimentMethodPrepData(
                id=tuple(method["id"]), bw_method_unit=bw_method["unit"]
            )

        self.methods: dict[str, ExperimentMethodPrepData] = {
            name: validate_method(method) for name, method in methods.items()
        }
        return list(self.methods.keys())

    def validate_activity_output(
        self,
        node_name: str,
        target_output: ActivityOutput,
    ) -> float:
        """
        validate and convert to the bw-activity unit
        :param node_name:
        :param target_output:
        :return:
        """
        bw_activity_unit = "not yet set"
        try:
            target_quantity: Quantity = (
                ureg.parse_expression(
                    bw_unit_fix(target_output.unit), case_sensitive=False
                )
                * target_output.magnitude
            )
            bw_activity_unit = self.activityMap[node_name].bw_activity["unit"]
            return target_quantity.to(bw_unit_fix(bw_activity_unit)).magnitude
        except UndefinedUnitError as err:
            logger.error(
                f"Cannot parse output unit '{target_output.unit}'- "
                f"of activity {node_name}. {err}. "
                f"Consider the unit definition to 'enbios2/base/unit_registry.py'"
            )
            # raise Exception(f"Unit error, {err}; For activity: {node_name}")
        except DimensionalityError as err:
            logger.error(
                f"Cannot convert output of activity {node_name}. -"
                f"From- \n{target_output}\n-To-"
                f"\n{bw_activity_unit} (brightway unit)"
                f"\n{err}"
            )
            # raise Exception(f"Unit error for activity: {node_name}")

    def validate_activity(
        self,
        node_name: str,
        activity_id: Any,
        output: ActivityOutput,
        required_output: bool = False,
    ):
        activity_id = ExperimentActivityId(**activity_id)
        # get the brightway activity
        bw_activity = _bw_activity_search(activity_id)

        self.activityMap[node_name] = BWActivityData(
            bw_activity=bw_activity,
            default_output=ActivityOutput(
                unit=bw_unit_fix(bw_activity["unit"]), magnitude=1
            ),
        )
        if output:
            self.activityMap[
                node_name
            ].default_output.magnitude = self.validate_activity_output(node_name, output)

    def get_default_output_value(self, activity_name: str) -> float:
        return self.activityMap[activity_name].default_output.magnitude

    def get_activity_output_unit(self, activity_name: str) -> str:
        return bw_unit_fix(self.activityMap[activity_name].bw_activity["unit"])

    def get_method_unit(self, method_name: str) -> str:
        return self.methods[method_name].bw_method_unit

    def prepare_scenario(self, scenario: Scenario):
        inventory: list[dict[Activity, float]] = []
        for act_alias, activity in self.activityMap.items():
            act_output = scenario.activities_outputs[act_alias]
            inventory.append({activity.bw_activity: act_output})

        methods = [m.id for m in self.methods.values()]
        calculation_setup = BWCalculationSetup(scenario.name, inventory, methods)
        calculation_setup.register()
        self.scenario_calc_setups[scenario.name] = calculation_setup

    def run_scenario(self, scenario: Scenario) -> dict[str, dict[str, ResultValue]]:
        use_distributions = self.config.use_k_bw_distributions > 1
        raw_results: Union[list[ndarray], ndarray] = []
        for i in range(self.config.use_k_bw_distributions):
            _raw_results: ndarray = StackedMultiLCA(
                self.scenario_calc_setups[scenario.name], use_distributions
            ).results
            raw_results.append(_raw_results)

        if self.config.store_raw_results:
            self.raw_results[scenario.name] = raw_results

        result_data: dict[str, Any] = {}
        for act_idx, act_alias in enumerate(self.activityMap.keys()):
            result_data[act_alias] = {}
            for m_idx, method in enumerate(self.methods.items()):
                method_name, method_data = method
                # todo this could be a type
                method_result = ResultValue(unit=method_data.bw_method_unit)
                if use_distributions:
                    method_result.amount = [res[act_idx, m_idx] for res in raw_results]
                else:
                    method_result.amount = raw_results[0][act_idx, m_idx]
                result_data[act_alias][method_name] = method_result
        return result_data

    def run(self):
        pass
