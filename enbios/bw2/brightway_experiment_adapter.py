from dataclasses import dataclass
from logging import getLogger
from typing import Optional, Any, Union, Sequence

import bw2data as bd
from bw2data.backends import Activity
from numpy import ndarray
from pint import Quantity, UndefinedUnitError
from pydantic import BaseModel, Field, RootModel, ConfigDict, model_validator

from enbios import get_enbios_ureg
from enbios.base.adapters_aggregators.adapter import EnbiosAdapter
from enbios.base.scenario import Scenario
from enbios.bw2.stacked_MultiLCA import StackedMultiLCA, BWCalculationSetup
from enbios.bw2.util import bw_unit_fix, get_activity
from enbios.models.experiment_base_models import NodeOutput, AdapterModel
from enbios.models.experiment_models import (
    ResultValue,
)
from experiment.regionalization import regionalization

logger = getLogger(__file__)

ureg = get_enbios_ureg()


class ExperimentMethodPrepData(BaseModel):
    id: tuple[str, ...]
    # todo should go...
    bw_method_unit: str


class RegionalizationConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True, strict=True)
    simple_regionalization: bool = Field(False)
    select_regions: set = Field(None)

    @model_validator(mode="before")
    @classmethod
    def validate(cls, data: Any) -> Any:
        if data["simple_regionalization"]:
            if data.get("select_regions") is None:
                raise ValueError(f"Select regions for BW regionalization (field: 'select_regions')")
        return data


class BWAdapterConfig(BaseModel):
    bw_project: str
    # methods: MethodsDataTypesExt
    use_k_bw_distributions: int = Field(
        1, description="Number of samples to use for MonteCarlo"
    )
    store_raw_results: bool = Field(
        False,
        description="If the numpy matrix of brightway should be stored in the adapter. "
                    "Will be stored in `raw_results[scenario.name]`",
    )
    store_lca_object: bool = Field(
        False,
        description="If the LCA object should be stored. "
                    "Will be stored in `lca_objects[scenario.name]`",
    )
    simple_regionalization: Optional[RegionalizationConfig] = Field(None,
                                                                    description="Generate regionalized LCA")


class BrightwayActivityConfig(BaseModel):
    # todo this is too bw specific
    name: str = Field(
        None, description="Search:Name of the brightway activity"
    )  # brightway name
    database: str = Field(
        None, description="Search:Name of the database to search first"
    )  # brightway database
    code: str = Field(
        None, description="Search:Brightway activity code"
    )  # brightway code
    # search and filter
    location: Union[str, tuple[str, ...]] = Field(None, description="Search:Location filter")  # location
    # additional filter
    unit: str = Field(None, description="Search: unit filter of results")  # unit
    # internal-name
    default_output: NodeOutput = Field(
        None, description="Default output of the activity for all scenarios"
    )


class BWMethodModel(BaseModel):
    name: str = Field(None, description="Name for identification")
    id: tuple[str, ...] = Field(None, description="Brightway method id")


class BWMethodDefinition(RootModel):
    model_config = ConfigDict(
        title="Method definition",
        json_schema_extra={"description": "Simply a dict: name : BW method tuple"},
    )
    root: dict[str, Sequence[str]]


def _bw_activity_search(activity_id: dict) -> Activity:
    """
    Search for the activity in the brightway project
    :param activity_id:
    :return: brightway activity
    """
    id_ = BrightwayActivityConfig(**activity_id)
    bw_activity: Optional[Activity] = None
    if id_.code:
        if id_.database:
            bw_activity = bd.Database(id_.database).get(id_.code)
        else:
            bw_activity = get_activity(id_.code)
    elif id_.name:
        filters = {}
        search_in_dbs = [id_.database] if id_.database else bd.databases
        for db in search_in_dbs:
            if id_.location and isinstance(id_.location, str):
                filters["location"] = id_.location
                search_results = bd.Database(db).search(id_.name, filter=filters)
            else:
                search_results = bd.Database(db).search(id_.name)
                # filter exact name
                search_results = list(
                    filter(lambda a: a["name"] == id_.name, search_results)
                )
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
    default_output: NodeOutput


class BrightwayAdapter(EnbiosAdapter):
    @staticmethod
    def name() -> str:
        return "brightway-adapter"

    def validate_definition(self, definition: AdapterModel):
        pass

    def __init__(self):
        super(BrightwayAdapter, self).__init__()
        self.config = None
        self.activityMap: dict[str, BWActivityData] = {}
        self.methods: dict[str, ExperimentMethodPrepData] = {}
        self.scenario_calc_setups: dict[
            str, BWCalculationSetup
        ] = {}  # scenario_alias to BWCalculationSetup
        self.raw_results: dict[str, list[ndarray]] = {}  # scenario_alias to results
        self.lca_objects: dict[str, list[StackedMultiLCA]] = {}  # scenario_alias to lca objects

    @staticmethod
    def node_indicator() -> str:
        return "bw"

    def validate_config(self, config: dict[str, Any]):
        self.config = BWAdapterConfig(**config)
        if self.config.use_k_bw_distributions < 1:
            raise ValueError(
                f"config.use_k_bw_distributions must be greater than 0, "
                f"but is {self.config.use_k_bw_distributions}"
            )

        if self.config.bw_project not in bd.projects:
            raise ValueError(f"Project {self.config.bw_project} not found")
        else:
            bd.projects.set_current(self.config.bw_project)

    def validate_methods(self, methods: dict[str, Any]) -> list[str]:
        assert methods, "Methods must be defined for brightway adapter"
        # validation
        BWMethodDefinition(methods)

        def validate_method(method_id: Sequence[str]) -> ExperimentMethodPrepData:
            # todo: should complain, if the same method is passed twice
            bw_method = bd.methods.get(method_id)
            if not bw_method:
                raise ValueError(f"Method with id: {method_id} does not exist")
            return ExperimentMethodPrepData(
                id=tuple(method_id), bw_method_unit=bw_method["unit"]
            )

        self.methods: dict[str, ExperimentMethodPrepData] = {
            name: validate_method(method) for name, method in methods.items()
        }
        return list(self.methods.keys())

    def validate_node_output(
            self,
            node_name: str,
            target_output: NodeOutput,
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
            raise UndefinedUnitError(f"Unit error, {err}; For activity: {node_name}")
        # except DimensionalityError as err:
        #     logger.error(
        #         f"Cannot convert output of activity {node_name}. -"
        #         f"From- \n{target_output}\n-To-"
        #         f"\n{bw_activity_unit} (brightway unit)"
        #         f"\n{err}"
        #     )
        #     raise DimensionalityError(f"Unit error for activity: {node_name}")

    def validate_node(self, node_name: str, node_config: Any):
        assert isinstance(
            node_config, dict
        ), f"Activity id (type: dict) must be defined for activity {node_name}"
        # get the brightway activity
        bw_activity = _bw_activity_search(node_config)

        self.activityMap[node_name] = BWActivityData(
            bw_activity=bw_activity,
            default_output=NodeOutput(unit=bw_unit_fix(bw_activity["unit"]), magnitude=1),
        )
        if "default_output" in node_config:
            self.activityMap[
                node_name
            ].default_output.magnitude = self.validate_node_output(
                node_name, NodeOutput(**node_config["default_output"])
            )
        if self.config.simple_regionalization:
            if node_config["enb_location"]:
                print(bw_activity._data)
                bw_activity["enb_location"] = node_config["enb_location"]
                bw_activity.save()

    def get_default_output_value(self, activity_name: str) -> float:
        return self.activityMap[activity_name].default_output.magnitude

    def get_node_output_unit(self, activity_name: str) -> str:
        return bw_unit_fix(self.activityMap[activity_name].bw_activity["unit"])

    def get_method_unit(self, method_name: str) -> str:
        return self.methods[method_name].bw_method_unit

    def prepare_scenario(self, scenario: Scenario):
        inventory: list[dict[Activity, float]] = []
        for act_alias, activity in self.activityMap.items():
            try:
                act_output = scenario.activities_outputs[act_alias]
                inventory.append({activity.bw_activity: act_output})
            except KeyError:
                # todo not sure if that ever happens..
                if not scenario.config.exclude_defaults:
                    raise Exception(
                        f"Activity {act_alias} not found in scenario {scenario.name}"
                    )

        methods = [m.id for m in self.methods.values()]
        calculation_setup = BWCalculationSetup(scenario.name, inventory, methods)
        calculation_setup.register()
        self.scenario_calc_setups[scenario.name] = calculation_setup

    def run_scenario(self, scenario: Scenario) -> dict[str, dict[str, ResultValue]]:
        self.prepare_scenario(scenario)
        use_distributions = self.config.use_k_bw_distributions > 1
        raw_results: Union[list[ndarray], ndarray] = [] # todo always a list?
        regionalization_results: list[dict[str, float]] = []
        run_regionalization = self.config.simple_regionalization.simple_regionalization
        for i in range(self.config.use_k_bw_distributions):
            print("multi", i)
            _lca = StackedMultiLCA(
                self.scenario_calc_setups[scenario.name], use_distributions
            )
            if self.config.store_lca_object:
                if not self.lca_objects.get(scenario.name):
                    self.lca_objects[scenario.name] = []
                self.lca_objects[scenario.name].append(_lca)
            raw_results.append(_lca.results)

            if run_regionalization:
                regionalization_results.append(regionalization(_lca.lca, "enb_location"))

        if self.config.store_raw_results:
            self.raw_results[scenario.name] = raw_results

        result_data: dict[str, Any] = {}
        act_idx = 0
        for act_alias in self.activityMap.keys():
            if act_alias not in scenario.activities_outputs:
                if not scenario.config.exclude_defaults:
                    # todo not sure if that ever happens...
                    raise ValueError(
                        f"Activity {act_alias} not found in scenario {scenario.name}"
                    )
                continue
            result_data[act_alias] = {}
            for m_idx, method in enumerate(self.methods.items()):
                method_name, method_data = method
                # todo this could be a type
                method_result = ResultValue(unit=method_data.bw_method_unit)
                if use_distributions:
                    method_result.multi_magnitude = [
                        res[act_idx, m_idx] for res in raw_results
                    ]
                else:
                    if run_regionalization:
                        pass
                    else:
                        method_result.magnitude = raw_results[0][act_idx, m_idx]
                result_data[act_alias][method_name] = method_result
            act_idx += 1
        return result_data

    @staticmethod
    def get_config_schemas() -> dict:
        return {
            "adapter": BWAdapterConfig.model_json_schema(),
            "activity": BrightwayActivityConfig.model_json_schema(),
            "method": BWMethodDefinition.model_json_schema(),
        }

    # def run(self) -> dict[str,dict[str, dict[str, ResultValue]]]:
    #     logger.error("Brightway adapter does not implment the generic run method")
    #     return {}
