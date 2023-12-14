from dataclasses import dataclass, field
from typing import Optional, Union, Any

from pydantic import BaseModel, Field, model_validator, field_validator, ConfigDict, ValidationInfo
from pydantic.dataclasses import dataclass as pydantic_dataclass
from pydantic.v1 import BaseSettings

from enbios.const import DEFAULT_SUM_AGGREGATOR
from enbios.generic.files import PathLike


class StrictInputConfig:
    validate_assignment = True
    extra = "forbid"


class OperationConfig:
    validate_assignment = True
    arbitrary_types_allowed = True


class ActivityOutput(BaseModel):
    unit: str
    magnitude: float = 1.0


class ExperimentHierarchyNodeData(BaseModel):
    name: str
    aggregator: str
    children: Optional[
        list[Union["ExperimentHierarchyNodeData", "ExperimentActivityData"]]
    ] = None


class ExperimentActivityData(BaseModel):
    """
    This is the dataclass for the activities in the experiment.
    the id, is
    """

    name: str
    config: Any = Field(..., description="setup data (id, outputs, ... arbitrary data")
    adapter: str = Field(..., description="The adapter to be used")


class ScenarioConfig(BaseModel):
    exclude_defaults: Optional[bool] = False  # will only use on activities that are specified in the scenario


class ExperimentScenarioData(BaseModel):
    model_config = ConfigDict(extra='forbid')
    name: Optional[str] = None
    # map from activity id to output. id is either as original (tuple) or name-dict
    activities: Optional[
        dict[str, ActivityOutput]
    ] = Field({})  # name to output, null means default-output (check exists)

    # either the name, or the id of any method. not method means running them all
    methods: Optional[list[Union[str]]] = None  # todo currently not used
    config: Optional[ScenarioConfig] = Field(default_factory=ScenarioConfig)

    def name_factory(self, index: int):
        if not self.name:
            self.name = f"Scenario {index}"



@pydantic_dataclass(config=StrictInputConfig)
class ExperimentConfig:
    warn_default_demand: bool = True  # todo: bring this back
    auto_aggregate: Optional[
        bool
    ] = True  # aggregate, with same indicator, as all children, if given.
    run_adapters_concurrently: bool = True
    # include_bw_activity_in_nodes: bool = True # todo: bring this to aggregator
    run_scenarios: Optional[
        list[str]
    ] = None  # list of scenario-name to run, ALSO AS ENV-VAR
    # only used by ExperimentDataIO
    # base_directory when loading files (activities, methods, ...)
    base_directory: Optional[Union[str, PathLike]] = None
    # those are only used for testing
    debug_test_is_valid: bool = True
    debug_test_replace_bw_config: Union[
        bool, list[str]
    ] = True  # standard replacement, or bw-project, bw-database
    debug_test_expected_error_code: Optional[int] = None
    debug_test_run: Optional[bool] = False
    note: Optional[str] = None


# with path
HierarchyDataTypesExt = Union[ExperimentHierarchyNodeData, PathLike]

ScenariosDataTypes = list[ExperimentScenarioData]
# with path
ScenariosDataTypesExt = Union[list[ExperimentScenarioData], PathLike]


class AdapterModel(BaseModel):
    model_config = ConfigDict(extra='allow')
    module_path: Optional[PathLike] = None
    adapter_name: Optional[str] = None  # this is to use inbuilt adapter (e.g. "simple-assignment-adapter")
    config: Optional[dict] = Field(default_factory=dict)
    methods: Optional[dict[str, Any]] = None
    note: Optional[str] = None

    # aggregates: Optional[bool] = False # todo: later allow one class to also aggregate

    @model_validator(mode="before")
    @classmethod
    def module_specified(cls, data: Any) -> Any:
        # either module_path or module_class must be specified
        if not ("module_path" in data or "adapter_name" in data):
            raise ValueError("Either module_path or adapter_name must be specified")
        return data


class AggregationModel(BaseModel):
    module_path: PathLike
    config: Optional[dict] = Field(default_factory=dict)


# @pydantic_dataclass(config=StrictInputConfig)
class ExperimentData(BaseModel):
    """
    This class is used to store the data of an experiment.
    """
    model_config = ConfigDict(extra='forbid')

    adapters: list[AdapterModel] = Field(..., description="The adapters to be used")
    aggregators: list[AggregationModel] = Field(
        [], description="The aggregators to be used"
    )
    hierarchy: HierarchyDataTypesExt = Field(
        None, description="The activity hierarchy to be used in the experiment"
    )
    scenarios: Optional[ScenariosDataTypesExt] = Field(
        None, description="The scenarios for this experiment"
    )
    config: ExperimentConfig = Field(
        default_factory=ExperimentConfig,
        description="The configuration of this experiment",
    )


class TechTreeNodeData(BaseModel):
    adapter: Optional[str] = None
    aggregator: Optional[str] = None
    # for
    config: Optional[Any] = Field(
        None, description="The identifies (method to find) an activity"
    )
    # todo should be in config?
    # output: Optional[ActivityOutput] = Field(
    #     None, description="The default output of the activity"
    # )

    @model_validator(mode="before")
    @classmethod
    def check_card_number_omitted(cls, data: Any) -> Any:
        if isinstance(data, dict):
            leaf_node = "adapter" in data and "config" in data
            non_leaf_node = "aggregator" in data
            assert leaf_node or non_leaf_node, (
                "Node must be either leaf ('id', 'adapter`) " "or non-leaf ('aggregator')"
            )
        return data

    @field_validator("aggregator", mode="before")
    @classmethod
    def add_default_aggregator(cls, v: str, values: ValidationInfo) -> str:
        if not values.data["adapter"]:
            return DEFAULT_SUM_AGGREGATOR
        return v


@dataclass
class ResultValue:
    model_config = ConfigDict(extra='forbid')
    unit: str
    amount: Optional[float] = 0
    multi_amount: Optional[list[float]] = field(default_factory=list)


@dataclass
class ScenarioResultNodeData:
    output: tuple[Optional[str], Optional[float]] = (None, None)
    results: dict[str, ResultValue] = field(default_factory=dict)
    adapter: Optional[str] = None
    aggregator: Optional[str] = None


class Settings(BaseSettings):
    CONFIG_FILE: Optional[str] = None
    RUN_SCENARIOS: Optional[list[str]] = None


Activity_Outputs = dict[str, float]
