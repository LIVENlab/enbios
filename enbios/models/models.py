from dataclasses import field
from typing import Optional, Union, Any

from pint.facets.plain import PlainQuantity
from pydantic import BaseModel, Field, model_validator, field_validator, ConfigDict

from enbios.generic.files import PathLike

StrictInputConfig = ConfigDict(extra="forbid", validate_assignment=True, strict=True)


class ExperimentConfig(BaseModel):
    model_config = StrictInputConfig
    warn_default_demand: bool = True  # todo: bring this back
    auto_aggregate: Optional[
        bool
    ] = True  # aggregate, with same indicator, as all children, if given.
    run_adapters_concurrently: bool = True
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


class AdapterModel(BaseModel):
    model_config = ConfigDict(extra="allow")
    module_path: Optional[PathLike] = None
    adapter_name: Optional[str] = Field(
        None,
        description="this this is to use inbuilt adapter "
        "(e.g. 'simple-assignment-adapter'",
    )
    config: dict = Field(default_factory=dict)
    methods: dict[str, Any] = Field(default_factory=dict)
    note: Optional[str] = Field(None, description="A note for this adapter")

    @model_validator(mode="before")  # type: ignore
    def module_specified(data: Any):
        # either module_path or module_class must be specified
        if not ("module_path" in data or "adapter_name" in data):
            raise ValueError("Either module_path or adapter_name must be specified")
        return data


class AggregationModel(BaseModel):
    module_path: Optional[PathLike] = None
    aggregator_name: str = Field(
        None,
        description="this this is to use inbuilt aggregator "
        "(e.g. 'simple-assignment-adapter'",
    )
    config: Optional[dict] = Field(default_factory=dict)
    note: Optional[str] = None

    @model_validator(mode="before")
    def module_specified(cls, data: Any) -> Any:
        # either module_path or module_class must be specified
        if not ("module_path" in data or "aggregator_name" in data):
            raise ValueError("Either module_path or aggregator_name must be specified")
        return data


class ExperimentHierarchyNodeData(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    aggregator: str = Field(..., description="name or node-indicator of the aggregator")
    config: Optional[Any] = Field(
        None, description="setup data (id, outputs, ... arbitrary data"
    )
    children: Optional[
        list[Union["ExperimentHierarchyNodeData", "ExperimentActivityData"]]
    ] = None


class ExperimentActivityData(BaseModel):
    """
    This is the dataclass for the activities in the experiment.
    """

    model_config = ConfigDict(extra="forbid")
    name: str
    config: Any = Field(..., description="setup data (id, outputs, ... arbitrary data")
    adapter: str = Field(..., description="The adapter to be used")


class HierarchyNodeReference(BaseModel):
    """
    This is a reference to a node in the hierarchy.
    """

    name: str
    config: Optional[Any] = Field(
        default=None, description="setup data (id, outputs, ... arbitrary data"
    )
    aggregator: Optional[str] = None
    children: Optional[list["HierarchyNodeReference"]] = None

    @field_validator("children", mode="before")
    @classmethod
    def transform_simple_string_children(
        cls, v: list[str]
    ) -> list[Union["HierarchyNodeReference", str]]:
        return [
            HierarchyNodeReference(name=child) if isinstance(child, str) else child
            for child in v
        ]


class ScenarioConfig(BaseModel):
    exclude_defaults: Optional[bool] = Field(
        False  # will only use on activities that are specified in the scenario
    )


class NodeOutput(BaseModel):
    model_config = StrictInputConfig
    unit: str
    magnitude: float = 1.0
    label: Optional[str] = None

    @model_validator(mode="before")
    def transform_value(cls, v: Any) -> dict:
        if isinstance(v, dict):
            return v
        elif isinstance(v, tuple) or isinstance(v, list):
            return {"unit": v[0], "magnitude": v[1]}
        return v

    def from_quantity(self, q: PlainQuantity):
        self.unit = str(q.units)
        self.magnitude = q.magnitude


class ExperimentScenarioData(BaseModel):
    model_config = StrictInputConfig
    name: Optional[str] = Field(None)
    nodes: dict[str, Any] = Field(
        ..., description="name to output, null means default-output (check exists)"
    )
    config: Optional[ScenarioConfig] = Field(default_factory=ScenarioConfig)  # type: ignore

    def name_factory(self, index: int):
        if not self.name:
            self.name = f"Scenario {index}"


class ExperimentData(BaseModel):
    """
    This class is used to store the data of an experiment.
    """

    model_config = StrictInputConfig

    adapters: list[AdapterModel] = Field(..., description="The adapters to be used")
    aggregators: list[AggregationModel] = Field(
        [], description="The aggregators to be used"
    )
    hierarchy: Union[ExperimentHierarchyNodeData, PathLike] = Field(
        ..., description="The activity hierarchy to be used in the experiment"
    )
    scenarios: Optional[Union[list[ExperimentScenarioData], PathLike]] = Field(
        None, description="The scenarios for this experiment"
    )
    config: ExperimentConfig = Field(
        default_factory=ExperimentConfig,
        description="The configuration of this experiment",
    )


class ExperimentDataResolved(BaseModel):
    """
    This class is used to store the data of an experiment.
    """

    model_config = StrictInputConfig

    adapters: list[AdapterModel] = Field(..., description="The adapters to be used")
    aggregators: list[AggregationModel] = Field(
        [], description="The aggregators to be used"
    )
    hierarchy: ExperimentHierarchyNodeData = Field(
        ..., description="The activity hierarchy to be used in the experiment"
    )
    scenarios: Optional[list[ExperimentScenarioData]] = Field(
        None, description="The scenarios for this experiment"
    )
    config: ExperimentConfig = Field(
        default_factory=ExperimentConfig,
        description="The configuration of this experiment",
    )


class TechTreeNodeData(BaseModel):
    adapter: Optional[str] = None
    aggregator: Optional[str] = None
    config: Optional[Any] = Field(
        None, description="The identifies (method to find) a node"
    )

    @model_validator(mode="before")
    def check_model(cls, data: Any) -> Any:
        if isinstance(data, dict):
            leaf_node = "adapter" in data and "config" in data
            non_leaf_node = "aggregator" in data
            assert leaf_node or non_leaf_node, (
                "Node must be either leaf ('id', 'adapter`) " "or non-leaf ('aggregator')"
            )
        return data


class ResultValue(BaseModel):
    model_config = StrictInputConfig
    unit: str
    magnitude: Optional[float] = None  # type: ignore
    multi_magnitude: Optional[list[float]] = field(default_factory=list)


class ScenarioResultNodeData(BaseModel):
    model_config = StrictInputConfig
    output: list[NodeOutput] = Field(default_factory=list)
    results: dict[str, ResultValue] = Field(default_factory=dict)
    adapter: Optional[str] = None
    aggregator: Optional[str] = None


class EnbiosValidationException(Exception):
    def __init__(self, message, exc_name=None, code: Optional[int] = None):
        super().__init__(message)
        self.exc_name = exc_name
        self.code = code
