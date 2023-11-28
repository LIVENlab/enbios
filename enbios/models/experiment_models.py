from dataclasses import dataclass, field
from typing import Optional, Union, Any, Type

import bw2data
from bw2data.backends import Activity
from pydantic import BaseModel, Field, model_validator, field_validator, ValidationError
from pydantic.dataclasses import dataclass as pydantic_dataclass
from pydantic.v1 import BaseSettings

from enbios.bw2.util import get_activity
from enbios.const import DEFAULT_SUM_AGGREGATOR
from enbios.generic.files import PathLike


class StrictInputConfig:
    validate_assignment = True
    extra = "forbid"


class OperationConfig:
    validate_assignment = True
    arbitrary_types_allowed = True


@pydantic_dataclass(config=StrictInputConfig)
class ActivityOutput:
    unit: str
    magnitude: float = 1.0


@pydantic_dataclass(config=StrictInputConfig)
class ExperimentActivityId:
    # todo this is too bw specific
    name: Optional[str] = None  # brightway name
    database: Optional[str] = None  # brightway database
    code: Optional[str] = None  # brightway code
    # search and filter
    location: Optional[str] = None  # location
    # additional filter
    unit: Optional[str] = None  # unit
    # internal-name
    alias: Optional[str] = None  # experiment name

    def get_bw_activity(
        self, allow_multiple: bool = False
    ) -> Union[Activity, list[Activity]]:
        if self.code:
            if not self.database:
                return get_activity(self.code)
            else:
                return bw2data.Database(self.database).get(self.code)
        elif self.name:
            filters = {}
            if self.location:
                filters["location"] = self.location
                assert (
                    self.database in bw2data.databases
                ), f"database {self.database} not found"
                search_results = bw2data.Database(self.database).search(
                    self.name, filter=filters
                )
            else:
                search_results = bw2data.Database(self.database).search(self.name)
            if self.unit:
                search_results = list(
                    filter(lambda a: a["unit"] == self.unit, search_results)
                )
            assert len(search_results) == 0, (
                f"No results for brightway activity-search:"
                f" {(self.name, self.location, self.unit)}"
            )
            if len(search_results) > 1:
                if allow_multiple:
                    return search_results
                assert False, (
                    f"results : {len(search_results)} for brightway activity-search:"
                    f" {(self.name, self.location, self.unit)}. Results are: "
                    f"{search_results}"
                )
            return search_results[0]
        else:
            raise ValueError("No code or name specified")

    def fill_empty_fields(
        self, fields: Optional[list[Union[str, tuple[str, str]]]] = None, **kwargs
    ):
        if not fields:
            fields = []
        for _field in fields:
            if isinstance(_field, tuple):
                if not getattr(self, _field[0]):
                    setattr(self, _field[0], kwargs[_field[1]])
            else:
                if not getattr(self, _field):
                    setattr(self, _field, kwargs[_field])


# this is just for the schema to accept an array.
ExperimentActivityOutputArray = tuple[str, float]

ExperimentActivityOutput = Union[ActivityOutput, ExperimentActivityOutputArray]


@pydantic_dataclass(config=StrictInputConfig)
class ExperimentHierarchyNodeData:
    name: str
    aggregator: str
    children: Optional[
        list[Union["ExperimentHierarchyNodeData", "ExperimentActivityData"]]
    ] = None


@pydantic_dataclass(config=StrictInputConfig)
class ExperimentActivityData:
    """
    This is the dataclass for the activities in the experiment.
    the id, is
    """

    name: str
    id: Any = Field(..., description="The identifies (method to find) an activity")
    adapter: str = Field(..., description="The adapter to be used")
    output: Optional[ExperimentActivityOutput] = Field(
        None, description="The default output of the activity"
    )
    orig_id: Optional[ExperimentActivityId] = Field(
        None, description="Temporary copy of the id"
    )


@pydantic_dataclass(config=StrictInputConfig, repr=False)
class ExperimentScenarioData:
    # map from activity id to output. id is either as original (tuple) or name-dict
    activities: Optional[
        dict[str, ExperimentActivityOutput]
    ] = None  # name to output, null means default-output (check exists)

    # either the name, or the id of any method. not method means running them all
    methods: Optional[list[Union[str]]] = None
    name: Optional[str] = None

    def name_factory(self, index: int):
        if not self.name:
            self.name = f"Scenario {index}"


@pydantic_dataclass(config=StrictInputConfig)
class ExperimentScenarioPrepData:
    activities: dict[str, ExperimentActivityOutput] = Field(default_factory=dict)
    # methods: list[ExperimentMethodData] = Field(default_factory=list)


@pydantic_dataclass(config=StrictInputConfig)
class ExperimentConfig:
    warn_default_demand: bool = True  # todo: bring this back
    auto_aggregate: Optional[
        bool
    ] = True  # aggregate, with same indicator, as all children, if given.
    # include_bw_activity_in_nodes: bool = True # todo: bring this to aggregator
    store_raw_results: bool = False  # store numpy arrays of lca results
    # use_k_bw_distributions: int = 1  # number of samples to use for monteCarlo
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


# with path
HierarchyDataTypesExt = Union[ExperimentHierarchyNodeData, PathLike]

ScenariosDataTypes = list[ExperimentScenarioData]
# with path
ScenariosDataTypesExt = Union[list[ExperimentScenarioData], PathLike]


@pydantic_dataclass(config=StrictInputConfig)
class ExperimentData:
    """
    This class is used to store the data of an experiment.
    """

    adapters: list["AdapterModel"] = Field(..., description="The adapters to be used")
    aggregators: list["AggregationModel"] = Field(
        [], description="The aggregators to be used"
    )
    hierarchy: Optional[HierarchyDataTypesExt] = Field(
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
    aggregator: Optional[str] = DEFAULT_SUM_AGGREGATOR
    # for
    id: Optional[Any] = Field(
        None, description="The identifies (method to find) an activity"
    )
    output: Optional[ActivityOutput] = Field(
        None, description="The default output of the activity"
    )

    @field_validator("output", mode="before")
    @classmethod
    def validate_output(cls, v: Any) -> Any:
        try:
            out = ActivityOutput(**v)
            return out
        except (ValidationError, Exception):
            pass
        try:
            out_tuple = ExperimentActivityOutputArray(v)
            # print(out_tuple)
            return {"unit": out_tuple[0], "magnitude": out_tuple[1]}
        except ValidationError:
            raise ValidationError(
                "Output must be either {unit, magnitude} or tuple[str, float]"
            )

    @model_validator(mode="before")
    @classmethod
    def check_card_number_omitted(cls, data: Any) -> Any:
        if isinstance(data, dict):
            leaf_node = "adapter" in data and "id" in data
            non_leaf_node = "aggregator" in data
            assert leaf_node or non_leaf_node, (
                "Node must be either leaf ('id', 'adapter`) " "or non-leaf ('aggregator')"
            )
        return data


@dataclass
class ResultValue:
    unit: str
    amount: Optional[Union[float, list[float]]] = 0


@dataclass
class ScenarioResultNodeData:
    output: tuple[Optional[str], Optional[float]] = (None, None)
    results: dict[str, ResultValue] = field(default_factory=dict)
    # distribution_results: dict[str, list[float]] = field(default_factory=dict)
    adapter: Optional[str] = None
    aggregator: Optional[str] = None


class AdapterModel(BaseModel):
    module_path: Optional[PathLike] = None
    module_class: Optional[Type] = None
    config: Optional[dict] = Field(default_factory=dict)
    methods: Optional[dict[str, Any]] = None

    # aggregates: Optional[bool] = False # todo: later allow one class to also aggregate

    @model_validator(mode="before")
    @classmethod
    def module_specified(cls, data: Any) -> Any:
        # either module_path or module_class must be specified
        if not ("module_path" in data or "module_class" in data):
            raise ValueError("Either module_path or module_class must be specified")
        return data


class AggregationModel(BaseModel):
    module_path: PathLike
    config: Optional[dict] = Field(default_factory=dict)


class Settings(BaseSettings):
    CONFIG_FILE: Optional[str] = None
    RUN_SCENARIOS: Optional[list[str]] = None


Activity_Outputs = dict[str, float]
