from dataclasses import dataclass, field
from typing import Optional, Union

import bw2data
from bw2data.backends import Activity
from pydantic import Field
from pydantic.dataclasses import dataclass as pydantic_dataclass
from pydantic.v1 import BaseSettings

from enbios2.bw2.util import get_activity
from enbios2.generic.files import PathLike


@pydantic_dataclass
class EcoInventSimpleIndex:
    version: str
    system_model: str


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
    database: Optional[str] = None
    code: Optional[str] = None
    # search and filter
    name: Optional[str] = None
    location: Optional[str] = None
    # additional filter
    unit: Optional[str] = None
    # internal-name
    alias: Optional[str] = None

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


@pydantic_dataclass(config=StrictInputConfig)
class SimpleScenarioActivityId:
    name: str
    code: str
    alias: str

    def __hash__(self):
        return hash(self.code)


# this is just for the schema to accept an array.
ExperimentActivityOutputArray = tuple[str, float]

ExperimentActivityOutput = Union[ActivityOutput, ExperimentActivityOutputArray]


@pydantic_dataclass(config=StrictInputConfig)
class ExperimentActivityData:
    """
    This is the dataclass for the activities in the experiment.
    the id, is
    """

    id: ExperimentActivityId = Field(
        ..., description="The identifies (method to find) an activity"
    )
    output: Optional[ExperimentActivityOutput] = Field(
        None, description="The default output of the activity"
    )
    orig_id: Optional[ExperimentActivityId] = Field(
        None, description="Temporary copy of the id"
    )

    @property
    def alias(self):
        return self.id.alias


@pydantic_dataclass(config=OperationConfig)
class ExtendedExperimentActivityData:
    id: ExperimentActivityId
    orig_id: ExperimentActivityId
    output: "ActivityOutput"
    bw_activity: Activity
    default_output_value: Optional[
        float
    ] = 1.0  # this is the value converted to the default (bw) unit

    def __hash__(self):
        return self.bw_activity["code"]

    @property
    def alias(self):
        return self.id.alias


@pydantic_dataclass(config=StrictInputConfig)
class ExperimentMethodData:
    id: tuple[str, ...]
    alias: Optional[str] = None

    @property
    def alias_(self) -> str:
        return str(self.alias)


@pydantic_dataclass(config=StrictInputConfig)
class ExperimentMethodPrepData:
    id: tuple[str, ...]
    alias: str
    bw_method_unit: str


@pydantic_dataclass(config=StrictInputConfig, repr=False)
class ExperimentScenarioData:
    # map from activity id to output. id is either as original (tuple) or alias-dict
    activities: Optional[
        Union[
            list[
                tuple[Union[str, ExperimentActivityId], ExperimentActivityOutput]
            ],  # alias or id to output
            dict[str, ExperimentActivityOutput],
        ]
    ] = None  # alias to output, null means default-output (check exists)

    # either the alias, or the id of any method. not method means running them all
    methods: Optional[
        Union[list[Union[ExperimentMethodData, str]], dict[str, tuple[str, ...]]]
    ] = None
    alias: Optional[str] = None

    @staticmethod
    def alias_factory(index: int):
        return f"Scenario {index}"


@pydantic_dataclass(config=StrictInputConfig)
class ExperimentScenarioPrepData:
    activities: dict[SimpleScenarioActivityId, ExperimentActivityOutput] = Field(
        default_factory=dict
    )
    methods: list[ExperimentMethodData] = Field(default_factory=list)


@pydantic_dataclass(config=StrictInputConfig)
class ExperimentConfig:
    warn_default_demand: bool = True  # todo: bring this back
    include_bw_activity_in_nodes: bool = True
    store_raw_results: bool = False  # store numpy arrays of lca results
    use_k_bw_distributions: int = 1  # number of samples to use for monteCarlo
    run_scenarios: Optional[
        list[str]
    ] = None  # list of scenario-alias to run, ALSO AS ENV-VAR
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


ActivitiesDataRows = list[ExperimentActivityData]
ActivitiesDataTypes = Union[ActivitiesDataRows, dict[str, ExperimentActivityData]]
# with path
ActivitiesDataTypesExt = Union[
    ActivitiesDataRows, dict[str, ExperimentActivityData], PathLike
]

MethodsDataTypes = Union[list[ExperimentMethodData], dict[str, tuple[str, ...]]]
# with path
MethodsDataTypesExt = Union[
    list[ExperimentMethodData], dict[str, tuple[str, ...]], PathLike
]

HierarchyDataTypes = Union[list, dict]
# with path
HierarchyDataTypesExt = Union[list, dict, PathLike]

ScenariosDataTypes = Union[
    list[ExperimentScenarioData], dict[str, ExperimentScenarioData]
]
# with path
ScenariosDataTypesExt = Union[
    list[ExperimentScenarioData], dict[str, ExperimentScenarioData], PathLike
]


@pydantic_dataclass(config=StrictInputConfig)
class ExperimentData:
    """
    This class is used to store the data of an experiment.
    """

    bw_project: Union[str, EcoInventSimpleIndex] = Field(
        ..., description="The brightway project name"
    )
    activities: ActivitiesDataTypesExt = Field(
        ..., description="The activities to be used in the experiment"
    )
    methods: MethodsDataTypesExt = Field(
        ..., description="The impact methods to be used in the experiment"
    )
    bw_default_database: Optional[str] = Field(
        None,
        description="The default database of activities to be used " "in the experiment",
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


@dataclass
class BWCalculationSetup:
    name: str
    inv: list[dict[Activity, float]]
    ia: list[tuple[str, ...]]

    def register(self):
        bw2data.calculation_setups[self.name] = {"inv": self.inv, "ia": self.ia}


@dataclass
class ScenarioResultNodeData:
    output: tuple[Optional[str], Optional[float]] = (None, None)
    results: dict[str, float] = field(default_factory=dict)
    distribution_results: dict[str, list[float]] = field(default_factory=dict)
    bw_activity: Optional[Activity] = None


class Settings(BaseSettings):
    CONFIG_FILE: Optional[str] = None
    RUN_SCENARIOS: Optional[list[str]] = None


Activity_Outputs = dict[SimpleScenarioActivityId, float]
