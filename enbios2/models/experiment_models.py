from copy import copy
from dataclasses import asdict, dataclass, field
from typing import Optional, Union

import bw2data
from bw2data.backends import Activity
from pydantic import Field, Extra
from pydantic.dataclasses import dataclass as pydantic_dataclass

from enbios2.bw2.util import get_activity


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

    def get_bw_activity(self, allow_multiple: bool = False) -> Union[Activity, list[Activity]]:
        if self.code:
            if not self.database:
                return get_activity(self.code)
            else:
                return bw2data.Database(self.database).get(self.code)
        elif self.name:
            filters = {}
            if self.location:
                filters["location"] = self.location
                assert self.database in bw2data.databases, f"database {self.database} not found"
                search_results = bw2data.Database(self.database).search(self.name, filter=filters)
            else:
                search_results = bw2data.Database(self.database).search(self.name)
            if self.unit:
                search_results = list(filter(lambda a: a["unit"] == self.unit, search_results))
            assert len(search_results) == 0, (f"No results for brightway activity-search:"
                                              f" {(self.name, self.location, self.unit)}")
            if len(search_results) > 1:
                if allow_multiple:
                    return search_results
                assert False, (f"results : {len(search_results)} for brightway activity-search:"
                               f" {(self.name, self.location, self.unit)}. Results are: {search_results}")
            return search_results[0]
        else:
            raise ValueError("No code or name specified")

    def fill_empty_fields(self, fields: Optional[list[Union[str, tuple[str, str]]]] = None, **kwargs):
        if not fields:
            fields = []
        for field in fields:
            if isinstance(field, tuple):
                if not getattr(self, field[0]):
                    setattr(self, field[0], kwargs[field[1]])
            else:
                if not getattr(self, field):
                    setattr(self, field, kwargs[field])


@pydantic_dataclass(config=OperationConfig)
class ExtendedExperimentActivityData:
    id: ExperimentActivityId
    orig_id: ExperimentActivityId
    output: "ActivityOutput"
    bw_activity: Activity
    default_output_value: Optional[float] = 1.0

    def __hash__(self):
        return self.bw_activity["code"]

    @property
    def alias(self):
        return self.id.alias


@pydantic_dataclass(config=OperationConfig)
class ExtendedExperimentActivityPrepData:
    id: ExperimentActivityId
    orig_id: ExperimentActivityId
    output: "ActivityOutput"
    default_output_value: float
    bw_activity: Activity
    scenario_outputs: dict[str, "ActivityOutput"] = Field(default_factory=dict)

    @property
    def alias(self):
        return self.id.alias


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
    id: ExperimentActivityId = Field(..., description="The identifies (method to find) an activity")
    output: Optional[ExperimentActivityOutput] = Field(None, description="The default output of the activity")
    orig_id: Optional[ExperimentActivityId] = Field(None, description="Temporary copy of the id")

    @property
    def alias(self):
        return self.id.alias

    def check_exist(self, default_id_attr: Optional[ExperimentActivityId] = None,
                    required_output: bool = False) -> "ExtendedExperimentActivityData":
        """
        This method checks if the activity exists in the database by several ways.
        :param default_id_attr:
        :param required_output:
        :return:
        """
        orig_id: ExperimentActivityId = copy(self.id)
        bw_activity: Optional[Activity] = None

        if not self.id.database:
            if default_id_attr:
                self.id.database = default_id_attr.database
        # assert result.id.database in bd.databases,
        # f"activity database does not exist: '{self.id.database}' for {self.id}"
        # todo, is the needed?
        default_dict = asdict(default_id_attr) if default_id_attr else {}
        self.id.fill_empty_fields(["alias"], **default_dict)
        if self.id.code:
            if self.id.database:
                bw_activity = bw2data.Database(self.id.database).get_node(self.id.code)
            else:
                bw_activity = get_activity(self.id.code)
        elif self.id.name:
            # assert result.id.database is not None, f"database must be specified for {self.id} or default_database set in config"
            filters = {}
            search_in_dbs = [self.id.database] if self.id.database else bw2data.databases
            for db in search_in_dbs:
                if self.id.location:
                    filters["location"] = self.id.location
                    search_results = bw2data.Database(db).search(self.id.name, filter=filters)
                else:
                    search_results = bw2data.Database(db).search(self.id.name)
                # print(len(search_results))
                # print(search_results)
                if self.id.unit:
                    search_results = list(filter(lambda a: a["unit"] == self.id.unit, search_results))
                #     if len(search_results) == 0:
                #         raise ValueError(f"No activity found with the specified unit {self.id}")
                # assert len(search_results) == 1, f"results : {len(search_results)}"
                if len(search_results) == 1:
                    bw_activity = search_results[0]
                    break
        if not bw_activity:
            raise ValueError(f"No activity found for {self.id}")
        self.id.fill_empty_fields(["name", "code", "location", "unit", ("alias", "name")],
                                  **bw_activity.as_dict())

        if not self.output:
            self.output = ActivityOutput(unit=bw_activity["unit"], magnitude=1.0)
        return ExtendedExperimentActivityData(**asdict(self),
                                              orig_id=orig_id,
                                              bw_activity=bw_activity)


# TODO are we using this?
@pydantic_dataclass(frozen=True)
class BWMethod:
    description: str
    filename: str
    unit: str
    abbreviation: str
    num_cfs: int
    geocollections: list[str]


@pydantic_dataclass(config=StrictInputConfig)
class ExperimentMethodData:
    id: tuple[str, ...]
    alias: Optional[str] = None

    # def __init__(self, id: tuple[str, ...], alias: Optional[str] = None):
    #     self.id = id
    #     if alias:
    #         self.alias = alias
    #     else:
    #         self.alias = "_".join(id)

    @property
    def alias_(self) -> str:
        return str(self.alias)


@pydantic_dataclass(config=StrictInputConfig)
class ExperimentMethodPrepData:
    id: tuple[str, ...]
    alias: str
    bw_method: BWMethod


@pydantic_dataclass(config=StrictInputConfig)
class ExperimentScenarioData:
    # map from activity id to output. id is either as original (tuple) or alias-dict
    activities: Optional[Union[
        list[
            tuple[Union[str, ExperimentActivityId], ExperimentActivityOutput]],  # alias or id to output
        dict[str, ExperimentActivityOutput]]] = None  # alias to output, null means default-output (check exists)

    # either the alias, or the id of any method. not method means running them all
    methods: Optional[Union[list[Union[ExperimentMethodData, str]], dict[str, tuple[str, ...]]]] = None
    alias: Optional[str] = None

    @staticmethod
    def alias_factory(index: int):
        return f"Scenario {index}"


@pydantic_dataclass(config=StrictInputConfig)
class ExperimentScenarioPrepData:
    activities: dict[SimpleScenarioActivityId, ExperimentActivityOutput] = Field(default_factory=dict)
    methods: list[ExperimentMethodData] = Field(default_factory=list)


@pydantic_dataclass(config=StrictInputConfig)
class ScenarioConfig:
    # only used by ExperimentDataIO
    base_directory: Optional[str] = None
    # those are only used for testing
    debug_test_is_valid: bool = True
    debug_test_replace_bw_config: Union[bool, list[str]] = True  # standard replacement, or bw-project, bw-database
    debug_test_expected_error_code: Optional[int] = None
    debug_test_run: Optional[bool] = False
    note: Optional[str] = None


ActivitiesDataRows = list[ExperimentActivityData]
ActivitiesDataTypes = Union[ActivitiesDataRows, dict[str, ExperimentActivityData]]
MethodsDataTypes = Union[list[ExperimentMethodData], dict[str, tuple[str, ...]]]
HierarchyDataTypes = Union[list, dict]
ScenariosDataTypes = Union[list[ExperimentScenarioData], dict[str, ExperimentScenarioData]]


@pydantic_dataclass(config=StrictInputConfig)
class ExperimentData:
    """
    This class is used to store the data of an experiment.
    """
    bw_project: Union[str, EcoInventSimpleIndex] = Field(..., description="The brightway project name")
    activities: ActivitiesDataTypes = Field(..., description="The activities to be used in the experiment")
    methods: MethodsDataTypes = Field(..., description="The impact methods to be used in the experiment")
    bw_default_database: Optional[str] = Field(None,
                                               description="The default database of activities to be used "
                                                           "in the experiment")
    hierarchy: Optional[Union[list, dict]] = Field(None,
                                                   description="The activity hierarchy to be used in the experiment")
    scenarios: Optional[ScenariosDataTypes] = Field(None, description="The scenarios for this experiment")
    config: Optional[ScenarioConfig] = Field(default_factory=ScenarioConfig,
                                             description="The configuration of this experiment")


@pydantic_dataclass
class ExperimentDataIO:
    bw_project: Union[str, EcoInventSimpleIndex]
    bw_default_database: Optional[str] = None
    activities: Optional[Union[ActivitiesDataTypes, str]] = None
    methods: Optional[Union[MethodsDataTypes, str]] = None
    hierarchy: Optional[Union[HierarchyDataTypes, str]] = None
    scenarios: Optional[Union[ScenariosDataTypes, str]] = None
    config: Optional[ScenarioConfig] = Field(default_factory=ScenarioConfig)


@dataclass
class BWCalculationSetup:
    name: str
    inv: list[dict[Activity, float]]
    ia: list[tuple[str, ...]]

    def register(self):
        bw2data.calculation_setups[self.name] = {
            "inv": self.inv,
            "ia": self.ia
        }


@dataclass
class ScenarioResultNodeData:
    output: tuple[str, float]
    results: dict[str, float] = field(default_factory=dict)


Activity_Outputs = dict[SimpleScenarioActivityId, float]
