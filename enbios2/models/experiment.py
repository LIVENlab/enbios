from copy import copy
from dataclasses import asdict
from typing import Optional, Union, Type

import bw2data as bd
from bw2data.backends import Activity
from pint import Quantity
from pydantic.dataclasses import dataclass

from enbios2.const import BASE_SCHEMA_PATH


class Config:
    arbitrary_types_allowed = True


@dataclass
class ExperimentActivitiesGlobalConf:
    default_database: Optional[str] = None


@dataclass
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
            return bd.Database(self.database).get(self.code)
        elif self.name:
            filters = {}
            if self.location:
                filters["location"] = self.location
                assert self.database in bd.databases, f"database {self.database} not found"
                search_results = bd.Database(self.database).search(self.name, filter=filters)
            else:
                search_results = bd.Database(self.database).search(self.name)
            if self.unit:
                search_results = list(filter(lambda a: a["unit"] == self.unit, search_results))
            assert len(search_results) == 0, f"No results for brightway activity-search: {(self.name, self.location, self.unit)}"
            if len(search_results) > 1:
                if allow_multiple:
                    return search_results
                assert False, f"results : {len(search_results)} for brightway activity-search: {(self.name, self.location, self.unit)}. Results are: {search_results}"
            return search_results[0]

    def fill_empty_fields(self, fields: list[Union[str, tuple[str, str]]] = (), **kwargs):
        for field in fields:
            if isinstance(field, tuple):
                if not getattr(self, field[0]):
                    setattr(self, field[0], kwargs[field[1]])
            else:
                if not getattr(self, field):
                    setattr(self, field, kwargs[field])


@dataclass
class ExperimentActivityOutputDict:
    unit: str
    magnitude: float = 1.0


# this is just for the schema to accept an array.
ExperimentActivityOutputArray: Type = tuple[str, float]

ExperimentActivityOutput = Union[ExperimentActivityOutputDict, ExperimentActivityOutputArray]


@dataclass(config=Config)
class ExtendedExperimentActivityOutput:
    unit: str
    magnitude: float = 1.0
    pint_quantity: Optional[Quantity] = None


@dataclass
class ExperimentActivity:
    id: ExperimentActivityId
    output: Optional[ExperimentActivityOutput] = None

    def check_exist(self, default_id_attr: Optional[ExperimentActivityId] = None,
                    required_output: bool = False) -> "ExtendedExperimentActivity":

        result: ExtendedExperimentActivity = ExtendedExperimentActivity(**asdict(self))
        result.orig_id = copy(self.id)
        if not self.id.database:
            assert default_id_attr.database is not None, f"database must be specified for {self.id} or default_database set in config"
            result.id.database = default_id_attr.database
        assert result.id.database in bd.databases, f"activity database does not exist: '{self.id.database}' for {self.id}"
        result.id.fill_empty_fields(["alias"], **asdict(default_id_attr))
        if result.id.code:
            result.bw_activity = bd.Database(result.id.database).get(result.id.code)
        elif result.id.name:
            filters = {}
            if result.id.location:
                filters["location"] = result.id.location
                search_results = bd.Database(result.id.database).search(result.id.name, filter=filters)
            else:
                search_results = bd.Database(result.id.database).search(result.id.name)
            # print(len(search_results))
            # print(search_results)
            if result.id.unit:
                search_results = list(filter(lambda a: a["unit"] == result.id.unit, search_results))
            assert len(search_results) == 1, f"results : {len(search_results)}"
            result.bw_activity = search_results[0]

        result.id.fill_empty_fields(["name", "code", "location", "unit", ("alias", "code")],
                                    **result.bw_activity.as_dict())
        if required_output:
            assert self.output is not None, f"Since there is no scenario, activity output is required: {self.orig_id}"
        return result


@dataclass
class BWMethod:
    description: str
    filename: str
    unit: str
    abbreviation: str
    num_cfs: int
    geocollections: list[str]


@dataclass
class ExperimentMethod:
    id: Union[list[str], tuple[str, ...]]
    alias: Optional[str] = None


@dataclass(config=Config)
class ExtendedExperimentActivity:
    id: ExperimentActivityId
    output: Optional["ExtendedExperimentActivityOutput"] = None
    orig_id: Optional[ExperimentActivityId] = None
    bw_activity: Optional[Activity] = None
    scenario_outputs: Optional[
        Union["ExtendedExperimentActivityOutput", dict[str, "ExtendedExperimentActivityOutput"]]] = None


@dataclass
class ExperimentHierarchyNode:
    children: Optional[
        Union[
            dict[str, "ExperimentHierarchyNode"],
            list[ExperimentActivityId],  # any activityId type
            list[str]]]  # activity alias


@dataclass
class ExperimentHierarchy:
    root: ExperimentHierarchyNode
    name: Optional[str] = None


@dataclass
class ExperimentScenario:
    # map from activity id to output. id is either as original (tupled) or alias-dict
    activities: Optional[Union[
        list[
            tuple[Union[str, ExperimentActivityId], ExperimentActivityOutput]],  # alias or id to output
        dict[str, Optional[ExperimentActivityOutput]]]]  # alias to output, null means default-output (check exists)

    # either the alias, or the id of any method. not method means running them all
    methods: Optional[list[Union[str, list[str], tuple[str, ...]]]] = None
    alias: Optional[str] = None

    @staticmethod
    def alias_factory(index: int):
        return f"Scenario {index}"


@dataclass
class ScenarioConfig:
    # cool
    debug_test_is_valid: bool = True
    debug_test_expected_error_code: Optional[int] = None


@dataclass
class ExperimentData:
    bw_project: str
    activities_config: ExperimentActivitiesGlobalConf = ExperimentActivitiesGlobalConf()
    activities: Optional[Union[list[ExperimentActivity], dict[str, ExperimentActivity]]] = None
    methods: Optional[Union[list[ExperimentMethod], dict[str, ExperimentMethod]]] = None
    hierarchy: Optional[Union[ExperimentHierarchy, list[ExperimentHierarchy]]] = None
    scenarios: Optional[Union[list[ExperimentScenario], dict[str, ExperimentScenario]]] = None
    config: Optional[ScenarioConfig] = ScenarioConfig()


if __name__ == "__main__":
    (BASE_SCHEMA_PATH / "scenario.schema.gen.json").write_text(
        ExperimentData.__pydantic_model__.schema_json(indent=2, ensure_ascii=False), encoding="utf-8")
