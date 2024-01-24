from dataclasses import dataclass
from typing import Any, Optional, Callable, Sequence

from bw2data.backends import Activity
from pydantic import BaseModel, ConfigDict, Field, model_validator, RootModel

from enbios.generic.enbios2_logging import get_logger
from enbios.models.experiment_base_models import NodeOutput

logger = get_logger(__name__)


class ExperimentMethodPrepData(BaseModel):
    id: tuple[str, ...]
    bw_method_unit: str


class RegionalizationConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_assignment=True, strict=True)
    run_regionalization: bool = Field(False)
    select_regions: set = Field(None, description="regions to store the results for")
    set_node_regions: dict[str, tuple[str, ...]] = Field(
        {}, description="Set node regions"
    )

    @model_validator(mode="before")
    @classmethod
    def validate(cls, data: Any) -> Any:
        if data.get("run_regionalization", False):
            if data.get("select_regions") is None:
                raise ValueError(
                    "Select regions for BW regionalization (field: 'select_regions')"
                )
        return data


class NonLinearMethodConfig(BaseModel):
    name: str= Field(None, description="bw method tuple name", init_var=False)
    bw_id: tuple[str, ...] = Field(None, description="bw method tuple name", init_var=False)
    functions: dict[tuple[str, str], Callable[[float], float]]
    # default_function: Optional[Callable[[float], float]]
    get_defaults_from_original: Optional[bool] = Field(False,
                                             description="Method is already defined in BW and has characterization values. ")
    # bw_methods_data: Optional[dict[Sequence[str], dict[int, Callable[[float], float]]]] = Field(
    #     default_factory=dict,
    #     description="Preparation. defaults filled in. Converted from names to bw tuples", init_var=False)

    @model_validator(mode="before")
    @classmethod
    def check_defaults(self, data: dict):
        has_default_function = data.get("default_function")
        has_get_defaults_from_original = data.get("get_defaults_from_original", False)
        if has_default_function and has_get_defaults_from_original:
            logger.warning(
                f"brightway nonlinear method config for method '{data['name']}' should only have one"
                f"'default_function' or 'get_defaults_from_original'. Using 'default_function'")
            data['get_defaults_from_original'] = False
        if not has_default_function and not has_get_defaults_from_original:
            logger.warning(f"brightway nonlinear method config for method '{data['name']}' should either have"
                           f"'default_function' or 'get_defaults_from_original'. Creating 'default_function'"
                           f"with f(x) = x")
            data["default_function"] = lambda v: v
        return data


class NonLinearCharacterizationConfig(BaseModel):
    methods: dict[str, NonLinearMethodConfig] = Field(
        ...,
        description="Non linear characterization. "
                    "Nested Dictionary: method_name > NonLinearMethodConfig")

    @model_validator(mode="before")
    @classmethod
    def add_method_names(cls, data):
        for method_name, method_config in data["methods"].items():
            method_config["name"] = method_name
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
    simple_regionalization: RegionalizationConfig = Field(
        description="Generate regionalized LCA", default_factory=RegionalizationConfig
    )
    nonlinear_characterization: Optional[NonLinearCharacterizationConfig] = Field(
        None,
        description="Nonlinear characterization")


class BrightwayActivityConfig(BaseModel):
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
    location: str = Field(None, description="Search:Location filter")  # location
    enb_location: tuple[str, ...] = Field(
        None, description="Location for regionalization"
    )
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


@dataclass
class BWActivityData:
    bw_activity: Activity
    default_output: NodeOutput
