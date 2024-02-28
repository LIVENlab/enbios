from pathlib import Path
from typing import Any, Optional, Literal

from pydantic import BaseModel, model_validator, Field

from enbios.base.adapters_aggregators.adapter import EnbiosAdapter
from enbios.base.scenario import Scenario
from enbios.generic.files import ReadPath
from enbios.generic.unit_util import get_output_in_unit, unit_match
from enbios.models.experiment_base_models import AdapterModel, NodeOutput
from enbios.models.experiment_models import ResultValue


class SimpleAssignment(BaseModel):
    node_name: str
    output_unit: str
    default_output: NodeOutput
    default_impacts: Optional[dict[str, ResultValue]] = Field(default_factory=dict)
    scenario_outputs: Optional[dict[str, NodeOutput]] = Field(default_factory=dict)
    scenario_impacts: Optional[dict[str, dict[str, ResultValue]]] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def validator(cls, data: Any) -> Any:
        if "default_output" not in data:
            data["default_output"] = {"unit": data["output_unit"], "magnitude": 1}
        if "default_impacts" not in data and "scenario_impacts" not in data:
            raise ValueError("Either default_impacts or scenario_impacts must be defined")
        return data


class SimpleAssignmentConfig(BaseModel):
    source_csv_file: Optional[Path] = Field(None)


class SimpleAssignmentDefinition(BaseModel):
    methods: dict[str, str]  # name: unit
    config: Optional["SimpleAssignmentConfig"] = Field(default_factory=SimpleAssignmentConfig)  # type: ignore


class SimpleAssignmentAdapter(EnbiosAdapter):
    @staticmethod
    def name():
        return "simple-assignment-adapter"

    def __init__(self) -> None:
        super().__init__()
        self.nodes: dict[str, SimpleAssignment] = {}  # name: node
        self.methods: dict[str, str] = {}  # name: unit
        self.definition: SimpleAssignmentDefinition = SimpleAssignmentDefinition(methods={})  # placeholder
        self.from_csv_file: bool = False

    def validate_definition(self, definition: AdapterModel):
        self.definition = SimpleAssignmentDefinition(**definition.model_dump())

    def validate_config(self, config: Optional[dict[str, Any]]):
        self.validate_methods(self.definition.methods)
        assert self.definition.config  # that's just for mypy
        if self.definition.config.source_csv_file:
            self.read_nodes_from_csv(self.definition.config.source_csv_file)
            self.from_csv_file = True

    def validate_methods(self, methods: Optional[dict[str, Any]]) -> list[str]:
        if methods:
            self.methods = methods
        if methods:
            return list(methods.keys())
        else:
            return []

    def validate_node(self, node_name: str, node_config: Any):
        if not self.from_csv_file:
            self.nodes[node_name] = SimpleAssignment(
                **{**{"node_name": node_name} | node_config}
            )

    def validate_scenario_node_output(self, node_name: str, target_output: NodeOutput) -> float:
        return get_output_in_unit(target_output, self.nodes[node_name].output_unit)

    def get_node_output_unit(self, node_name: str) -> str:
        return self.nodes[node_name].output_unit

    def get_method_unit(self, method_name: str) -> str:
        return self.methods[method_name]

    def get_default_output_value(self, node_name: str) -> float:
        return self.nodes[node_name].default_output.magnitude

    def run_scenario(self, scenario: Scenario) -> dict[str, dict[str, ResultValue]]:
        result: dict[str, dict[str, ResultValue]] = {}
        for node, config in self.nodes.items():
            node_results = result.setdefault(node, {})
            for method in self.methods:
                if config.scenario_impacts:
                    if scenario.name in config.scenario_impacts:
                        node_results[method] = config.scenario_impacts[scenario.name][
                            method
                        ]
                elif config.default_impacts:
                    if method in config.default_impacts:
                        node_results[method] = config.default_impacts[method]
        return result

    @staticmethod
    def node_indicator() -> str:
        return "assign"

    @staticmethod
    def get_config_schemas() -> dict:
        return {"node_name": SimpleAssignment.model_json_schema()}

    def read_nodes_from_csv(self, file_path: Path):
        """
        Read nodes default, scenario outputs and impacts from a csv file. Is used when config includes path to
        csv file in
        'source_csv_file'. One row per node/scenario. 1. row of a node is used for its defaults, after that,
        one row per scenario. If no scenario impacts are given all nodes must have default_impacts.


        Format of headers:

        node_name:

        output_unit: Base output unit

        default_output_unit:

        default_output_magnitude:

        default_impacts_XXX_unit

        default_impacts_XXX_magnitude

        scenario

        scenario_output_unit

        scenario_output_magnitude

        scenario_impacts_XXX_unit

        scenario_impacts_XXX_magnitude

        :param file_path:
        :return:
        """
        # TODO validate output and impacts units
        # one default output and multiple different scenario impacts would be weird...
        rows = ReadPath(file_path).read_data()
        headers = list(h.strip() for h in rows[0].keys())
        assert (
            all(k in headers for k in ["node_name", "output_unit"])), f"'node_name' and 'node_output' must be defined"
        assert ("default_output_unit" in headers) == ("default_output_magnitude" in headers), (
            f"'default_output_unit' and 'default_output_magnitude' must both be defined or absent")

        default_out_unit_header = "default_output_unit"
        def_out_mag_header = "default_output_magnitude"
        scenario_out_unit_header = "scenario_output_unit"
        scenario_out_mag_header = "scenario_output_magnitude"
        # Either default outputs or scenario outputs must be included
        assert ((default_out_unit_header in headers) and (def_out_mag_header in headers) or
                (scenario_out_unit_header in headers) and (scenario_out_mag_header in headers)), (
            f"Header must contain either 'default_output_unit' and 'default_output_magnitude' or"
            f"'scenario_output_unit' and 'scenario_output_magnitude'")

        has_scenario_outputs = scenario_out_unit_header in headers

        default_impacts_header: dict[str, tuple[str, str]] = {}
        scenario_impacts: dict[str, tuple[str, str]] = {}

        types_ = ["default", "scenario"]
        for header in headers:
            parts = header.split("_")
            if len(parts) != 4:
                continue
            # we just check for ..._unit and check if magnitude exists too
            if parts[1] == "impacts" and parts[3] == "unit":
                for type_, collection in zip(types_, [default_impacts_header, scenario_impacts]):
                    if parts[0] == type_:
                        mag_header = f"{type_}_impacts_{parts[2]}_magnitude"
                        assert mag_header in headers, (
                            f"magnitude header missing for {header}")
                        collection[parts[2]] = (header, mag_header)

        for impact in list(default_impacts_header.keys()) + list(scenario_impacts.keys()):
            if impact not in self.methods.keys():
                raise ValueError(
                    f"Header includes undefined impact: '{impact}'."
                    f"Options are {self.methods.values()}")

        if scenario_impacts:
            assert "scenario" in headers, f"header: 'scenario' is missing"

        node_map: dict[str, SimpleAssignment] = {}

        def get_impacts(row_: dict, type_: Literal["default", "scenario"]) -> dict[str, ResultValue]:
            coll = default_impacts_header if type_ == "default" else scenario_impacts
            result: dict[str, ResultValue] = {}
            for impact_name, (impact_unit_header, impact_mag_header) in coll.items():
                if row_[impact_mag_header] or row_[impact_unit_header]:
                    assert row_[impact_unit_header] and float(row_[impact_mag_header])
                    result[impact_name] = ResultValue(unit=row_[impact_unit_header],
                                                      magnitude=float(row_[impact_mag_header]))
            return result

        for idx, row in enumerate(rows):
            assert row.get("node_name"), "All rows must include a 'node_name'"
            node_name = row.get("node_name")
            try:
                row = {k.strip(): (v.strip() if v else v) for k, v in row.items()}
                node: SimpleAssignment
                # new node
                if node_name not in node_map:
                    assert row["output_unit"], f"First row defining '{node_name}' must include 'output_unit'"
                    unit = row.get(default_out_unit_header) if row.get(default_out_unit_header) else row["output_unit"]
                    magnitude = float(row.get(def_out_mag_header)) if row.get(def_out_mag_header) else 1
                    default_output = NodeOutput(unit=unit, magnitude=magnitude)
                    assert unit_match(unit, row["output_unit"])
                    default_impacts: dict[str, ResultValue] = get_impacts(row, "default")
                    for impact_name, result in default_impacts.items():
                        assert unit_match(result.unit, self.methods[impact_name])
                    node = SimpleAssignment(node_name=node_name,
                                            output_unit=row["output_unit"],
                                            default_output=default_output,
                                            scenario_outputs={},
                                            default_impacts=default_impacts,
                                            scenario_impacts={})
                    node_map[node_name] = node
                else:
                    node = node_map[node_name]
                    assert not row.get("output_unit", None), f"Redefinition of output_unit for '{node_name}'"
                    assert has_scenario_outputs, (f"Multiple rows per node, means header '{scenario_out_mag_header}' "
                                                  f"needs to be included")
                    assert row.get("scenario", None), f"No scenario defined"
                    assert row.get(scenario_out_mag_header,
                                   None), f"For each scenario row, a new output needs to be defined"

                scenario: str = row.get("scenario")
                assert scenario not in node.scenario_outputs, (  # type: ignore
                    f"Redefinition of scenario impacts for '{node_name}'")
                if row.get(scenario_out_mag_header):
                    unit = row.get(scenario_out_unit_header) if row.get(scenario_out_unit_header) else node.output_unit
                    assert unit_match(unit, node.output_unit)
                    node.scenario_outputs[scenario] = NodeOutput(  # type: ignore
                        unit=unit,
                        magnitude=float(row[scenario_out_mag_header]))
                if scenario:
                    row_impacts = get_impacts(row, "scenario")
                    for impact_name, result in row_impacts.items():
                        assert unit_match(result.unit, self.methods[impact_name])
                    node.scenario_impacts[scenario] = row_impacts  # type: ignore
                    assert node.scenario_impacts.get(scenario, {}), (  # type: ignore
                        f"Rows with scenarios must define scenario impacts")
            except Exception as err:
                logger = self.get_logger()
                logger.error(f"Error in row {idx} of '{node_name}'")
                logger.error(row)
                logger.error(err)
                raise err
        self.nodes = node_map
        return self.nodes
