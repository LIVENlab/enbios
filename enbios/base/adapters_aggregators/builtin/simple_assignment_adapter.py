from pathlib import Path
from typing import Any, Optional

from pydantic import BaseModel, model_validator, Field

from enbios.base.adapters_aggregators.adapter import EnbiosAdapter
from enbios.base.scenario import Scenario
from enbios.generic.files import ReadPath
from enbios.generic.unit_util import get_output_in_unit
from enbios.models.experiment_base_models import AdapterModel, NodeOutput
from enbios.models.experiment_models import ResultValue


class SimpleAssignment(BaseModel):
    node_name: str
    output_unit: str
    default_output: Optional[NodeOutput] = None
    default_impacts: Optional[dict[str, ResultValue]] = None
    scenario_outputs: Optional[dict[str, NodeOutput]] = None
    scenario_impacts: Optional[dict[str, dict[str, ResultValue]]] = None

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

    def validate_scenario_node(self, node_name: str, target_output: NodeOutput) -> float:
        return get_output_in_unit(target_output, self.nodes[node_name].output_unit)

    def validate_node(self, node_name: str, node_config: Any):
        if not self.from_csv_file:
            self.nodes[node_name] = SimpleAssignment(
                **{**{"node_name": node_name} | node_config}
            )

    def get_node_output_unit(self, node_name: str) -> str:
        return self.nodes[node_name].output_unit

    def get_method_unit(self, method_name: str) -> str:
        return self.methods[method_name]

    def get_default_output_value(self, node_name: str) -> float:
        return self.nodes[node_name].default_output.magnitude

    # def run(self):
    #     pass

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
        rows = ReadPath(file_path).read_data()
        headers = list(h.strip() for h in rows[0].keys())
        assert (
            all(k in headers for k in ["node_name", "output_unit"])), f"'node_name' and 'node_output' must be defined"
        assert ("default_output_unit" in headers) == ("default_output_magnitude" in headers), (
            f"'default_output_unit' and 'default_output_magnitude' must both be defined or absent")

        DEF_OUT_UNIT = "default_output_unit"
        DEF_OUT_MAG = "default_output_magnitude"
        SC_OUT_UNIT = "scenario_output_unit"
        SC_OUT_MAG = "scenario_output_magnitude"
        # Either default outputs or scenario outputs must be included
        assert ((DEF_OUT_UNIT in headers) and (DEF_OUT_MAG in headers) or
                (SC_OUT_UNIT in headers) and (SC_OUT_MAG in headers)), (
            f"Header must contain either 'default_output_unit' and 'default_output_magnitude' or"
            f"'scenario_output_unit' and 'scenario_output_magnitude'")

        has_default_outputs = DEF_OUT_UNIT in headers
        has_scenario_outputs = SC_OUT_UNIT in headers

        default_impacts: dict[str, tuple[str, str]] = {}
        scenario_impacts: dict[str, tuple[str, str]] = {}

        types_ = ["default", "scenario"]
        for header in headers:
            parts = header.split("_")
            if len(parts) != 4:
                continue
            # we just check for ..._unit and check if magnitude exists too
            if parts[1] == "impacts" and parts[3] == "unit":
                for type_, collection in zip(types_, [default_impacts, scenario_impacts]):
                    if parts[0] == type_:
                        mag_header = f"{type_}_impacts_{parts[2]}_magnitude"
                        assert mag_header in headers, (
                            f"magnitude header missing for {header}")
                        collection[parts[2]] = (header, mag_header)

        for impact in list(default_impacts.keys()) + list(scenario_impacts.keys()):
            if impact not in self.methods.values():
                raise ValueError(
                    f"Header includes undefined header for adapter: '{impact}'."
                    f"Options are {self.methods.values()}")

        if scenario_impacts:
            assert "scenario" in headers, f"header: 'scenario' is missing"

        node_map: dict[str, SimpleAssignment] = {}

        def get_row_outputs(row: dict) -> tuple[Optional[NodeOutput], Optional[NodeOutput]]:
            row_default_output = None
            row_scenario_output = None
            if has_default_outputs:
                if row.get(DEF_OUT_UNIT) and row.get(DEF_OUT_MAG):
                    row_default_output = NodeOutput(unit=row[DEF_OUT_UNIT],
                                                    magnitude=float(row[DEF_OUT_MAG]))
            if has_default_outputs:
                if row.get(SC_OUT_UNIT) and row.get(SC_OUT_MAG):
                    row_scenario_output = NodeOutput(unit=row[SC_OUT_UNIT], magnitude=float(row[SC_OUT_MAG]))
            return row_default_output, row_scenario_output

        def get_row_impacts(row: dict) -> tuple[dict[str, ResultValue], dict[str, ResultValue]]:
            row_default_impacts: dict[str, ResultValue] = {}
            row_scenario_impacts: dict[str, ResultValue] = {}
            for coll, row_coll in zip([default_impacts, scenario_impacts], [row_default_impacts, row_scenario_impacts]):
                for impact_name, (impact_unit_header, impact_mag_header) in coll.items():
                    if row[impact_mag_header]:
                        row_coll[impact_name] = ResultValue(unit=impact_unit_header,
                                                            magnitude=float(row[impact_mag_header]))
            return row_default_impacts, row_scenario_impacts

        for row in rows:
            row = {k.strip(): (v.strip() if v else v) for k, v in row.items()}
            node_name = row["node_name"]
            def_output, scenario_output_ = get_row_outputs(row)
            scenario:str = row.get("scenario")
            row_scenario_output: dict[str, NodeOutput] = {}
            row_scenario_impacts: dict[str, dict[str, ResultValue]] = {}
            if scenario_output_:
                row_scenario_output[scenario] = scenario_output_
            row_default_impacts, row_scenario_impacts_ = get_row_impacts(row)
            if row_scenario_impacts_:
                row_scenario_impacts[scenario] = row_scenario_impacts_
            if node_name not in node_map:
                assert row["output_unit"], f"First row defining '{node_name}' must include 'output_unit'"
                if scenario_output_ or row_scenario_impacts_:
                    assert scenario, f"Row that defines scenario outputs/impacts must also include the scenario"
                if not def_output:
                    def_output = NodeOutput(unit=row["output_unit"], magnitude=1)
                node = SimpleAssignment(node_name=node_name,
                                        output_unit=row["output_unit"],
                                        default_output=def_output,
                                        scenario_outputs=row_scenario_output,
                                        default_impacts=row_default_impacts,
                                        scenario_impacts=row_scenario_impacts)
                node_map[node_name] = node
            else:
                existing_node = node_map[node_name]
                if row['output_unit'] and row['output_unit'] != existing_node.output_unit:
                    raise ValueError(f"Redefinition of output_unit for '{node_name}'")
                assert scenario, f"Repeat row of {node_name} should include a scenario"
                assert existing_node.scenario_outputs  # for mypy
                assert scenario not in existing_node.scenario_outputs, (
                    f"Redefinition of scenario impacts for '{node_name}'")
                assert scenario_output_  # for mypy
                existing_node.scenario_outputs[scenario] = scenario_output_
                existing_node.scenario_impacts[scenario] = row_scenario_impacts_

        self.nodes = node_map
        return self.nodes
