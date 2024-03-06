import re
from copy import deepcopy
from pathlib import Path
from typing import Any, Optional, Literal

from pydantic import BaseModel, model_validator, Field

from enbios.base.adapters_aggregators.adapter import EnbiosAdapter
from enbios.base.scenario import Scenario
from enbios.generic.files import ReadPath
from enbios.generic.unit_util import get_output_in_unit, unit_match
from enbios.models.experiment_base_models import AdapterModel, NodeOutput
from enbios.models.experiment_models import ResultValue


class SimpleAssignmentNodeConfig(BaseModel):
    node_name: str
    output_units: list[str]
    default_output: list[NodeOutput]
    default_impacts: dict[str, ResultValue] = Field(default_factory=dict)
    scenario_data: dict[str, "SimpleAssignmentNodeScenarioData"] = Field(
        default_factory=dict
    )

    @model_validator(mode="before")  # type: ignore
    def validator(data: Any):
        if "default_output" not in data:
            data["default_output"] = {"unit": data["output_unit"], "magnitude": 1}
        if "default_impacts" not in data:
            if "scenario_data" not in data or "impacts" not in data["scenario_data"]:
                raise ValueError(
                    "Either default_impacts or scenario_data.impacts must be defined"
                )
        return data


class SimpleAssignmentNodeScenarioData(BaseModel):
    outputs: list[NodeOutput] = Field(default_factory=list)
    impacts: dict[str, ResultValue] = Field(default_factory=dict)


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
        self.nodes: dict[str, SimpleAssignmentNodeConfig] = {}  # name: node
        self.methods: dict[str, str] = {}  # name: unit
        self.definition: SimpleAssignmentDefinition = SimpleAssignmentDefinition(
            methods={}
        )  # placeholder
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
            self.nodes[node_name] = SimpleAssignmentNodeConfig(
                **{**{"node_name": node_name} | node_config}
            )

    def validate_scenario_node(
            self, node_name: str, scenario_name: str, scenario_node_data: Any
    ):
        if self.from_csv_file:
            scenario_output: list[NodeOutput] = []
            node_data = self.nodes[node_name]
            if scenario_name in node_data.scenario_data:
                scenario_data = node_data.scenario_data[scenario_name]
                if scenario_data.outputs:
                    scenario_output = scenario_data.outputs
            if not scenario_output:
                scenario_output = node_data.default_output
            #return get_output_in_unit(scenario_output, node_data.output_units)

        self.get_logger().warning("fix node output")
        # TODO
        # return get_output_in_unit(scenario_node_data, self.nodes[node_name].output_unit)

    # def get_node_output_unit(self, node_name: str) -> str:
    #     return self.nodes[node_name].output_unit

    def get_node_output(self, node_name: str, scenario_name: str) -> list[NodeOutput]:
        node_data = self.nodes[node_name]
        if scenario_name in self.nodes[node_name].scenario_data:
            return node_data.scenario_data[scenario_name]
        return node_data.default_output


    def get_method_unit(self, method_name: str) -> str:
        return self.methods[method_name]

    def get_default_output_value(self, node_name: str) -> float:
        return self.nodes[node_name].default_output.magnitude

    def run_scenario(self, scenario: Scenario) -> dict[str, dict[str, ResultValue]]:
        result: dict[str, dict[str, ResultValue]] = {}
        for node, node_data in self.nodes.items():
            result[node] = deepcopy(node_data.default_impacts)
            if scenario.name in node_data.scenario_data:
                scenario_data = node_data.scenario_data[scenario.name]
                result[node].update(scenario_data.impacts)
        return result

    @staticmethod
    def node_indicator() -> str:
        return "assign"

    @staticmethod
    def get_config_schemas() -> dict:
        return {"node_name": SimpleAssignmentNodeConfig.model_json_schema()}

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

        # scenario_impacts_XXX_unit

        # scenario_impacts_XXX_magnitude

        :param file_path:
        :return:
        """

        __default = "default"
        __scenario = "scenario"
        __output = "output"
        __impacts = "impacts"
        __unit = "unit"
        __magnitude = "magnitude"

        id_re_p = "[1-9a-zA-Z][0-9a-z-A-Z]*"

        # TODO validate output and impacts units
        # one default output and multiple different scenario impacts would be weird...
        rows = ReadPath(file_path).read_data()
        headers = list(h.strip() for h in rows[0].keys())
        output_unit_re = re.compile(f"^{__output}_{id_re_p}_{__unit}$")
        assert all(
            any((output_unit_re.match(h) for h in headers)) for k in [re.compile("node_name"), output_unit_re]
        ), "'node_name' and 'node_{i}_output' must be defined"

        output_units: dict[str, str] = {}

        default_output_headers: dict[str, dict[str, str]] = {}
        scenario_output_headers: dict[str, dict[str, str]] = {}
        default_impacts_headers: dict[str, dict[str, str]] = {}
        scenario_impacts_headers: dict[str, dict[str, str]] = {}

        collections = [default_output_headers, scenario_output_headers,
                       default_impacts_headers, scenario_impacts_headers]

        group_regexes = [[col, []] for col in collections]
        for def_o_sce in (__default, __scenario):
            for out_o_impact in [__output, __impacts]:
                for unit_o_mag in [__unit, __magnitude]:
                    reg_ = re.compile(f"^{def_o_sce}_{out_o_impact}_{id_re_p}_{unit_o_mag}$")
                    if def_o_sce == __default:
                        if out_o_impact == __output:
                            group_regexes[0][1].append(reg_)
                        else:
                            group_regexes[2][1].append(reg_)
                    else:
                        if out_o_impact == __output:
                            group_regexes[1][1].append(reg_)
                        else:
                            group_regexes[3][1].append(reg_)

        for header in headers:
            parts = header.split("_")
            if len(parts) == 3:
                match_ = output_unit_re.match(header)
                if match_:
                    output_units[parts[1]] = header
                continue
            if len(parts) != 4:
                continue
            for idx, (group, regexes) in enumerate(group_regexes):
                for (reg, unit_o_mag) in zip(tuple(regexes), [__unit, __magnitude]):
                    match_ = reg.match(header)
                    if match_:
                        id_: str = parts[2]  # number or impact name
                        u_m: str = parts[3]
                        if id_ in group:
                            if group[id_].get(unit_o_mag):
                                raise ValueError(f"already set {parts}")
                        else:
                            group[id_] = {}
                        group[id_][unit_o_mag] = header

        for col in collections:
            for col_id_headers in col.values():
                assert (len(col_id_headers) == 2), f"row not complete: {headers}"

        # check if the impacts exists in the methods
        for impact in list(default_impacts_headers.keys()) + list(scenario_impacts_headers.keys()):
            if impact not in self.methods.keys():
                raise ValueError(
                    f"Header includes undefined impact: '{impact}'."
                    f"Options are {self.methods.values()}"
                )

        if scenario_impacts_headers:
            assert "scenario" in headers, "header: 'scenario' is missing"

        node_map: dict[str, SimpleAssignmentNodeConfig] = {}

        def get_outputs(row_: dict, type_: Literal[__default, __scenario]
                        ) -> list[NodeOutput]:
            coll = scenario_output_headers if type_ == __scenario else default_output_headers
            result: list[NodeOutput] = []
            for (id_, id_out_headers) in coll.items():
                unit_header, mag_header = (id_out_headers["unit"], id_out_headers["magnitude"])
                if row_[unit_header] or row_[mag_header]:
                    # todo not sure about this assert, we could just use output_{i}_unit
                    assert row_[unit_header] and float(row_[mag_header])
                    unit = row_[unit_header]
                    assert unit_match(unit, row[output_units[id_]])
                    result.append(NodeOutput(
                        unit=unit,
                        magnitude=float(row_[mag_header]),
                    ))

            return result

        def get_impacts(
                row_: dict, type_: Literal["default", "scenario"]
        ) -> dict[str, ResultValue]:
            coll = default_impacts_headers if type_ == "default" else scenario_impacts_headers
            result: dict[str, ResultValue] = {}
            for (id_, id_impact_headers) in coll.items():
                unit_header, mag_header = (id_impact_headers["unit"], id_impact_headers["magnitude"])
                if row_[unit_header] or row_[mag_header]:
                    # todo not sure about this assert, we could just use output_{i}_unit
                    assert row_[unit_header] and float(row_[mag_header])
                    unit = row_[unit_header]
                    # assert unit_match(unit, row[output_units[id_]])
                    result[id_] = ResultValue(
                        unit=unit,
                        magnitude=float(row_[mag_header]),
                    )

            return result

        for idx, row in enumerate(rows):
            assert row.get("node_name"), "All rows must include a 'node_name'"
            node_name = row.get("node_name")
            try:
                row = {k.strip(): (v.strip() if v else v) for k, v in row.items()}
                node: SimpleAssignmentNodeConfig
                # new node
                if node_name not in node_map:
                    #
                    row_out_units: dict[str, str] = {id_: row.get(out_unit_h) for (id_, out_unit_h) in
                                                     output_units.items()}
                    assert all(
                        row_out_units.values()), f"First row defining '{node_name}' must include 'output_units' {row_out_units}"
                    default_outputs = get_outputs(row, "default")
                    default_impacts: dict[str, ResultValue] = get_impacts(row, "default")
                    #             for impact_name, result in default_impacts.items():
                    #                 assert unit_match(result.unit, self.methods[impact_name])
                    node = SimpleAssignmentNodeConfig(
                        node_name=node_name,
                        output_units=list(row_out_units.values()),
                        default_output=default_outputs,
                        scenario_data={},
                        default_impacts=default_impacts,
                    )
                    node_map[node_name] = node
                else:
                    pass
                    node = node_map[node_name]

                    assert all([row.get(out_unit_h) == "" for out_unit_h in
                                output_units.values()]), f"Redefinition of output_unit for '{node_name}'"
                    assert scenario_output_headers, (
                        f"Multiple rows per node, means header scenario_output_<i>_mangintude"
                        f"needs to be included"
                    )
                    scenario: str = row.get("scenario")
                    assert scenario, "No scenario defined"
                    scenario_outputs = get_outputs(row, __scenario)
                    assert scenario_outputs, "For each scenario row, a new output needs to be defined"

                    assert scenario not in node.scenario_data, (  # type: ignore
                        f"Redefinition of scenario impacts for '{node_name}'"
                    )
            #         if scenario:
            #             assert row.get(scenario_out_mag_header)
            #             unit = (
            #                 row.get(scenario_out_unit_header)
            #                 if row.get(scenario_out_unit_header)
            #                 else node.output_unit
            #             )
            #             assert unit_match(unit, node.output_unit)
            #
                    row_impacts = get_impacts(row, "scenario")
                    for impact_name, result in row_impacts.items():
                        assert unit_match(result.unit, self.methods[impact_name])
                    node.scenario_data[scenario] = SimpleAssignmentNodeScenarioData(
                        outputs=scenario_outputs,
                        impacts=row_impacts,
                    )
            #
            except Exception as err:
                logger = self.get_logger()
                logger.error(f"Error in row {idx} of '{node_name}'")
                logger.error(row)
                logger.error(err)
                raise err
        self.nodes = node_map
        return self.nodes
