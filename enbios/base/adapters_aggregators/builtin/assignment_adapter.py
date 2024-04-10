import re
from contextlib import contextmanager
from contextvars import ContextVar
from copy import deepcopy
from pathlib import Path
from typing import Any, Optional, Literal, Iterator, Union

from pydantic import BaseModel, model_validator, Field, ConfigDict, field_validator
from pydantic_core.core_schema import ValidationInfo

from enbios.base.adapters_aggregators.adapter import EnbiosAdapter
from enbios.base.scenario import Scenario
from enbios.base.unit_registry import ureg
from enbios.generic.files import ReadPath
from enbios.generic.unit_util import unit_match
from enbios.models.models import (
    AdapterModel,
    NodeOutput,
    EnbiosValidationException,
    ResultValue,
)

unit_ = "unit"

class AssignmentNodeOutputConfig(BaseModel):
    unit: str
    label: Optional[str] = None

    @model_validator(mode="before")
    def transform_value(cls, v: Any) -> dict:
        if isinstance(v, dict):
            if v.get("label") == "":
                v["label"] = None
        return v


class AssignmentNode(BaseModel):
    model_config = ConfigDict(extra="forbid")
    node_name: str
    outputs: list["AssignmentNodeOutputConfig"]
    default_outputs: list[Optional[NodeOutput]] = Field(default_factory=list)
    default_impacts: dict[str, ResultValue] = Field(default_factory=dict)
    scenario_data: dict[str, "AssignmentNodeScenarioData"] = Field(
        default_factory=dict
    )

    @model_validator(mode="before")  # type: ignore
    def validator(data: Any):
        # if "default_impacts" not in data:
        #     if "scenario_data" not in data or "impacts" not in data["scenario_data"]:
        #         raise ValueError(
        #             "Either default_impacts or scenario_data.impacts must be defined"
        #         )
        if (default_outputs := data.get("default_outputs")) is not None:
            if not len(data["outputs"]) == len(default_outputs):
                raise EnbiosValidationException(
                    "Length of 'default_output' and 'output_units' do not match",
                    "SimpleAssignmentNodeConfig-unequal unit length",
                )

            outputs = data["outputs"]
            outputs_units = [o.get(unit_) for o in outputs]
            for idx, (unit, def_out) in enumerate(zip(outputs_units, default_outputs)):
                if def_out:
                    def_out_unit = def_out.get(unit_)
                    if not def_out_unit:
                        def_out[unit_] = unit
                    else:
                        assert unit_match(
                            unit, def_out.get(unit_, unit)
                        ), (f"unit of 'default_output' : {default_outputs}, "
                            f"does not match specified unit: '{unit}' (index: {idx})")

                    def_out["label"] = outputs[idx].get("label", None)

        return data


class AssignmentNodeScenarioData(BaseModel):
    model_config = ConfigDict(extra="forbid")
    outputs: list[NodeOutput] = Field(default_factory=list)
    impacts: dict[str, ResultValue] = Field(default_factory=dict)

    def __init__(self, /, **data: Any) -> None:
        self.__pydantic_validator__.validate_python(
            data,
            self_instance=self,
            context=_init_context_var.get(),
        )

    @field_validator('outputs', mode="before")
    @classmethod
    def output_with_context(cls, value: list[Union[NodeOutput,dict]], info: ValidationInfo) -> list[NodeOutput]:
        if not info.context:
            raise ValueError("Always call AssignmentNodeScenarioData with node as context. Should not happen..")

        node = info.context.get('node')
        outputs = node.outputs
        output_len = len(outputs)
        # check length
        if len(value) != output_len:
            raise EnbiosValidationException(f"Length of scenario output must match ouputs: {output_len}. "
                                            f"Found length: {len(value)}")

        # set unit if missing
        for idx, output in enumerate(value):
            if output:
                node_out_unit = outputs[idx].unit
                if isinstance(output, NodeOutput):
                    output = output.model_dump()
                if not output.get(unit_):
                    output[unit_] = node_out_unit

                output["label"] = outputs[idx].label
                value[idx] = output
        return value


class AssignmentAdapterConfig(BaseModel):
    source_csv_file: Optional[Path] = Field(None)


class AssignmentAdapterDefinition(BaseModel):
    methods: dict[str, str]  # name: unit
    config: Optional["AssignmentAdapterConfig"] = Field(default_factory=AssignmentAdapterConfig)  # type: ignore


_init_context_var = ContextVar('_init_context_var', default=None)


@contextmanager
def init_node_context(value: dict[str, Any]) -> Iterator[None]:
    token = _init_context_var.set(value)
    try:
        yield
    finally:
        _init_context_var.reset(token)


class AssignmentAdapter(EnbiosAdapter):
    @staticmethod
    def name():
        return "assignment-adapter"

    def __init__(self) -> None:
        super().__init__()
        self.nodes: dict[str, AssignmentNode] = {}  # name: node
        self.methods: dict[str, str] = {}  # name: unit
        self.definition: AssignmentAdapterDefinition = AssignmentAdapterDefinition(
            methods={}
        )  # placeholder
        self.from_csv_file: bool = False

    def validate_definition(self, definition: AdapterModel):
        self.definition = AssignmentAdapterDefinition(**definition.model_dump())

    def validate_config(self, config: Optional[dict[str, Any]]):
        self.validate_methods(self.definition.methods)
        assert self.definition.config  # that's just for mypy
        if self.definition.config.source_csv_file:
            self.nodes = self.read_nodes_from_csv(self.definition.config.source_csv_file)
            self.from_csv_file = True

    def validate_methods(self, methods: Optional[dict[str, Any]]) -> list[str]:
        if methods:
            self.methods = methods
        if methods:
            return list(methods.keys())
        else:
            return []

    def _validate_impact(self, method_name, result) -> tuple[bool, bool]:
        method_name_valid = method_name in self.methods.keys()
        unit_valid = unit_match(self.methods[method_name], result.unit)
        return method_name_valid, unit_valid

    def validate_node(self, node_name: str, node_config: Any):
        if self.from_csv_file:
            node = self.nodes[node_name]
            for method, result in node.default_impacts.items():
                method_name_valid, unit_valid = self._validate_impact(method, result)
                if not method_name_valid:
                    raise EnbiosValidationException(
                        f"Node {node_name} specifies undefined default-impact method: '{method}'"
                    )
                if not unit_valid:
                    raise EnbiosValidationException(
                        f"Node specifies a default-impact '{method}' with unit: '{result.unit}' "
                        f"that does not match unit specified in adapter '{self.methods[method]}'"
                    )
        if not self.from_csv_file:
            self.nodes[node_name] = AssignmentNode(
                **{**{"node_name": node_name} | node_config}
            )

    def validate_scenario_node(
            self, node_name: str, scenario_name: str, scenario_node_data: Any
    ):
        if self.from_csv_file:
            # todo validate output...
            node_data = self.nodes[node_name]
            if scenario_data := node_data.scenario_data.get(scenario_name):
                for method, result in scenario_data.impacts.items():
                    method_name_valid, unit_valid = self._validate_impact(method, result)
                    if not method_name_valid:
                        raise EnbiosValidationException(
                            f"Node {node_name} specifies undefined scenario-impact method: '{method}' "
                            f"for scenario '{scenario_name}'"
                        )
                    if not unit_valid:
                        raise EnbiosValidationException(
                            f"Node specifies a scenario-impact '{method}' with unit: '{result.unit}' "
                            f"that does not match unit specified in adapter '{self.methods[method]}'"
                            f"for scenario '{scenario_name}'"
                        )
        else:
            node = self.nodes[node_name]
            with init_node_context({'node': node}):
                node.scenario_data[
                    scenario_name
                ] = AssignmentNodeScenarioData(**scenario_node_data)

    def get_node_output(self, node_name: str, scenario_name: str) -> list[NodeOutput]:
        node_data = self.nodes[node_name]
        outputs: list[NodeOutput]
        if scenario_name in self.nodes[node_name].scenario_data:
            outputs = node_data.scenario_data[scenario_name].outputs
        else:
            if not node_data.default_outputs:
                self.get_logger().warning(f"Node '{node_name}' has no default outputs. Output will be empty")
            outputs = node_data.default_outputs
        return outputs

    def get_method_unit(self, method_name: str) -> str:
        return self.methods[method_name]

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
        return {"node_name": AssignmentNode.model_json_schema()}

    def read_nodes_from_csv(
            self, file_path: Path
    ) -> dict[str, AssignmentNode]:
        """
        Read nodes default, scenario outputs and impacts from a csv file. Is used when config includes path to
        csv file in
        'source_csv_file'. One row per node/scenario. 1. row of a node is used for its defaults, after that,
        one row per scenario. If no scenario impacts are given all nodes must have default_impacts.

        Format of headers:

        :param file_path:
        :return:
        """

        __default = "default"
        __scenario = "scenario"
        __outputs = "outputs"
        __impacts = "impacts"
        __unit = unit_
        __magnitude = "magnitude"
        __label = "label"

        id_re_str = "[1-9a-zA-Z][0-9a-z-A-Z]*"
        id_re_pat = re.compile(f"^{id_re_str}$")

        # TODO validate output and impacts units
        # one default output and multiple different scenario impacts would be weird...
        rows = ReadPath(file_path).read_data()
        headers = list(h.strip() for h in rows[0].keys())
        output_unit_re = re.compile(f"^{__outputs}_{id_re_str}_{__unit}$")
        output_label_re = re.compile(f"^{__outputs}_{id_re_str}_{__label}$")
        assert all(
            any((k.match(h) for h in headers))
            for k in [re.compile("node_name"), output_unit_re]
        ), "'node_name' and 'node_{i}_output' must be defined"

        # rename to outputs_units_labels str: idx/name: dict. keys: unit, label, value: header
        outputs_headers: dict[str, dict[str, str]] = {}

        # dicts for headers. keys: ids [strings for outputs/impacts ids]
        # value:dict: [str:str] : "unit","magnitude" : <full-header> (e.g. default_output_1_unit)
        default_output_headers: dict[str, dict[str, str]] = {}
        scenario_output_headers: dict[str, dict[str, str]] = {}
        default_impacts_headers: dict[str, dict[str, str]] = {}
        scenario_impacts_headers: dict[str, dict[str, str]] = {}

        flat_collections = [
            default_output_headers,
            scenario_output_headers,
            default_impacts_headers,
            scenario_impacts_headers,
        ]

        struct_collections = {
            __default: {
                __outputs: default_output_headers,
                __impacts: default_impacts_headers,
            },
            __scenario: {
                __outputs: scenario_output_headers,
                __impacts: scenario_impacts_headers,
            },
        }

        for header in headers:
            parts = header.split("_")
            if len(parts) == 3:
                if output_unit_re.match(header):
                    outputs_headers.setdefault(parts[1], {})[__unit] = header
                elif output_label_re.match(header):
                    outputs_headers.setdefault(parts[1], {})[__label] = header
                continue
            if len(parts) != 4:
                continue
            def_o_sce, out_o_impact, id_, unit_o_mag = tuple(parts)
            assert def_o_sce in [__default, __scenario]
            assert out_o_impact in [__outputs, __impacts]
            col = struct_collections[def_o_sce][out_o_impact]
            assert id_re_pat.match(id_)
            assert unit_o_mag in [__unit, __magnitude]
            col.setdefault(id_, {})[unit_o_mag] = header

        for col in flat_collections:
            for col_id_headers in col.values():
                assert len(col_id_headers) >= 2, f"row not complete: {headers}"

        if scenario_impacts_headers:
            assert "scenario" in headers, "header: 'scenario' is missing"

        node_map: dict[str, AssignmentNode] = {}

        # check if the keys for the default/scenario outputs match outputs and put them in the same order as
        outputs_order = list(outputs_headers.keys())
        for type_, type_values in struct_collections.items():
            if set(type_values[__outputs].keys()):
                assert set(type_values[__outputs].keys()) == set(outputs_order), (
                    f"{set(type_values[__outputs].keys())}, {set(type_values[__outputs].keys())}, {set(outputs_order)}")

                type_values[__outputs] = {
                    id_: type_values[__outputs][id_] for id_ in outputs_order
                }

        def get_outputs(row_: dict) -> list[tuple[str, AssignmentNodeOutputConfig]]:
            return [(id_, AssignmentNodeOutputConfig(
                unit=row_.get(output_headers[__unit]),
                label=row_.get(
                    output_headers.get(__label)
                    if output_headers.get(__label)
                    else None
                ),
            )) for (id_, output_headers) in outputs_headers.items()]

        def get_outputs_values(
                row_: dict,
                type_: Literal["default", "scenario"],
                node_: Optional[AssignmentNode] = None,
        ) -> list[NodeOutput]:
            coll = (
                scenario_output_headers if type_ == __scenario else default_output_headers
            )
            result: list[NodeOutput] = []
            for idx, (id_, id_out_headers) in enumerate(coll.items()):
                unit_header, mag_header = (
                    id_out_headers[unit_],
                    id_out_headers[__magnitude],
                )
                if row_[unit_header] or row_[mag_header]:
                    # todo not sure about this assert, we could just use output_{i}_unit
                    assert float(row_[mag_header])
                    unit = row_.get(unit_header)
                    if not unit:
                        unit = node_.outputs[idx].unit if node_ else None
                    assert unit, f"No unit defined for row: {row_} unit id: '{id_}'"
                    # todo: always use node?
                    out_unit = (
                        node_.outputs[idx].unit if node_ else row[outputs_headers[id_]]
                    )
                    # todo check if this can be replaced by new validator implementation...
                    assert unit_match(unit, out_unit), (
                        f"Unit of output at index [{id_}]/{type_} do not match '{unit}' does not match '{out_unit}' "
                        f"for row {row_}"
                    )
                    result.append(
                        NodeOutput(
                            unit=str(ureg(unit).units),
                            magnitude=float(row_[mag_header]),
                            label=node.outputs[idx].label
                        )
                    )

            return result

        def get_impacts(
                row_: dict, type_: Literal["default", "scenario"]
        ) -> dict[str, ResultValue]:
            coll = (
                default_impacts_headers
                if type_ == "default"
                else scenario_impacts_headers
            )
            result: dict[str, ResultValue] = {}
            for id_, id_impact_headers in coll.items():
                unit_header, mag_header = (
                    id_impact_headers[unit_],
                    id_impact_headers[__magnitude],
                )
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
                node: AssignmentNode
                # new node
                if node_name not in node_map:
                    node_outputs: list[tuple[str, AssignmentNodeOutputConfig]] = get_outputs(row)

                    assert all(
                        n[1] for n in node_outputs
                    ), f"First row defining '{node_name}' must include 'output' {node_outputs}"

                    default_impacts: dict[str, ResultValue] = get_impacts(row, "default")
                    node = AssignmentNode(
                        node_name=node_name,
                        outputs=[n[1] for n in node_outputs],
                        scenario_data={},
                        default_impacts=default_impacts,
                    )
                    node.default_outputs = get_outputs_values(row, "default", node)
                    node_map[node_name] = node
                else:
                    node = node_map[node_name]
                    assert all(
                        [
                            row.get(k, "") == ""
                            for output_headers in outputs_headers.values()
                            for k in output_headers.keys()
                        ]
                    ), f"Redefinition of output_unit for '{node_name}'"
                    assert scenario_output_headers, (
                        "Multiple rows per node, means header scenario_output_<i>_magnitude"
                        "needs to be included"
                    )
                    assert row.get("scenario"), "No scenario defined"
                scenario: str = row.get("scenario")
                if scenario:
                    scenario_outputs = get_outputs_values(row, "scenario", node)
                    assert (
                        scenario_outputs
                    ), "For each scenario row, a new output needs to be defined"

                    assert scenario not in node.scenario_data, (  # type: ignore
                        f"Redefinition of scenario impacts for '{node_name}'"
                    )
                    #
                    row_impacts = get_impacts(row, "scenario")
                    for impact_name, result in row_impacts.items():
                        assert unit_match(result.unit, self.methods[impact_name])

                    with init_node_context({'node': node}):
                        node.scenario_data[scenario] = AssignmentNodeScenarioData(
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
        return node_map

    def __repr__(self):
        return "Enbios builtin Assignment Adapter"
