from typing import Any, Optional

from pydantic import BaseModel, model_validator

from enbios.base.adapters_aggregators.adapter import EnbiosAdapter
from enbios.base.scenario import Scenario
from enbios.generic.unit_util import get_output_in_unit
from enbios.models.experiment_base_models import AdapterModel, NodeOutput
from enbios.models.experiment_models import ResultValue


class SimpleAssignment(BaseModel):
    node_name: str
    output_unit: str
    default_output: NodeOutput
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


class SimpleAssignmentDefinition(BaseModel):
    methods: dict[str, str]  # name: unit
    # activities: dict[str, SimpleAssignment]


class SimpleAssignmentAdapter(EnbiosAdapter):
    @staticmethod
    def name():
        return "simple-assignment-adapter"

    def __init__(self):
        super().__init__()
        self.nodes: dict[str, SimpleAssignment] = {}  # name: node
        self.methods: Optional[dict[str, str]] = None  # name: unit

    def validate_definition(self, definition: AdapterModel):
        SimpleAssignmentDefinition(**definition.model_dump())

    def validate_config(self, config: Optional[dict[str, Any]]):
        pass

    def validate_methods(self, methods: Optional[dict[str, Any]]) -> list[str]:
        self.methods = methods
        return list(methods.keys())

    def validate_node_output(self, node_name: str, target_output: NodeOutput) -> float:
        return get_output_in_unit(target_output, self.nodes[node_name].output_unit)

    def validate_node(self, node_name: str, node_config: Any):
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
        result = {}
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
