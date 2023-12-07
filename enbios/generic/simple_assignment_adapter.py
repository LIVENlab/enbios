from typing import Any, Optional

from pydantic import BaseModel, model_validator

from enbios.base.adapters_aggregators.adapter import EnbiosAdapter
from enbios.base.scenario import Scenario
from enbios.generic.unit_util import get_output_in_unit
from enbios.models.experiment_models import ResultValue, ActivityOutput


class SimpleAssignment(BaseModel):
    activity: str
    output_unit: str
    default_output: ActivityOutput
    default_impact: Optional[ResultValue] = None
    scenario_outputs: Optional[dict[str, ActivityOutput]] = None
    scenario_impacts: Optional[dict[str, dict[str, ResultValue]]] = None

    @model_validator(mode='before')
    @classmethod
    def validator(cls, data: Any) -> Any:
        if "default_output" not in data:
            data["default_output"] = {"unit": data["output_unit"], "magnitude": 1}
        return data



class SimpleAssignmentDefinition(BaseModel):
    methods: dict[str, str] # name: unit
    activities: dict[str, SimpleAssignment]


class SimpleAssignmentAdapter(EnbiosAdapter):

    def __init__(self):
        super().__init__()
        self.activities: dict[str, SimpleAssignment] = {} # name: activity
        self.methods: Optional[dict[str, str]] = None # name: unit

    def validate_definition(self, definition: dict[str, Any]):
        SimpleAssignmentDefinition(**definition)

    def validate_config(self, config: Optional[dict[str, Any]]):
        pass

    def validate_methods(self, methods: Optional[dict[str, Any]]) -> list[str]:
        self.methods = methods
        return list(methods.keys())

    def validate_activity_output(self, node_name: str, target_output: ActivityOutput) -> float:
        return get_output_in_unit(target_output, self.activities[node_name].output_unit)

    def validate_activity(self, node_name: str, activity_config: Any, output: ActivityOutput,
                          required_output: bool = False):
        self.activities[node_name] = SimpleAssignment(**{**{"activity": node_name} | activity_config})

    def get_activity_output_unit(self, activity_name: str) -> str:
        return self.activities[activity_name].output_unit

    def get_method_unit(self, method_name: str) -> str:
        return self.methods[method_name]

    def get_default_output_value(self, activity_name: str) -> float:
        return self.activities[activity_name].default_output.magnitude

    def run(self):
        pass

    def run_scenario(self, scenario: Scenario) -> dict[str, dict[str, ResultValue]]:
        return {}

    @property
    def activity_indicator(self) -> str:
        return "assign"

    @property
    def name(self) -> str:
        return "simple-assignment-adapter"
