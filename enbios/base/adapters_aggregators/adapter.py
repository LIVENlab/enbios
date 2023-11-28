from abc import ABC, abstractmethod
from typing import Any, Optional

from enbios.base.scenario import Scenario
from enbios.models.experiment_models import ActivityOutput, ExperimentActivityId


class EnbiosAdapter(ABC):

    def __init__(self):
        self._config = None

    @abstractmethod
    def validate_config(self, config: dict[str, Any]):
        pass

    @abstractmethod
    def validate_methods(self):
        pass

    @abstractmethod
    def validate_activity_output(self, node_name: str, target_output: ActivityOutput):
        pass

    @abstractmethod
    def validate_activity(self,
                          node_name: str,
                          activity_id: ExperimentActivityId,
                          output: ActivityOutput,
                          required_output: bool = False):
        pass

    @abstractmethod
    def get_activity_output_unit(self, activity_name: str) -> str:
        pass

    @abstractmethod
    def get_activity_result_units(self, activity_name: str) -> list[str]:
        pass

    @abstractmethod
    def get_default_output_value(self, activity_name: str) -> float:
        pass

    @abstractmethod
    def prepare_scenario(self, scenario: Scenario):
        pass

    @abstractmethod
    def run(self):
        pass

    @abstractmethod
    def run_scenario(self, scenario: Scenario) -> dict[str, Any]:
        pass

    @property
    @abstractmethod
    def activity_indicator(self) -> str:
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        pass
