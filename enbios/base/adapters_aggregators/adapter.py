from abc import ABC, abstractmethod
from typing import Any, Optional

from enbios.base.scenario import Scenario
from enbios.generic.enbios2_logging import get_logger
from enbios.models.experiment_models import (
    ActivityOutput,
    ResultValue, AdapterModel,
)


class EnbiosAdapter(ABC):
    def __init__(self):
        self._config = None

    @abstractmethod
    def validate_definition(self, definition: AdapterModel):
        pass

    @abstractmethod
    def validate_config(self, config: Optional[dict[str, Any]]):
        """
        This is the first validator to be called. Validate the config. The creator may store anything in the adapter object here.
        :param config:
        :return:
        """
        pass

    @abstractmethod
    def validate_methods(self, methods: Optional[dict[str, Any]]) -> list[str]:
        pass

    @abstractmethod
    def validate_activity_output(
            self, node_name: str, target_output: ActivityOutput
    ) -> float:
        pass

    @abstractmethod
    def validate_activity(
            self,
            node_name: str,
            activity_id: Any,
            output: ActivityOutput,
            required_output: bool = False,
    ):
        pass

    @abstractmethod
    def get_activity_output_unit(self, activity_name: str) -> str:
        pass

    @abstractmethod
    def get_method_unit(self, method_name: str) -> str:
        pass

    @abstractmethod
    def get_default_output_value(self, activity_name: str) -> float:
        pass

    @abstractmethod
    def run(self):
        pass

    @abstractmethod
    def run_scenario(self, scenario: Scenario) -> dict[str, dict[str, ResultValue]]:
        """
        Run a specific scenario. The adapter should return a dictionary of the form:
            {
                activity_name: {
                    method_name: ResultValue (unit, amount)
                }
            }
        :param scenario:
        :return:
        """
        pass

    @property
    @abstractmethod
    def activity_indicator(self) -> str:
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    def get_logger(self):
        return get_logger(f"__name__ ({self.name})")