from abc import ABC, abstractmethod
from logging import Logger
from typing import Any, Optional

from enbios.base.scenario import Scenario
from enbios.generic.enbios2_logging import get_logger
from enbios.models.models import AdapterModel, NodeOutput, ResultValue


class EnbiosAdapter(ABC):
    def __init__(self):
        self._config = None

    @abstractmethod
    def validate_definition(self, definition: AdapterModel):
        pass

    @abstractmethod
    def validate_config(self, config: Optional[dict[str, Any]]):
        """
        This is the first validator to be called. Validate the config. The creator may store anything in the
        adapter object here.
        :param config:
        """
        pass

    @abstractmethod
    def validate_methods(self, methods: Optional[dict[str, Any]]) -> list[str]:
        pass

    @abstractmethod
    def validate_node(self, node_name: str, node_config: Any):
        pass

    @abstractmethod
    def validate_scenario_node(
        self, node_name: str, scenario_name: str, scenario_node_data: Any
    ):
        pass

    @abstractmethod
    def get_node_output(self, node_name: str, scenario: str) -> list[NodeOutput]:
        pass

    @abstractmethod
    def get_method_unit(self, method_name: str) -> str:
        pass

    @abstractmethod
    def run_scenario(self, scenario: Scenario) -> dict[str, dict[str, ResultValue]]:
        """
        Run a specific scenario. The adapter should return a dictionary of the form:
            {
                node_name: {
                    method_name: ResultValue (unit, magnitude)
                }
            }
        :param scenario:
        :return:
        """
        pass

    @staticmethod
    @abstractmethod
    def node_indicator() -> str:
        pass

    @staticmethod
    @abstractmethod
    def get_config_schemas() -> dict[str, dict[str, Any]]:
        pass

    @staticmethod
    @abstractmethod
    def name() -> str:
        pass

    def get_logger(self) -> Logger:
        return get_logger(f"({self.name()})")

    def result_extras(self, node_name: str) -> dict[str, Any]:
        return {}
