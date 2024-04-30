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
        """
        This is the first validator to be called. Validates the whole adapter definition, which is the whole dictionary (parse as enbios.models.models.AdapterModel)
        :param definition: the whole adapter definition (containing 'config' and 'methods')
        """
        pass

    @abstractmethod
    def validate_config(self, config: Optional[dict[str, Any]]):
        """
        Validate the config. The creator may store anything in the
        adapter object through this method.
        :param config: the configuration of the adapter, which might have its own BaseModel. For understanding the structure,
        it makes sense to provide this model as a return value of "adapter" in the get_config_schemas() method.
        """
        pass

    @abstractmethod
    def validate_methods(self, methods: Optional[dict[str, Any]]) -> list[str]:
        """
        Validate the methods. The creator might store method specific data in the adapter through this method.
        :param methods: A dictionary of method names and their config (identifiers for the adapter).
        :return: list of method names
        """
        pass

    @abstractmethod
    def validate_node(self, node_name: str, node_config: Any):
        """
        Validate one node. This method is called for each node experiment config (hierarchy) that is using this adapter.
        :param node_name: name of the node in the hierarchy
        :param node_config: Configuration of the node (data for identification and default outputs...)
        """
        pass

    @abstractmethod
    def validate_scenario_node(
            self, node_name: str, scenario_name: str, scenario_node_data: Any
    ):
        """
        Validates the output of a node within a scenario. Is called for each node within a scenario.
        :param node_name: Name of the node in the hierarchy.
        :param scenario_name: Name of scenario
        :param scenario_node_data: The output or config of the node in the scenario
        """

    @abstractmethod
    def get_node_output(self, node_name: str, scenario: str) -> list[NodeOutput]:
        """
        The output of a node for a scenario. A list of NodeOutput objects.
        :param node_name: Name of the node in the hierarchy
        :param scenario: Name of the scenario
        :return: Multiple NodeOutput objects.
        """
        pass

    @abstractmethod
    def get_method_unit(self, method_name: str) -> str:
        """
        Unit of a method
        :param method_name:
        :return:
        """
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
        :return: Returns a dictionary node-name: (method-name: results)
        """
        pass

    @staticmethod
    @abstractmethod
    def node_indicator() -> str:
        """
        This string can be used in order to indicate that a node in the hierarchy should use this adapter.
        :return: node-indicator string
        """
        pass

    @staticmethod
    @abstractmethod
    def get_config_schemas() -> dict[str, dict[str, Any]]:
        """
        Get the Jsonschema for the adapter. These can be derived, when there are pydantic based models for validation
        (using the `model_json_schema` function). The structure of the return value should correspond to the three parts of validation,
         the adapter-config, the activity-configs in the hierarchy and the methods.
        :return: dictionary, where each key corresponds to one part of validation (proposed keys: `adapter`, `activty` and `method`.
        """
        pass

    @staticmethod
    @abstractmethod
    def name() -> str:
        """
        Name of the adapter (which can also used to indicate in the hierarchy that a node should use this adapter.
        :return: string: name of the adapter
        """
        pass

    def get_logger(self) -> Logger:
        """
        Logger of this adapter. Use this inside the adapter.
        :return: Use this to make logs inside the adapter
        """
        return get_logger(f"({self.name()})")

    def result_extras(self, node_name: str, scenario_name: str) -> dict[str, Any]:
        """

        :param node_name: Name of the node in the hierarchy
        :param scenario_name: Name of the scenario
        :return: A dictionary of string values pairs. The values should be primitives (like int, or string) since, they
        are generally serialized.
        """
        return {}
