from abc import ABC, abstractmethod
from logging import Logger
from typing import TypeVar, Generic, Optional, Any

from pydantic import BaseModel

from enbios.generic.enbios2_logging import get_logger

T = TypeVar('T', bound=BaseModel)


class EnbiosNodeModule(ABC, Generic[T]):

    @abstractmethod
    def validate_definition(self, definition: T):
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
    def validate_node(self, node_name: str, node_config: Any):
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
