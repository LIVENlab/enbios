from abc import abstractmethod
from typing import Any, Optional

from enbios.base.adapters_aggregators.node_module import EnbiosNodeModule
from enbios.base.scenario import Scenario
from enbios.models.models import AdapterModel, NodeOutput, ResultValue


class EnbiosAdapter(EnbiosNodeModule[AdapterModel]):
    def __init__(self):
        self._config = None

    @abstractmethod
    def validate_methods(self, methods: Optional[dict[str, Any]]) -> list[str]:
        """
        Validate the methods. The creator might store method specific data in the adapter through this method.
        :param methods: A dictionary of method names and their config (identifiers for the adapter).
        :return: list of method names
        """
        pass

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
