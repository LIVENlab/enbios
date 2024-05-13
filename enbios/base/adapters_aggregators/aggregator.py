from abc import abstractmethod
from typing import Optional, Any

from enbios.base.adapters_aggregators.node_module import EnbiosNodeModule
from enbios.generic.enbios2_logging import get_logger
from enbios.generic.tree.basic_tree import BasicTreeNode
from enbios.base.models import AggregationModel, output_merge_type, ScenarioResultNodeData


class EnbiosAggregator(EnbiosNodeModule[AggregationModel]):
    @abstractmethod
    def aggregate_node_output(
        self,
        node: BasicTreeNode[ScenarioResultNodeData],
        scenario_name: Optional[str] = "",
    ) -> output_merge_type:
        pass

    # @abstractmethod
    # def validate_scenario_node(
    #     self, node_name: str, scenario_name: str, scenario_node_data: Any
    # ):
    #     """
    #     Validates the output of a node within a scenario. Is called for each node within a scenario.
    #     :param node_name: Name of the node in the hierarchy.
    #     :param scenario_name: Name of scenario
    #     :param scenario_node_data: The output or config of the node in the scenario
    #     """
    #     pass

    @abstractmethod
    def aggregate_node_result(
        self, node: BasicTreeNode[ScenarioResultNodeData], scenario_name: str
    ):
        pass

    # @abstractmethod
    # def aggregate_node_result(self, node: BasicTreeNode[ScenarioResultNodeData], scenario_name: str):
    #     pass

    @staticmethod
    @abstractmethod
    def node_indicator() -> str:
        pass

    @staticmethod
    @abstractmethod
    def name() -> str:
        pass

    @staticmethod
    @abstractmethod
    def get_config_schemas() -> dict:
        pass

    def result_extras(self, node_name: str, scenario_name: str) -> dict[str, Any]:
        return {}

    def get_logger(self):
        return get_logger(f"__name__ ({self.name})")
