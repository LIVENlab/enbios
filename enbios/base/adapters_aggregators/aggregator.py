from abc import ABC, abstractmethod
from typing import Optional, Any

from enbios.generic.enbios2_logging import get_logger
from enbios.generic.tree.basic_tree import BasicTreeNode
from enbios.models.experiment_base_models import NodeOutput
from enbios.models.experiment_models import ScenarioResultNodeData


class EnbiosAggregator(ABC):
    @abstractmethod
    def validate_config(self, config: Optional[dict[str, Any]]):
        pass

    @abstractmethod
    def validate_node(self, node_name: str, node_config: Any):
        pass

    @abstractmethod
    def aggregate_node_output(
        self,
        node: BasicTreeNode[ScenarioResultNodeData],
        scenario_name: Optional[str] = "",
    ) -> Optional[NodeOutput]:
        pass

    @abstractmethod
    def aggregate_node_result(self, node: BasicTreeNode[ScenarioResultNodeData]):
        pass

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

    def get_logger(self):
        return get_logger(f"__name__ ({self.name})")
