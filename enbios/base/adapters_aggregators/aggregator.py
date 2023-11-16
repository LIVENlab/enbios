from abc import ABC, abstractmethod

from enbios.generic.tree.basic_tree import BasicTreeNode
from enbios.models.experiment_models import TechTreeNodeData, ScenarioResultNodeData


class EnbiosAggregator(ABC):

    @abstractmethod
    def validate_config(self):
        pass

    @abstractmethod
    def validate_node_output(self, node: BasicTreeNode[ScenarioResultNodeData]):
        pass

    @abstractmethod
    def aggregate_results(self, node: BasicTreeNode[ScenarioResultNodeData]):
        pass

    @property
    @abstractmethod
    def node_indicator(self) -> str:
        pass


class SumAggregator(EnbiosAggregator):
    def validate_config(self):
        pass

    def validate_node_output(self, node: BasicTreeNode[TechTreeNodeData]):
        pass

    def aggregate_results(self, node: BasicTreeNode[ScenarioResultNodeData]):
        pass

    @property
    def node_indicator(self) -> str:
        return "sum"
