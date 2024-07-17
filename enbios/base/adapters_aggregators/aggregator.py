from abc import abstractmethod
from typing import Optional

from enbios.base.adapters_aggregators.node_module import EnbiosNodeModule
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

    @abstractmethod
    def aggregate_node_result(
        self, node: BasicTreeNode[ScenarioResultNodeData], scenario_name: str
    ):
        pass
