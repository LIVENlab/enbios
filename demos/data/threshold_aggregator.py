from typing import Any

from pydantic import BaseModel, Field

from enbios import BasicTreeNode, ScenarioResultNodeData, ResultValue
from enbios.base.adapters_aggregators.builtin import SumAggregator


class MethodThreshold(BaseModel):
    method: str
    threshold: float


class NodeThresholdConfig(BaseModel):
    method_thresholds: list[MethodThreshold] = Field(default_factory=list)


class ThresholdAggregator(SumAggregator):

    def __init__(self):
        super().__init__()
        self.node_thresholds: dict[str, NodeThresholdConfig] = {}
        self.threshold_results: dict[str, dict[str, bool]] = {}

    def validate_node(self, node_name: str, node_config: Any):
        if node_config:
            self.node_thresholds[node_name] = NodeThresholdConfig.model_validate(node_config)

    def name(self) -> str:
        return "sum-threshold-aggregator"

    def node_indicator(self) -> str:
        return "threshold"

    def aggregate_node_result(
            self, node: BasicTreeNode[ScenarioResultNodeData]
    ) -> dict[str, ResultValue]:
        sum_ = super().aggregate_node_result(node)
        if node.name in self.node_thresholds:
            node_thresholds = self.node_thresholds[node.name]
            self.threshold_results[node.name] = {}
            for method_threshold in node_thresholds.method_thresholds:
                if method_threshold.method in sum_:
                    method = method_threshold.method
                    self.threshold_results[node.name][method] = sum_[method].magnitude >= method_threshold.threshold
        return sum_

    def result_extras(self, node_name: str) -> dict[str, Any]:
        results = self.threshold_results.get(node_name, {})
        if results:
            return {"threshold_results": results}
        else:
            return {}
