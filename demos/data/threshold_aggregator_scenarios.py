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
        # node -> threshold
        self.node_thresholds: dict[str, NodeThresholdConfig] = {}
        # scenario -> node -> threshold
        self.scenario_configs: dict[str, dict[str, NodeThresholdConfig]] = {}
        # scenario -> node -> method -> result
        self.threshold_results: dict[str, dict[str, bool]] = {}

    def validate_node(self, node_name: str, node_config: Any):
        if node_config:
            self.node_thresholds[node_name] = NodeThresholdConfig.model_validate(node_config)

    def name(self) -> str:
        return "sum-scenario-threshold-aggregator"

    def node_indicator(self) -> str:
        return "scenario-threshold"

    def validate_scenario_node(
            self, node_name: str, scenario_name: str, scenario_node_data: Any
    ):
        self.scenario_configs.setdefault(scenario_name, {})[node_name] = NodeThresholdConfig.model_validate(
            scenario_node_data)

    def aggregate_node_result(
            self, node: BasicTreeNode[ScenarioResultNodeData],
            scenario_name: str
    ) -> dict[str, ResultValue]:
        sum_ = super().aggregate_node_result(node, scenario_name)

        def threshold_checks(threshold_config: NodeThresholdConfig):
            self.threshold_results[node.name] = {}
            for method_threshold in threshold_config.method_thresholds:
                if method_threshold.method in sum_:
                    method = method_threshold.method
                    self.threshold_results[node.name][method] = sum_[
                                                                    method].magnitude >= method_threshold.threshold

        if scenario_name in self.scenario_configs:
            if node.name in self.scenario_configs[scenario_name]:
                threshold_checks(self.scenario_configs[scenario_name][node.name])

        elif node.name in self.node_thresholds:
            threshold_checks(self.node_thresholds[node.name])

        return sum_

    def result_extras(self, node_name: str, scenario_name: str) -> dict[str, Any]:
        results = self.threshold_results.get(node_name, {})
        if results:
            return {"threshold_results": results}
        else:
            return {}
