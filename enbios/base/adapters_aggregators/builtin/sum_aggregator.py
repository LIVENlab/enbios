from typing import Optional, Any

from enbios.base.adapters_aggregators.aggregator import EnbiosAggregator
from enbios.generic.enbios2_logging import get_logger
from enbios.generic.output_merge import merge_outputs
from enbios.generic.tree.basic_tree import BasicTreeNode
from enbios.models.models import ResultValue, ScenarioResultNodeData, output_merge_type, AggregationModel


class SumAggregator(EnbiosAggregator):

    def __init__(self):
        self.logger = get_logger(__name__)

    def validate_definition(self, definition: AggregationModel):
        pass

    def validate_config(self, config: Optional[dict[str, Any]]):
        pass

    def validate_node(self, node_name: str, node_config: Any):
        pass

    def validate_scenario_node(
            self, node_name: str, scenario_name: str, scenario_node_data: Any
    ):
        pass

    def aggregate_node_output(
            self,
            node: BasicTreeNode[ScenarioResultNodeData],
            scenario_name: Optional[str] = "",
    ) -> output_merge_type:
        return merge_outputs([n.data.output for n in node.children])

    def aggregate_node_result(
            self, node: BasicTreeNode[ScenarioResultNodeData],
            scenario_name: str
    ) -> dict[str, ResultValue]:
        result: dict[str, ResultValue] = {}
        for child in node.children:
            for key, value in child.data.results.items():
                ignore_short_multi_mag = False
                if result.get(key) is None:
                    result[key] = ResultValue(
                        magnitude=0, unit=value.unit, multi_magnitude=[]
                    )
                    ignore_short_multi_mag = True
                node_agg_result = result[key]
                if value.magnitude:
                    node_agg_result.magnitude += value.magnitude
                # multi_magnitude
                max_len = max(
                    len(node_agg_result.multi_magnitude), len(value.multi_magnitude)
                )
                if len(node_agg_result.multi_magnitude) < max_len:
                    node_agg_result.multi_magnitude.extend(
                        [0] * (max_len - len(node_agg_result.multi_magnitude))
                    )
                    if not ignore_short_multi_mag:
                        self.get_logger().warning(
                            f"Multi magnitude of node {node.name} is shorter than child {child.name}."
                        )
                if len(value.multi_magnitude) < max_len:
                    value.multi_magnitude.extend(
                        [0] * (max_len - len(value.multi_magnitude))
                    )
                    self.get_logger().warning(
                        f"Multi magnitude of child {child.name} is shorter than node {node.name}."
                    )

                result[key].multi_magnitude = [
                    a + b
                    for a, b in zip(
                        node_agg_result.multi_magnitude, value.multi_magnitude
                    )
                ]
        return result

    @staticmethod
    def node_indicator() -> str:
        return "sum"

    @staticmethod
    def name() -> str:
        return "sum-aggregator"

    @staticmethod
    def get_config_schemas() -> dict:
        return {}
