from typing import Optional, Any, Union, Tuple

from pint import Quantity, UndefinedUnitError, DimensionalityError
from pint.facets.plain import PlainQuantity

from enbios.base.adapters_aggregators.aggregator import EnbiosAggregator
from enbios.base.unit_registry import ureg
from enbios.generic.enbios2_logging import get_logger
from enbios.generic.tree.basic_tree import BasicTreeNode
from enbios.models.experiment_base_models import NodeOutput
from enbios.models.experiment_models import ScenarioResultNodeData, ResultValue


class SumAggregator(EnbiosAggregator):
    def __init__(self):
        self.logger = get_logger(__name__)

    def validate_config(self, config: Optional[dict[str, Any]]):
        pass

    def validate_node(self, node_name: str, node_config: Any):
        pass

    def aggregate_node_output(
        self,
        node: BasicTreeNode[ScenarioResultNodeData],
        scenario_name: Optional[str] = "",
    ) -> list[NodeOutput]:
        node_outputs: list[tuple[NodeOutput, Quantity]] = []

        def find_node_output_index(given_output: NodeOutput) -> Optional[int]:
            for idx, out_q in enumerate(node_outputs):
                out, quant = out_q
                if given_output.label and out.label == given_output.label:
                        return idx
                else:
                    if not out.label and ureg(out.unit).units.is_compatible_with(ureg(given_output.unit).units):
                        return idx
            return None

        for child in node.children:
            for output in child.data.output:
                assign_to: Optional[int] = find_node_output_index(output)
                if assign_to:
                    node_outputs[assign_to][1] += ureg(output.unit) * output.magnitude
                else:
                    node_outputs.append((output.model_copy(),ureg.parse_expression(output.unit) * output.magnitude))

        return [n[0] for n in node_outputs]


    def aggregate_node_result(
        self, node: BasicTreeNode[ScenarioResultNodeData]
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
