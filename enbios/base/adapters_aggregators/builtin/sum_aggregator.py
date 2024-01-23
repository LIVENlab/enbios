from typing import Optional, Any, Union

from pint import Quantity, UndefinedUnitError, DimensionalityError
from pint.facets.plain import PlainQuantity

import enbios
from enbios.base.adapters_aggregators.aggregator import EnbiosAggregator
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
    ) -> Optional[NodeOutput]:
        node_output: Optional[Union[Quantity, PlainQuantity]] = None
        for child in node.children:
            # if not child._data:
            #     raise ValueError(f"Node {child.name} has no data")
            if not child.data.output:
                continue
            node_output_unit = child.data.output.unit
            if node_output_unit is None:
                node_output = None
                self.logger.warning(f"No output unit of node '{child.name}'.")
                break
            output_mag: Optional[Quantity] = None
            try:
                output_mag = (
                    enbios.get_enbios_ureg().parse_expression(node_output_unit)
                    * child.data.output.magnitude
                )
                if not node_output:
                    node_output = output_mag
                else:
                    node_output += output_mag
            except UndefinedUnitError as err:
                self.logger.error(
                    f"Cannot parse output unit '{node_output_unit}' of node "
                    f"{child.name}. {err}. "
                    f"Consider the unit definition to 'enbios2/base/unit_registry.py'"
                )
                node_output = None
                break
            except DimensionalityError as err:
                set_base_unit = node_output.to_base_units().units if node_output else ""
                self.logger.warning(
                    f"Cannot aggregate output to parent: '{node.name}'. "
                    f"From earlier children the base unit is '{set_base_unit}'"
                    f"and from '{child.name}' it is '{output_mag.units}'."
                    f" {err}"
                )
                node_output = None
                break
        if node_output is not None:
            node_output = node_output.to_compact()
            result_node_output = NodeOutput(
                unit=str(node_output.units), magnitude=node_output.magnitude
            )
            return result_node_output
        else:
            # node.set_data(TechTreeNodeData())
            self.logger.warning(
                f"Scenario: '{scenario_name}': No output for node: '{node.name}' "
                f"(lvl: {node.level}). "
                f"Not calculating any upper nodes."
            )
            return None

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
