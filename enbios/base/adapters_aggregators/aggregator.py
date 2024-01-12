from abc import ABC, abstractmethod
from typing import Optional, Union

from pint import Quantity, UndefinedUnitError, DimensionalityError
from pint.facets.plain import PlainQuantity

import enbios
from enbios.generic.enbios2_logging import get_logger
from enbios.generic.tree.basic_tree import BasicTreeNode
from enbios.models.experiment_base_models import ActivityOutput
from enbios.models.experiment_models import ScenarioResultNodeData, ResultValue


class EnbiosAggregator(ABC):
    @abstractmethod
    def validate_config(self):
        pass

    @abstractmethod
    def validate_node_output(
        self,
        node: BasicTreeNode[ScenarioResultNodeData],
        scenario_name: Optional[str] = "",
    ) -> bool:
        pass

    @abstractmethod
    def aggregate_results(self, node: BasicTreeNode[ScenarioResultNodeData]):
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


class SumAggregator(EnbiosAggregator):
    def __init__(self):
        self.logger = get_logger(__name__)

    def validate_config(self):
        pass

    def validate_node_output(
        self,
        node: BasicTreeNode[ScenarioResultNodeData],
        scenario_name: Optional[str] = "",
    ) -> bool:
        node_output: Optional[Union[Quantity, PlainQuantity]] = None
        for child in node.children:
            # if not child._data:
            #     raise ValueError(f"Node {child.name} has no data")
            activity_output = child.data.output.unit
            if activity_output is None:
                node_output = None
                self.logger.warning(f"No output unit of activity '{child.name}'.")
                break
            output_mag: Optional[float] = None
            try:
                output_mag = (
                    enbios.get_enbios_ureg().parse_expression(activity_output)
                    * child.data.output.magnitude
                )
                if not node_output:
                    node_output = output_mag
                else:
                    node_output += output_mag
            except UndefinedUnitError as err:
                self.logger.error(
                    f"Cannot parse output unit '{activity_output}' of activity "
                    f"{child.name}. {err}. "
                    f"Consider the unit definition to 'enbios2/base/unit_registry.py'"
                )
                node_output = None
                break
            except DimensionalityError as err:
                set_base_unit = node_output.to_base_units() if node_output else ""
                self.logger.warning(
                    f"Cannot aggregate output to parent: {node.name}. "
                    f"From earlier children the base unit is {set_base_unit} "
                    f"and from {child.name} it is {output_mag}."
                    f" {err}"
                )
                node_output = None
                break
        if node_output is not None:
            node_output = node_output.to_compact()
            node.data.output = ActivityOutput(
                unit=str(node_output.units), magnitude=node_output.magnitude
            )
            return True
        else:
            # node.set_data(TechTreeNodeData())
            self.logger.warning(
                f"Scenario: '{scenario_name}': No output for node: '{node.name}' "
                f"(lvl: {node.level}). "
                f"Not calculating any upper nodes."
            )
            return False

    def aggregate_results(self, node: BasicTreeNode[ScenarioResultNodeData]):
        for child in node.children:
            for key, value in child.data.results.items():
                ignore_short_multi_mag = False
                if node.data.results.get(key) is None:
                    node.data.results[key] = ResultValue(
                        magnitude=0, unit=value.unit, multi_magnitude=[]
                    )
                    ignore_short_multi_mag = True
                node_agg_result = node.data.results[key]
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

                node.data.results[key].multi_magnitude = [
                    a + b
                    for a, b in zip(
                        node_agg_result.multi_magnitude, value.multi_magnitude
                    )
                ]

    @staticmethod
    def node_indicator() -> str:
        return "sum"

    @staticmethod
    def name() -> str:
        return "sum-aggregator"

    @staticmethod
    def get_config_schemas() -> dict:
        return {}
