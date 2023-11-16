from abc import ABC, abstractmethod
from typing import Optional, Union

from pint import Quantity, UndefinedUnitError, DimensionalityError
from pint.facets.plain import PlainQuantity

import enbios
from enbios.generic.enbios2_logging import get_logger
from enbios.generic.tree.basic_tree import BasicTreeNode
from enbios.models.experiment_models import ScenarioResultNodeData


class EnbiosAggregator(ABC):

    @abstractmethod
    def validate_config(self):
        pass

    @abstractmethod
    def validate_node_output(self, node: BasicTreeNode[ScenarioResultNodeData],
                             scenario_name: Optional[str] = "") -> bool:
        pass

    @abstractmethod
    def aggregate_results(self, node: BasicTreeNode[ScenarioResultNodeData]):
        pass

    @property
    @abstractmethod
    def node_indicator(self) -> str:
        pass


class SumAggregator(EnbiosAggregator):

    def __init__(self):
        self.logger = get_logger(__name__)

    def validate_config(self):
        pass

    def validate_node_output(self, node: BasicTreeNode[ScenarioResultNodeData], scenario_name: Optional[str] = "") -> bool:
        node_output: Optional[Union[Quantity, PlainQuantity]] = None
        for child in node.children:
            # if not child._data:
            #     raise ValueError(f"Node {child.name} has no data")
            activity_output = child.data.output[0]
            if activity_output is None:
                node_output = None
                self.logger.warning(f"No output unit of activity '{child.name}'.")
                break
            output = None
            try:
                output = enbios.get_enbios_ureg().parse_expression(activity_output) * child.data.output[1]
                if not node_output:
                    node_output = output
                else:
                    node_output += output
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
                    f"and from {child.name} it is {output}."
                    f" {err}"
                )
                node_output = None
                break
        if node_output is not None:
            node_output = node_output.to_compact()
            node.data.output = (str(node_output.units), node_output.magnitude)
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
                if node.data.results.get(key) is None:
                    node.data.results[key] = 0
                node.data.results[key] += value
        # if child.data:
        #         if add_to_distribution:
        #             for key, value in child.data.distribution_results.items():
        #                 if node.data.distribution_results.get(key) is None:
        #                     num_distribution = len(
        #                         list(child.data.distribution_results.values())[0]
        #                     )
        #                     node.data.distribution_results[key] = [0] * num_distribution
        #                 node.data.distribution_results[key] = list(
        #                     [
        #                         a + b
        #                         for a, b in zip(
        #                         node.data.distribution_results[key],
        #                         child.data.distribution_results[key],
        #                     )
        #                     ]
        #                 )
        #         else:
        #             for key, value in child.data.results.items():
        #                 if node.data.results.get(key) is None:
        #                     node.data.results[key] = 0
        #                 node.data.results[key] += value

    @property
    def node_indicator(self) -> str:
        return "sum"
