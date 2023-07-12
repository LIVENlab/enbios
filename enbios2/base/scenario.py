from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Union, TYPE_CHECKING, Any

from bw2data.backends import Activity
from numpy import ndarray
from pint import DimensionalityError, Quantity
from pint.facets.plain import PlainQuantity

from enbios2.base.stacked_MultiLCA import StackedMultiLCA
from enbios2.base.unit_registry import ureg
from enbios2.generic.enbios2_logging import get_logger

# for type hinting
if TYPE_CHECKING:
    from enbios2.base.experiment import Experiment
from enbios2.generic.tree.basic_tree import BasicTreeNode
from enbios2.models.experiment_models import (BWCalculationSetup,
                                              ScenarioResultNodeData, ExperimentMethodPrepData, Activity_Outputs)

logger = get_logger(__file__)


@dataclass
class Scenario:
    experiment: "Experiment"
    alias: str
    result_tree: BasicTreeNode[ScenarioResultNodeData]
    # this should be a simpler type - just str: float
    # todo not used yet
    orig_outputs: Optional[dict[str, float]] = field(
        default_factory=dict[str, float])  # output declaration before conversion
    activities_outputs: Activity_Outputs = field(default_factory=dict)
    methods: Optional[dict[str, ExperimentMethodPrepData]] = None

    def __post_init__(self):
        self.prepare_tree()

    def prepare_tree(self):
        activity_nodes = self.result_tree.get_leaves()
        activities_simple_ids = list(self.activities_outputs.keys())
        for result_index, simple_id in enumerate(activities_simple_ids):
            alias = simple_id.alias
            bw_activity = self.experiment.get_activity(alias).bw_activity
            activity_node = next(
                filter(lambda node: node.temp_data()["activity"].bw_activity == bw_activity, activity_nodes))
            # todo this does not consider magnitude...
            activity_node.data = ScenarioResultNodeData(output=(bw_activity['unit'].replace(" ", "_"),
                                                                self.activities_outputs[simple_id]))

    def create_bw_calculation_setup(self, register: bool = True) -> BWCalculationSetup:
        inventory: list[dict[Activity, float]] = []
        for activity_alias, act_out in self.activities_outputs.items():
            bw_activity = self.experiment.get_activity(activity_alias.alias).bw_activity
            inventory.append({bw_activity: act_out})

        methods = [m.id for m in self.get_methods().values()]
        calculation_setup = BWCalculationSetup(self.alias, inventory, methods)
        if register:
            calculation_setup.register()
        return calculation_setup

    def create_results_to_technology_tree(self, results: ndarray) -> BasicTreeNode[ScenarioResultNodeData]:
        """
        Add results to the technology tree, for each method
        """

        def recursive_resolve_node(node: BasicTreeNode[ScenarioResultNodeData], _: Any = None):
            for child in node.children:
                if child.data:
                    for key, value in child.data.results.items():
                        if node.data.results.get(key) is None:
                            node.data.results[key] = 0
                        node.data.results[key] += value

        if not self.result_tree:
            raise ValueError(f"Scenario '{self.alias}' has no tree...")
        activity_nodes = list(self.result_tree.get_leaves())
        # todo should be the same set of activities
        activities_simple_ids = list(self.activities_outputs.keys())

        methods_aliases: list[str] = list(self.get_methods().keys())
        for result_index, simple_id in enumerate(activities_simple_ids):
            alias = simple_id.alias
            bw_activity = self.experiment.get_activity(alias).bw_activity
            activity_node = next(
                filter(lambda node: node.temp_data()["activity"].bw_activity == bw_activity, activity_nodes))
            for method_index, method in enumerate(methods_aliases):
                activity_node.data.results[method] = results[result_index][method_index]

        self.result_tree.recursive_apply(recursive_resolve_node, depth_first=True)
        return self.result_tree

    def get_methods(self) -> dict[str, ExperimentMethodPrepData]:
        if self.methods:
            return self.methods
        else:
            return self.experiment.methods

    def run(self) -> BasicTreeNode[ScenarioResultNodeData]:
        if not self.get_methods():
            raise ValueError(f"Scenario '{self.alias}' has no methods")
        logger.info(f"Running scenario '{self.alias}'")
        bw_calc_setup = self.create_bw_calculation_setup()

        def recursive_resolve_outputs(node: BasicTreeNode[ScenarioResultNodeData], _: Optional[Any] = None):
            if node.is_leaf:
                return
            node_output: Optional[Union[Quantity, PlainQuantity]] = None
            for child in node.children:
                output = ureg.parse_expression(child.data.output[0]) * child.data.output[1]
                try:
                    if not node_output:
                        node_output = output
                    else:
                        node_output += output
                except DimensionalityError as err:
                    existing_output = node_output.to_base_units() if node_output else "NOTHING YET"
                    logger.warning(f"Cannot aggregate output to parent: {node.name}. "
                                   f"From earlier children the base unit is {existing_output} "
                                   f"and from {child.name} it is {output.units}."
                                   f" {err}")
            if node_output:
                node_output = node_output.to_compact()
                node.data = ScenarioResultNodeData(output=(str(node_output.units), node_output.magnitude))
            else:
                logger.warning(f"No output for node: {node.name}")

        if not self.experiment.lca:
            results: ndarray = StackedMultiLCA(bw_calc_setup).results
        self.result_tree.recursive_apply(recursive_resolve_outputs, depth_first=True)
        return self.create_results_to_technology_tree(results)

    def results_to_csv(self, file_path: Path, include_method_units: bool = False):
        """
        Save the results (as tree) to a csv file
         :param file_path:  path to save the results to
         :param include_method_units:
        """
        if not self.result_tree:
            raise ValueError(f"Scenario '{self.alias}' has no results")

        def data_serializer(data: ScenarioResultNodeData) -> dict:
            result: dict[str, Union[str, float]] = {}
            if data.output:
                result["unit"] = data.output[0]
                result['amount'] = data.output[1]
            if not include_method_units:
                return result | data.results
            else:
                for method_alias, value in data.results.items():
                    final_name = f"{method_alias} ({self.experiment.methods[str(method_alias)].bw_method.unit})"
                    result[final_name] = value
                return result

        self.result_tree.to_csv(file_path, include_data=True, data_serializer=data_serializer)
