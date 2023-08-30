import math
import time
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Optional, Union, TYPE_CHECKING, Any

from bw2data.backends import Activity
from numpy import ndarray
from pint import DimensionalityError, Quantity, UndefinedUnitError
from pint.facets.plain import PlainQuantity

from enbios2.base.stacked_MultiLCA import StackedMultiLCA
from enbios2.base.unit_registry import ureg
from enbios2.bw2.util import bw_unit_fix
from enbios2.generic.enbios2_logging import get_logger
from enbios2.generic.files import PathLike

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
    results: Optional[ndarray] = None
    _has_run: bool = False
    # this should be a simpler type - just str: float
    activities_outputs: Activity_Outputs = field(default_factory=dict)
    methods: Optional[dict[str, ExperimentMethodPrepData]] = None
    _execution_time: float = float('NaN')

    def prepare_tree(self):
        # activity_nodes = list(self.result_tree.get_leaves())
        activities_simple_ids = list(self.activities_outputs.keys())
        for result_index, simple_id in enumerate(activities_simple_ids):
            alias = simple_id.alias
            bw_activity = self.experiment.get_activity(alias).bw_activity
            try:
                activity_node = self.result_tree.find_subnode_by_name(alias)
            except StopIteration:
                raise ValueError(f"Activity {alias} not found in result tree")
            activity_node._data = ScenarioResultNodeData(output=(bw_unit_fix(bw_activity['unit']),
                                                                 self.activities_outputs[simple_id]))
            if self.experiment.config.include_bw_activity_in_nodes:
                activity_node._data.bw_activity = bw_activity
        self.result_tree.recursive_apply(Scenario._recursive_resolve_outputs, depth_first=True)

    def _create_bw_calculation_setup(self, register: bool = True) -> BWCalculationSetup:
        inventory: list[dict[Activity, float]] = []
        for activity_alias, act_out in self.activities_outputs.items():
            bw_activity = self.experiment.get_activity(activity_alias.alias).bw_activity
            inventory.append({bw_activity: act_out})

        methods = [m.id for m in self._get_methods().values()]
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
                if child._data:
                    for key, value in child._data.results.items():
                        assert node._data
                        if node._data.results.get(key) is None:
                            node._data.results[key] = 0
                        node._data.results[key] += value

        if not self.result_tree:
            raise ValueError(f"Scenario '{self.alias}' has no results...")
        activities_simple_ids = list(self.activities_outputs.keys())

        methods_aliases: list[str] = list(self._get_methods().keys())
        for result_index, simple_id in enumerate(activities_simple_ids):
            alias = simple_id.alias
            activity_node = self.result_tree.find_subnode_by_name(alias)
            assert activity_node
            # bw_activity = self.experiment.get_activity(alias).bw_activity
            # activity_node = next(
            #     filter(lambda node: node.temp_data()["activity"].bw_activity == bw_activity, activity_nodes))
            for method_index, method in enumerate(methods_aliases):
                activity_node._data.results[method] = results[result_index][method_index]

        self.result_tree.recursive_apply(recursive_resolve_node, depth_first=True)
        return self.result_tree

    def _get_methods(self) -> dict[str, ExperimentMethodPrepData]:
        if self.methods:
            return self.methods
        else:
            return self.experiment.methods

    @staticmethod
    def _recursive_resolve_outputs(node: BasicTreeNode[ScenarioResultNodeData], _: Optional[Any] = None):
        # todo, does this takes default values when an activity is not defined in the scenario?
        if node.is_leaf:
            return
        node_output: Optional[Union[Quantity, PlainQuantity]] = None
        for child in node.children:
            # todo, this should be removable
            if not child._data:
                raise ValueError(f"Node {child.name} has no data")
            activity_output = child.data.output[0]
            if activity_output is None:
                node_output = None
                logger.warning(
                    f"No output unit of activity '{child.name}'.")
                break
            output = None
            try:
                output = ureg.parse_expression(activity_output) * child.data.output[1]
                if not node_output:
                    node_output = output
                else:
                    node_output += output
            except UndefinedUnitError as err:
                logger.error(
                    f"Cannot parse output unit '{activity_output}' of activity {child.name}. {err}. "
                    f"Consider the unit definition to 'enbios2/base/unit_registry.py'")
                node_output = None
                break
            except DimensionalityError as err:
                set_base_unit = node_output.to_base_units() if node_output else ""
                logger.warning(f"Cannot aggregate output to parent: {node.name}. "
                               f"From earlier children the base unit is {set_base_unit} "
                               f"and from {child.name} it is {output}."
                               f" {err}")
                node_output = None
                break
        if node_output:
            node_output = node_output.to_compact()
            node.set_data(ScenarioResultNodeData(output=(str(node_output.units), node_output.magnitude)))
        else:
            node.set_data(ScenarioResultNodeData())
            logger.warning(f"No output for node: {node.name}")

    def run(self) -> BasicTreeNode[ScenarioResultNodeData]:
        if not self._get_methods():
            raise ValueError(f"Scenario '{self.alias}' has no methods")
        logger.info(f"Running scenario '{self.alias}'")
        start_time = time.time()
        bw_calc_setup = self._create_bw_calculation_setup()
        results: ndarray = StackedMultiLCA(bw_calc_setup).results
        result_tree = self.set_results(results)
        self._execution_time = time.time() - start_time
        return result_tree

    @property
    def execution_time(self) -> str:
        if not math.isnan(self._execution_time):
            return str(timedelta(seconds=int(self._execution_time)))
        else:
            return "not run"

    def reset_execution_time(self):
        self._execution_time = float('NaN')

    def set_results(self, results: ndarray) -> BasicTreeNode[ScenarioResultNodeData]:
        if self.experiment.config.store_raw_results:
            self.results = results
        self.result_tree = self.create_results_to_technology_tree(results)
        self._has_run = True
        return self.result_tree

    def wrapper_data_serializer(self, include_method_units: bool = False):

        method_alias2units: dict[str, str] = {
            method_alias: method_info.bw_method.unit
            for method_alias, method_info in self.experiment.methods.items()
        }

        def data_serializer(data: ScenarioResultNodeData) -> dict:
            result: dict[str, Union[str, float]] = {}
            if data.output:
                result["unit"] = data.output[0] or ""
                result['amount'] = data.output[1] or ""
            if not include_method_units:
                return result | data.results
            else:
                for method_alias, value in data.results.items():
                    final_name = f"{method_alias} ({method_alias2units[str(method_alias)]})"
                    result[final_name] = value
                return result

        return data_serializer

    def results_to_csv(self,
                       file_path: PathLike,
                       level_names: Optional[list[str]] = None,
                       include_method_units: bool = False,
                       warn_no_results: bool = True):
        """
        Save the results (as tree) to a csv file
         :param file_path:  path to save the results to
         :param level_names: names of the levels to include in the csv (must not match length of levels)
         :param include_method_units:
        """
        if not self.result_tree:
            raise ValueError(f"Scenario '{self.alias}' has no results")

        if warn_no_results and not self._has_run:
            logger.warning(f"Scenario '{self.alias}' has not been run yet")

        self.result_tree.to_csv(file_path,
                                include_data=True,
                                level_names=level_names,
                                data_serializer=self.wrapper_data_serializer(include_method_units))

    def result_to_dict(self, include_output: bool = True, warn_no_results: bool = True) -> dict[str, Any]:

        def data_serializer(data: ScenarioResultNodeData) -> dict:
            result: dict[str, Any] = {
                "results": data.results
            }
            if include_output:
                result["output"] = {
                    "unit": data.output[0],
                    'amount': data.output[1]
                }
            if data.bw_activity:
                result["bw_activity"] = data.bw_activity["code"]

            return result

        def recursive_transform(node: BasicTreeNode[ScenarioResultNodeData]) -> dict:
            result: dict[str, Any] = {"alias": node.name,
                                      **data_serializer(node._data)}
            if node.children:
                result["children"] = [recursive_transform(child) for child in node.children]
            return result

        if warn_no_results and not self._has_run:
            logger.warning(f"Scenario '{self.alias}' has not been run yet")
        return recursive_transform(self.result_tree.copy())
