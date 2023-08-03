from dataclasses import dataclass, field
from typing import Optional, Union, TYPE_CHECKING, Any

from bw2data.backends import Activity
from numpy import ndarray
from pint import DimensionalityError, Quantity, UndefinedUnitError
from pint.facets.plain import PlainQuantity

from enbios2.base.stacked_MultiLCA import StackedMultiLCA
from enbios2.base.unit_registry import ureg
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

    def prepare_tree(self, include_bw_activity: bool = False):
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
            if include_bw_activity:
                activity_node.data.bw_activity = bw_activity
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
                if child.data:
                    for key, value in child.data.results.items():
                        if node.data.results.get(key) is None:
                            node.data.results[key] = 0
                        node.data.results[key] += value

        if not self.result_tree:
            raise ValueError(f"Scenario '{self.alias}' has no results...")
        activities_simple_ids = list(self.activities_outputs.keys())

        methods_aliases: list[str] = list(self._get_methods().keys())
        for result_index, simple_id in enumerate(activities_simple_ids):
            alias = simple_id.alias
            activity_node = self.result_tree.find_child_by_name(alias)
            assert activity_node
            # bw_activity = self.experiment.get_activity(alias).bw_activity
            # activity_node = next(
            #     filter(lambda node: node.temp_data()["activity"].bw_activity == bw_activity, activity_nodes))
            for method_index, method in enumerate(methods_aliases):
                activity_node.data.results[method] = results[result_index][method_index]

        self.result_tree.recursive_apply(recursive_resolve_node, depth_first=True)
        return self.result_tree

    def _get_methods(self) -> dict[str, ExperimentMethodPrepData]:
        if self.methods:
            return self.methods
        else:
            return self.experiment.methods

    @staticmethod
    def _recursive_resolve_outputs(node: BasicTreeNode[ScenarioResultNodeData], _: Optional[Any] = None):
        if node.is_leaf:
            return
        node_output: Optional[Union[Quantity, PlainQuantity]] = None
        for child in node.children:
            activity_output = child.data.output[0]
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
            node.data = ScenarioResultNodeData(output=(str(node_output.units), node_output.magnitude))
        else:
            logger.warning(f"No output for node: {node.name}")

    def run(self) -> BasicTreeNode[ScenarioResultNodeData]:
        if not self._get_methods():
            raise ValueError(f"Scenario '{self.alias}' has no methods")
        logger.info(f"Running scenario '{self.alias}'")
        bw_calc_setup = self._create_bw_calculation_setup()
        results: ndarray = StackedMultiLCA(bw_calc_setup).results
        result_tree = self.set_results(results)
        return result_tree

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
                result["unit"] = data.output[0]
                result['amount'] = data.output[1]
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
                       include_method_units: bool = False):
        """
        Save the results (as tree) to a csv file
         :param file_path:  path to save the results to
         :param level_names: names of the levels to include in the csv (must not match length of levels)
         :param include_method_units:
        """
        if not self.result_tree:
            raise ValueError(f"Scenario '{self.alias}' has no results")

        self.result_tree.to_csv(file_path,
                                include_data=True,
                                level_names=level_names,
                                data_serializer=self.wrapper_data_serializer(include_method_units))

    def result_to_dict(self):

        def data_serializer(data: ScenarioResultNodeData) -> dict:
            result: dict[str, Any] = {
                "output": {
                    "unit": data.output[0],
                    'amount': data.output[1]
                },
                "results": data.results
            }
            if data.bw_activity:
                result["bw_activity"] = data.bw_activity["code"]

            return result

        def recursive_transform(node: BasicTreeNode[ScenarioResultNodeData]) -> dict:
            result: dict[str, Any] = {"alias": node.name,
                                      **data_serializer(node.data)}
            if node.children:
                result["children"] = [recursive_transform(child) for child in node.children]
            return result

        return recursive_transform(self.result_tree.copy())
