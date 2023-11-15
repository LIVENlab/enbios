import math
import time
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Optional, Union, TYPE_CHECKING, Any

from pint import DimensionalityError, Quantity, UndefinedUnitError
from pint.facets.plain import PlainQuantity

from enbios.base.unit_registry import ureg
from enbios.generic.enbios2_logging import get_logger
from enbios.generic.files import PathLike

# for type hinting
if TYPE_CHECKING:
    from enbios.base.experiment import Experiment
from enbios.generic.tree.basic_tree import BasicTreeNode
from enbios.models.experiment_models import (
    ScenarioResultNodeData,
    ExperimentMethodPrepData,
    Activity_Outputs,
)

logger = get_logger(__name__)


@dataclass
class Scenario:
    experiment: "Experiment"
    alias: str
    result_tree: BasicTreeNode[ScenarioResultNodeData]

    _has_run: bool = False
    # this should be a simpler type - just str: float
    activities_outputs: Activity_Outputs = field(default_factory=dict)
    methods: Optional[dict[str, ExperimentMethodPrepData]] = None
    _execution_time: float = float("NaN")

    def prepare_tree(self):
        """Prepare the result tree for calculating scenario outputs.
        This populates the result tree with ScenarioResultNodeData objects
        for each activity node, which store the output amount and units.
        If config is set, it also stores the BW activity dict with the node.
        """

        activities_aliases = list(self.activities_outputs.keys())
        for result_index, activity_alias in enumerate(activities_aliases):
            try:
                activity_node = self.result_tree.find_subnode_by_name(activity_alias)
            except StopIteration:
                raise ValueError(f"Activity {activity_alias} not found in result tree")
            activity_node._data = ScenarioResultNodeData(
                output=(
                    self.experiment.get_activity_unit(activity_alias),
                    self.activities_outputs[activity_alias],
                )
            )
            # todo adapter/aggregator specific additional data
            # if self.experiment.config.include_bw_activity_in_nodes:
            #     activity_node.data.bw_activity = bw_activity
        self.result_tree.recursive_apply(
            Scenario._recursive_resolve_outputs,
            depth_first=True,
            scenario=self,
            cancel_parents_of=set(),
        )

    @staticmethod
    def _propagate_results_upwards(node: BasicTreeNode[ScenarioResultNodeData]):
        if node.is_leaf:
            return
        else:
            pass

    def _get_methods(self) -> dict[str, ExperimentMethodPrepData]:
        if self.methods:
            return self.methods
        else:
            return self.experiment.methods

    @staticmethod
    def _recursive_resolve_outputs(
        node: BasicTreeNode[ScenarioResultNodeData], _: Optional[Any] = None, **kwargs
    ):
        # todo, does this takes default values when an activity is not defined
        #  in the scenario?
        scenario: Scenario = kwargs["scenario"]
        cancel_parts_of: set = kwargs["cancel_parents_of"]
        if node.is_leaf:
            return
        node_output: Optional[Union[Quantity, PlainQuantity]] = None
        if any(child.id in cancel_parts_of for child in node.children):
            node.set_data(ScenarioResultNodeData())
            cancel_parts_of.add(node.id)
            return
        for child in node.children:
            # if not child._data:
            #     raise ValueError(f"Node {child.name} has no data")
            activity_output = child.data.output[0]
            if activity_output is None:
                node_output = None
                logger.warning(f"No output unit of activity '{child.name}'.")
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
                    f"Cannot parse output unit '{activity_output}' of activity "
                    f"{child.name}. {err}. "
                    f"Consider the unit definition to 'enbios2/base/unit_registry.py'"
                )
                node_output = None
                break
            except DimensionalityError as err:
                set_base_unit = node_output.to_base_units() if node_output else ""
                logger.warning(
                    f"Cannot aggregate output to parent: {node.name}. "
                    f"From earlier children the base unit is {set_base_unit} "
                    f"and from {child.name} it is {output}."
                    f" {err}"
                )
                node_output = None
                break
        if node_output:
            node_output = node_output.to_compact()
            node.set_data(
                ScenarioResultNodeData(
                    output=(str(node_output.units), node_output.magnitude)
                )
            )
        else:
            node.set_data(ScenarioResultNodeData())
            logger.warning(
                f"Scenario: '{scenario.alias}': No output for node: '{node.name}' "
                f"(lvl: {node.level}). "
                f"Not calculating any upper nodes."
            )
            cancel_parts_of.add(node.id)

    def run(self) -> BasicTreeNode[ScenarioResultNodeData]:
        if not self._get_methods():
            raise ValueError(f"Scenario '{self.alias}' has no methods")
        logger.info(f"Running scenario '{self.alias}'")
        # distributions_config = self.experiment.config.use_k_bw_distributions
        # distribution_results = distributions_config > 1
        start_time = time.time()

        for adapter in self.experiment.adapters:
            adapter.prepare_scenario(self)

        result_tree = {}
        for adapter in self.experiment.adapters:
            result_data = adapter.run_scenario(self)
            self.set_results(result_data)

            self.result_tree.recursive_apply(
                Scenario._propagate_results_upwards,
                depth_first=True
            )

        self._has_run = True
        self._execution_time = time.time() - start_time
        return result_tree

    @property
    def execution_time(self) -> str:
        if not math.isnan(self._execution_time):
            return str(timedelta(seconds=int(self._execution_time)))
        else:
            return "not run"

    def reset_execution_time(self):
        self._execution_time = float("NaN")

    def set_results(self, result_data: dict[str, Any]):
        for activity, activity_result in result_data.items():
            activity_node = self.result_tree.find_subnode_by_name(activity)
            activity_node.data.results = activity_result

    def wrapper_data_serializer(self, include_method_units: bool = False):
        method_alias2units: dict[str, str] = {
            method_alias: method_info.bw_method_unit
            for method_alias, method_info in self.experiment.methods.items()
        }

        def data_serializer(data: ScenarioResultNodeData) -> dict:
            result: dict[str, Union[str, float]] = {}
            if data.output:
                result["unit"] = data.output[0] or ""
                result["amount"] = data.output[1] or ""
            if not include_method_units:
                return result | data.results
            else:
                for method_alias, value in data.results.items():
                    final_name = (
                        f"{method_alias} ({method_alias2units[str(method_alias)]})"
                    )
                    result[final_name] = value
                return result

        return data_serializer

    def results_to_csv(
        self,
        file_path: PathLike,
        level_names: Optional[list[str]] = None,
        include_method_units: bool = False,
        warn_no_results: bool = True,
        alternative_hierarchy: BasicTreeNode[ScenarioResultNodeData] = None,
    ):
        """
        Save the results (as tree) to a csv file
         :param warn_no_results:
         :param file_path:  path to save the results to
         :param level_names: names of the levels to include in the csv
         (must not match length of levels)
         :param alternative_hierarchy: An alternative hierarchy to use for the results,
          which comes from Scenario.rearrange_results.
         :param include_method_units:
        """
        if not self.result_tree:
            raise ValueError(f"Scenario '{self.alias}' has no results")

        if warn_no_results and not self._has_run:
            logger.warning(f"Scenario '{self.alias}' has not been run yet")

        use_tree = self.result_tree
        if alternative_hierarchy:
            use_tree = alternative_hierarchy

        use_tree.to_csv(
            file_path,
            include_data=True,
            level_names=level_names,
            data_serializer=self.wrapper_data_serializer(include_method_units),
        )

    def result_to_dict(
        self,
        include_output: bool = True,
        warn_no_results: bool = True,
        alternative_hierarchy: BasicTreeNode[ScenarioResultNodeData] = None,
    ) -> dict[str, Any]:
        """
        Return the results as a dictionary
        :param include_output:
        :param warn_no_results:
        :param alternative_hierarchy: An alternative hierarchy to use for the results,
        which comes from Scenario.rearrange_results.
        :return:
        """

        def data_serializer(data: ScenarioResultNodeData) -> dict:
            result: dict[str, Any] = {"results": data.results}
            if include_output:
                result["output"] = {"unit": data.output[0], "amount": data.output[1]}
            # todo: adapter specific additional data
            # if data.bw_activity:
            #     result["bw_activity"] = data.bw_activity["code"]

            return result

        def recursive_transform(node: BasicTreeNode[ScenarioResultNodeData]) -> dict:
            result: dict[str, Any] = {"alias": node.name, **data_serializer(node.data)}
            if node.children:
                result["children"] = [
                    recursive_transform(child) for child in node.children
                ]
            return result

        if warn_no_results and not self._has_run:
            logger.warning(f"Scenario '{self.alias}' has not been run yet")
        if alternative_hierarchy:
            return recursive_transform(alternative_hierarchy.copy())
        else:
            return recursive_transform(self.result_tree.copy())

    def rearrange_results(
        self, hierarchy: Union[list, dict]
    ) -> BasicTreeNode[ScenarioResultNodeData]:
        alt_result_tree = self.experiment.validate_hierarchy(hierarchy)

        activity_nodes = self.result_tree.iter_leaves()
        alt_activity_nodes = list(alt_result_tree.iter_leaves())
        for node in activity_nodes:
            try:
                alt_node = next(filter(lambda n: n.name == node.name, alt_activity_nodes))
                alt_node._data = node.data
            except StopIteration:
                raise ValueError(
                    f"Activity '{node.name}' not found in alternative hierarchy"
                )
        alt_result_tree.recursive_apply(
            Scenario._recursive_resolve_outputs,
            depth_first=True,
            scenario=self,
            cancel_parents_of=set(),
        )

        alt_result_tree.recursive_apply(
            Scenario._propagate_results_upwards, depth_first=True
        )
        return alt_result_tree

    def get_execution_time(self) -> float:
        return self._execution_time

    def __repr__(self):
        return f"<Scenario '{self.alias}'>"
