import json
import math
import time
from dataclasses import dataclass, field, asdict
from datetime import timedelta
from typing import Optional, Union, TYPE_CHECKING, Any

from enbios.generic.enbios2_logging import get_logger
from enbios.generic.files import PathLike

# for type hinting
if TYPE_CHECKING:
    from enbios.base.experiment import Experiment
from enbios.generic.tree.basic_tree import BasicTreeNode
from enbios.models.experiment_models import (
    ScenarioResultNodeData,
    Activity_Outputs, ScenarioConfig,
)

logger = get_logger(__name__)


@dataclass
class Scenario:
    experiment: "Experiment"
    name: str
    result_tree: BasicTreeNode[ScenarioResultNodeData]

    _has_run: bool = False
    # this should be a simpler type - just str: float
    activities_outputs: Activity_Outputs = field(default_factory=dict)
    # methods: Optional[dict[str, ExperimentMethodPrepData]] = None
    _execution_time: float = float("NaN")
    config: ScenarioConfig = field(default_factory=ScenarioConfig)

    def prepare_tree(self):
        """Prepare the result tree for calculating scenario outputs.
        This populates the result tree with ScenarioResultNodeData objects
        for each activity node, which store the output amount and units.
        If config is set, it also stores the BW activity dict with the node.
        """

        activities_names = list(self.activities_outputs.keys())
        for result_index, activity_name in enumerate(activities_names):
            try:
                activity_node = self.result_tree.find_subnode_by_name(activity_name)
            except StopIteration:
                raise ValueError(f"Activity {activity_name} not found in result tree")
            activity_node.data.output = (
                self.experiment.get_activity_output_unit(activity_name),
                self.activities_outputs[activity_name],
            )
            # todo adapter/aggregator specific additional data
            # if self.experiment.config.include_bw_activity_in_nodes:
            #     activity_node.data.bw_activity = bw_activity

        if self.config.exclude_defaults:
            def remove_empty_nodes(node: BasicTreeNode[ScenarioResultNodeData], cancel_parents_of: set[str]):
                # aggregators without children are not needed
                if node.is_leaf and node.data.aggregator:
                    node.remove_self()

            for leave in self.result_tree.iter_leaves():
                if leave.name not in activities_names:
                    leave.remove_self()
            self.result_tree.recursive_apply(remove_empty_nodes, depth_first=True, cancel_parents_of=set())

        self.result_tree.recursive_apply(
            self.experiment.recursive_resolve_outputs,
            experiment=self.experiment,
            depth_first=True,
            cancel_parents_of=set(),
        )

    @staticmethod
    def _propagate_results_upwards(
            node: BasicTreeNode[ScenarioResultNodeData], experiment: "Experiment"
    ):
        if node.is_leaf:
            return
        else:
            experiment.get_node_aggregator(node.data.aggregator).aggregate_results(node)

    # def _get_methods(self) -> dict[str, ExperimentMethodPrepData]:
    #     if self.methods:
    #         return self.methods
    #     else:
    #         return self.experiment.methods

    def run(self, results_as_dict: bool = True) -> Union[BasicTreeNode[ScenarioResultNodeData], dict]:
        # if not self._get_methods():
        #     raise ValueError(f"Scenario '{self.name}' has no methods")
        logger.info(f"Running scenario '{self.name}'")
        # distributions_config = self.experiment.config.use_k_bw_distributions
        # distribution_results = distributions_config > 1
        start_time = time.time()

        for adapter in self.experiment.adapters:
            result_data = adapter.run_scenario(self)
            self.set_results(result_data)

        self.result_tree.recursive_apply(
            Scenario._propagate_results_upwards,
            experiment=self.experiment,
            depth_first=True,
        )

        self._has_run = True
        self._execution_time = time.time() - start_time
        return self.result_to_dict() if results_as_dict else self.result_tree

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
            if not activity_node:
                if self.config.exclude_defaults:
                    logger.warning(
                        f"Activity '{activity}' not found in result tree. "
                        f"Make sure that the adapter for does not generate results for default"
                        f"activities that are not in the scenario.")
                else:
                    logger.error(f"Activity '{activity}' not found in result tree")
                continue
            activity_node.data.results = activity_result

    def wrapper_data_serializer(self, include_method_units: bool = True):
        # todo: use this for json as well...
        method_name2units: dict[str, str] = {
            method_name: self.experiment.get_method_unit(method_name)
            for method_name in self.experiment.methods
        }

        def data_serializer(data: ScenarioResultNodeData) -> dict:
            result: dict[str, Union[str, float]] = {}
            if data.output:
                result["unit"] = data.output[0] or ""
                result["amount"] = data.output[1] or ""
            if not include_method_units:
                return result | data.results
            else:
                for method_name, value in data.results.items():
                    final_name = f"{method_name} ({method_name2units[str(method_name)]})"
                    result[final_name] = value
                return result

        return data_serializer

    def results_to_csv(
            self,
            file_path: PathLike,
            level_names: Optional[list[str]] = None,
            include_method_units: bool = True,
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
            raise ValueError(f"Scenario '{self.name}' has no results")

        if warn_no_results and not self._has_run:
            logger.warning(f"Scenario '{self.name}' has not been run yet")

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
            result: dict[str, Any] = {
                "results": {
                    method_name: asdict(result_value)
                    for method_name, result_value in data.results.items()
                }
            }
            if include_output:
                result["output"] = {"unit": data.output[0], "amount": data.output[1]}
            # todo: adapter specific additional data
            # if data.bw_activity:
            #     result["bw_activity"] = data.bw_activity["code"]

            return result

        def recursive_transform(node: BasicTreeNode[ScenarioResultNodeData]) -> dict:
            result: dict[str, Any] = {"name": node.name, **data_serializer(node.data)}
            if node.children:
                result["children"] = [
                    recursive_transform(child) for child in node.children
                ]
            return result

        if warn_no_results and not self._has_run:
            logger.warning(f"Scenario '{self.name}' has not been run yet")
        if alternative_hierarchy:
            return recursive_transform(alternative_hierarchy.copy())
        else:
            return recursive_transform(self.result_tree.copy())

    # def rearrange_results(
    #     self, hierarchy: Union[list, dict]
    # ) -> BasicTreeNode[ScenarioResultNodeData]:
    #     alt_result_tree = self.experiment.validate_hierarchy(hierarchy)
    #
    #     activity_nodes = self.result_tree.iter_leaves()
    #     alt_activity_nodes = list(alt_result_tree.iter_leaves())
    #     for node in activity_nodes:
    #         try:
    #             alt_node = next(filter(lambda n: n.name == node.name, alt_activity_nodes))
    #             alt_node._data = node.data
    #         except StopIteration:
    #             raise ValueError(
    #                 f"Activity '{node.name}' not found in alternative hierarchy"
    #             )
    #     alt_result_tree.recursive_apply(
    #         Scenario._recursive_resolve_outputs,
    #         depth_first=True,
    #         scenario=self,
    #         cancel_parents_of=set(),
    #     )
    #
    #     alt_result_tree.recursive_apply(
    #         Scenario._propagate_results_upwards, depth_first=True
    #     )
    #     return alt_result_tree

    def get_execution_time(self) -> float:
        return self._execution_time

    def __repr__(self):
        return f"<Scenario '{self.name}'>"

    def describe(self):
        output = f"Scenario '{self.name}'\n"

        output += json.dumps(self.activities_outputs, indent=2)
        # todo: the tree instead...

        return output
