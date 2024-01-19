import concurrent.futures
import json
import math
import time
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Optional, Union, TYPE_CHECKING, Any, Callable

from enbios.base.tree_operations import validate_experiment_reference_hierarchy
from enbios.generic.enbios2_logging import get_logger
from enbios.generic.files import PathLike
from enbios.models.experiment_base_models import (
    HierarchyNodeReference,
    ScenarioConfig,
    NodeOutput,
)

# for type hinting
if TYPE_CHECKING:
    from enbios.base.experiment import Experiment
from enbios.generic.tree.basic_tree import BasicTreeNode
from enbios.models.experiment_models import (
    ScenarioResultNodeData,
    ResultValue,
    TechTreeNodeData,
)

logger = get_logger(__name__)


@dataclass
class Scenario:
    experiment: "Experiment"
    name: str
    result_tree: BasicTreeNode[ScenarioResultNodeData]

    _has_run: bool = False
    # this should be a simpler type - just str: float
    structural_nodes_outputs: dict[str, float] = field(default_factory=dict)
    # methods: Optional[dict[str, ExperimentMethodPrepData]] = None
    _execution_time: float = float("NaN")
    config: ScenarioConfig = field(default_factory=ScenarioConfig)

    def prepare_tree(self):
        """Prepare the result tree for calculating scenario outputs.
        This populates the result tree with ScenarioResultNodeData objects
        for each node, which store the output magnitude and units.
        If config is set, it also stores the BW node dict with the node.
        """

        structural_nodes_names = list(self.structural_nodes_outputs.keys())
        for result_index, node_name in enumerate(structural_nodes_names):
            try:
                structural_result_node = self.result_tree.find_subnode_by_name(node_name)
            except StopIteration:
                raise ValueError(f"Node {node_name} not found in result tree")
            structural_node = self.experiment.get_structural_node(node_name)
            structural_result_node.data.output = NodeOutput(
                unit=self.experiment.get_node_adapter(
                    structural_node
                ).get_node_output_unit(node_name),
                magnitude=self.structural_nodes_outputs[node_name],
            )
            # todo adapter/aggregator specific additional data
            # if self.experiment.config.include_bw_activity_in_nodes:
            #     node_node.data.bw_activity = bw_activity

        if self.config.exclude_defaults:

            def remove_empty_nodes(
                node: BasicTreeNode[ScenarioResultNodeData], cancel_parents_of: set[str]
            ):
                # aggregators without children are not needed
                if node.is_leaf and node.data.aggregator:
                    node.remove_self()

            for leave in self.result_tree.iter_leaves():
                if leave.name not in structural_nodes_names:
                    leave.remove_self()
            self.result_tree.recursive_apply(
                remove_empty_nodes, depth_first=True, cancel_parents_of=set()
            )

        from enbios.base.tree_operations import recursive_resolve_outputs

        self.result_tree.recursive_apply(
            recursive_resolve_outputs,
            experiment=self.experiment,
            depth_first=True,
            scenario_name=self.name,
            cancel_parents_of=set(),
        )

    @staticmethod
    def _propagate_results_upwards(
        node: BasicTreeNode[ScenarioResultNodeData], experiment: "Experiment"
    ):
        if node.is_leaf:
            return
        else:
            node.data.results = experiment.get_node_aggregator(
                node
            ).aggregate_node_result(node)

    def run(
        self, results_as_dict: bool = True
    ) -> Union[BasicTreeNode[ScenarioResultNodeData], dict]:
        # if not self._get_methods():
        #     raise ValueError(f"Scenario '{self.name}' has no methods")
        logger.info(f"Running scenario '{self.name}'")
        # distributions_config = self.experiment.config.use_k_bw_distributions
        # distribution_results = distributions_config > 1
        start_time = time.time()

        if self.experiment.config.run_adapters_concurrently:
            # Create a ThreadPoolExecutor
            with concurrent.futures.ThreadPoolExecutor() as executor:
                # Use a list comprehension to start a thread for each adapter
                futures = [
                    executor.submit(adapter.run_scenario, self)
                    for adapter in self.experiment.adapters
                ]
                # As each future completes, set the results
                for future in concurrent.futures.as_completed(futures):
                    result_data = future.result()
                    self.set_results(result_data)
        else:
            for adapter in self.experiment.adapters:
                # run in parallel:
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
        for node_name, node_result in result_data.items():
            node = self.result_tree.find_subnode_by_name(node_name)
            if not node:
                if self.config.exclude_defaults:
                    logger.warning(
                        f"Structural node '{node_name}' not found in result tree. "
                        f"Make sure that the adapter for does not generate results for default"
                        f"nodes that are not in the scenario."
                    )
                else:
                    logger.error(f"Node '{node_name}' not found in result tree")
                continue
            node.data.results = node_result

    @staticmethod
    def wrapper_data_serializer(
        include_output: bool = True, expand_results: bool = False
    ) -> Callable[[ScenarioResultNodeData], dict]:
        def _expand_results(results: dict[str, ResultValue]) -> dict:
            """
            brings all results to the data level (one down) which is useful for csv
            :param results:
            :return:
            """
            expanded_results = {}
            for method_name, result_value in results.items():
                expanded_results[
                    f"{method_name}_magnitude ({result_value.unit})"
                ] = result_value.magnitude
                if result_value.multi_magnitude:
                    for idx, magnitude in enumerate(result_value.multi_magnitude):
                        expanded_results[
                            f"{method_name}_{result_value.unit}_{idx}"
                        ] = magnitude
            return expanded_results

        def data_serializer(data: ScenarioResultNodeData) -> dict:
            if expand_results:
                result: dict[str, Any] = _expand_results(data.results)
            else:
                result: dict[str, Any] = {
                    "results": {
                        method_name: result_value.model_dump(exclude_defaults=True)
                        for method_name, result_value in data.results.items()
                    }
                }
            # there might be no output, when the units dont match
            if include_output and data.output:
                if expand_results:
                    result["output_unit"] = data.output.unit
                    result["output_magnitude"] = data.output.magnitude
                else:
                    result["output"] = data.output.model_dump()
            # todo: adapter specific additional data
            # if data.bw_activity:
            #     result["bw_activity"] = data.bw_activity["code"]

            return result

        return data_serializer

    def results_to_csv(
        self,
        file_path: PathLike,
        level_names: Optional[list[str]] = None,
        include_method_units: bool = True,
        warn_no_results: bool = True,
        alternative_hierarchy: Optional[dict] = None,
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
            use_tree = self._rearrange_results(alternative_hierarchy)

        use_tree.to_csv(
            file_path,
            include_data=True,
            level_names=level_names,
            data_serializer=self.wrapper_data_serializer(include_method_units, True),
        )

    def result_to_dict(
        self,
        include_output: bool = True,
        warn_no_results: bool = True,
        alternative_hierarchy: dict = None,
    ) -> dict[str, Any]:
        """
        Return the results as a dictionary
        :param include_output:
        :param warn_no_results:
        :param alternative_hierarchy: An alternative hierarchy to use for the results,
        which comes from Scenario.rearrange_results.
        :return:
        """

        def recursive_transform(node: BasicTreeNode[ScenarioResultNodeData]) -> dict:
            result: dict[str, Any] = {
                "name": node.name,
                **Scenario.wrapper_data_serializer(include_output)(node.data),
            }
            if node.children:
                result["children"] = [
                    recursive_transform(child) for child in node.children
                ]
            return result

        if warn_no_results and not self._has_run:
            logger.warning(f"Scenario '{self.name}' has not been run yet")
        if alternative_hierarchy:
            h = self._rearrange_results(alternative_hierarchy.copy())
            return recursive_transform(h)
        else:
            return recursive_transform(self.result_tree.copy())

    def _rearrange_results(
        self, hierarchy: dict
    ) -> BasicTreeNode[ScenarioResultNodeData]:
        hierarchy_obj = HierarchyNodeReference(**hierarchy)

        hierarchy_root: BasicTreeNode[
            TechTreeNodeData
        ] = validate_experiment_reference_hierarchy(
            hierarchy_obj,
            self.experiment.hierarchy_root,
            self.experiment.get_node_aggregator,
        )

        def recursive_convert(
            node_: BasicTreeNode[TechTreeNodeData],
        ) -> BasicTreeNode[ScenarioResultNodeData]:
            output: Optional[NodeOutput] = None
            results = {}
            if node_.is_leaf:
                calc_data = self.result_tree.find_subnode_by_name(node_.name).data
                output = calc_data.output
                results = calc_data.results
            return BasicTreeNode(
                name=node_.name,
                data=ScenarioResultNodeData(
                    output=output,
                    results=results,
                    adapter=node_.data.adapter,
                    aggregator=node_.data.aggregator,
                ),
                children=[recursive_convert(child) for child in node_.children],
            )

        result_tree: BasicTreeNode[ScenarioResultNodeData] = recursive_convert(
            hierarchy_root
        )

        from enbios.base.tree_operations import recursive_resolve_outputs

        result_tree.recursive_apply(
            recursive_resolve_outputs,
            experiment=self.experiment,
            depth_first=True,
            cancel_parents_of=set(),
        )

        result_tree.recursive_apply(
            Scenario._propagate_results_upwards,
            experiment=self.experiment,
            depth_first=True,
        )

        return result_tree

    def get_execution_time(self) -> float:
        return self._execution_time

    def __repr__(self):
        return f"<Scenario '{self.name}'>"

    def describe(self):
        output = f"Scenario '{self.name}'\n"
        output += json.dumps(self.structural_nodes_outputs, indent=2)
        # todo: the tree instead...
        return output
