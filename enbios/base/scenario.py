import concurrent.futures
import math
import time
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Optional, Union, TYPE_CHECKING, Any, Callable, Type

from enbios.base.tree_operations import validate_experiment_reference_hierarchy
from enbios.generic.enbios2_logging import get_logger
from enbios.generic.files import PathLike
from enbios.models.models import (
    HierarchyNodeReference,
    ScenarioConfig,
    NodeOutput,
    TechTreeNodeData,
    ResultValue,
    ScenarioResultNodeData,
)

# for type hinting
if TYPE_CHECKING:
    from enbios.base.experiment import Experiment
from enbios.generic.tree.basic_tree import BasicTreeNode

logger = get_logger(__name__)


@dataclass
class Scenario:
    experiment: "Experiment"
    name: str
    result_tree: BasicTreeNode[ScenarioResultNodeData]

    _has_run: bool = False
    # this should be a simpler type - just str: float
    # structural_nodes_outputs: dict[str, float] = field(default_factory=dict)
    # methods: Optional[dict[str, ExperimentMethodPrepData]] = None
    _execution_time: float = float("NaN")
    config: ScenarioConfig = field(default_factory=ScenarioConfig)  # type: ignore

    def prepare_tree(self):
        """Prepare the result tree for calculating scenario outputs.
        This populates the result tree with ScenarioResultNodeData objects
        for each node, which store the output magnitude and units.
        If config is set, it also stores the BW node dict with the node.
        """

        # structural_nodes_names = list(n.name for n in )
        from enbios.base.adapters_aggregators.adapter import EnbiosAdapter
        structural_nodes_names: list[str] = []
        for result_index, node in enumerate(self.result_tree.iter_leaves()):
            # try:
            #     structural_result_node = self.result_tree.find_subnode_by_name(node_name)
            # except StopIteration:
            #     raise ValueError(f"Node {node_name} not found in result tree")
            # structural_node = self.experiment.get_structural_node(node.name)
            # todo: should be dealt returned by the adapter...
            node.data.output = self.experiment.get_node_module(
                node.name,
                Type[EnbiosAdapter]
            ).get_node_output(node.name, self.name)
            structural_nodes_names.append(node.name)

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
            node: BasicTreeNode[ScenarioResultNodeData],
            experiment: "Experiment",
            scenario_name: str
    ):
        from enbios.base.adapters_aggregators.aggregator import EnbiosAggregator
        if node.is_leaf:
            return
        else:
            aggregator: EnbiosAggregator = experiment.get_node_module(node.name, Type[EnbiosAggregator])
            node.data.results = aggregator.aggregate_node_result(node, scenario_name)
            node.data.extras = aggregator.result_extras(node.name, scenario_name)

    def run(
            self, results_as_dict: bool = True
    ) -> Union[BasicTreeNode[ScenarioResultNodeData], dict]:
        # if not self._get_methods():
        #     raise ValueError(f"Scenario '{self.name}' has no methods")
        self.reset_execution_time()
        logger.info(f"Running scenario '{self.name}'")
        # distributions_config = self.experiment.config.use_k_bw_distributions
        # distribution_results = distributions_config > 1
        start_time = time.time()

        if self.experiment.config.run_adapters_concurrently:
            # Create a ThreadPoolExecutor
            with concurrent.futures.ThreadPoolExecutor() as executor:
                # Use a list comprehension to start a thread for each adapter
                futures = [
                    executor.submit(adapter.run_scenario, self)  # type: ignore
                    for adapter in self.experiment.adapters
                ]
                # As each future completes, set the results
                for future in concurrent.futures.as_completed(futures):
                    result_data = future.result()
                    self.set_results(result_data)
        else:
            for adapter in self.experiment.adapters:
                # run in parallel:
                result_data = adapter.run_scenario(self)  # type: ignore
                self.set_results(result_data)

        self.result_tree.recursive_apply(
            Scenario._propagate_results_upwards,  # type: ignore
            experiment=self.experiment,
            depth_first=True,
            scenario_name=self.name
        )

        self._has_run = True
        self._execution_time = time.time() - start_time
        return self.result_to_dict(include_extras=True) if results_as_dict else self.result_tree

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
            node.data.extras = self.experiment.get_node_module(node).result_extras(node.name, self.name)
        if self.config.exclude_defaults:
            for leave in self.result_tree.iter_leaves():
                if not leave.data.results:
                    parent = leave.parent
                    leave.remove_self()
                    while parent:
                        if parent.get_num_children() == 0:
                            node = parent
                            parent = parent.parent
                            node.remove_self()
                        else:
                            break

    @staticmethod
    def wrapper_data_serializer(
            *,
            include_output: bool = True,
            include_method_units: bool = True,
            include_extras: bool = True,
    ) -> Callable[[ScenarioResultNodeData], dict]:
        def _expand_results(results: dict[str, ResultValue]) -> dict:
            """
            brings all results to the data level (one down) which is useful for csv
            :param results:
            :return:
            """
            expanded_results = {}
            for method_name, result_value in results.items():
                expanded_results[method_name] = result_value.model_dump(
                    exclude_defaults=True,
                    exclude_unset=True,
                    exclude={} if include_method_units else {"unit"},
                )
            return expanded_results

        def data_serializer(data: ScenarioResultNodeData) -> dict:
            result: dict[str, Any] = {}
            result["results"] = _expand_results(data.results)
            # there might be no output, when the units don't match
            if include_output:
                result["output"] = [output.model_dump() for output in data.output]
            if include_extras and data.extras:
                extras: dict[str, Any] = data.extras
                for k in ["name", "results", "output"]:
                    if k in extras:
                        logger.warning(
                            f"node result extras contain key '{k}', which is reserved for the scenario result"
                        )
                        extras.pop(k)
                result.update(extras)
            return result

        return data_serializer

    @staticmethod
    def wrapped_flat_output_list_serializer(
            serializer: Callable[[ScenarioResultNodeData], dict]
    ) -> Callable[[ScenarioResultNodeData], dict]:
        from enbios.generic.flatten_dict import flatten_dict
        def flattened_wrap(result_data: ScenarioResultNodeData) -> dict:
            res = serializer(result_data)
            return flatten_dict.flatten(res, reducer="underscore", enumerate_types={list})

        return flattened_wrap

    def results_to_csv(
            self,
            file_path: PathLike,
            level_names: Optional[list[str]] = None,
            include_output: bool = True,
            include_method_units: bool = True,
            warn_no_results: bool = True,
            alternative_hierarchy: Optional[dict] = None,
            flat_hierarchy: Optional[bool] = False,
            repeat_parent_name: bool = False,
            include_extras: bool = True,
    ):
        """
        Save the results (as tree) to a csv file
         :param include_extras:
         :param repeat_parent_name:
         :param flat_hierarchy:
         :param include_output:
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
            data_serializer=self.wrapped_flat_output_list_serializer(
                self.wrapper_data_serializer(
                    include_output=include_output,
                    include_method_units=include_method_units,
                    include_extras=include_extras,
                )
            ),
            repeat_parent_name=repeat_parent_name,
            flat_hierarchy=flat_hierarchy,
        )

    def result_to_dict(
            self,
            include_output: bool = True,
            include_method_units: bool = True,
            include_extras: bool = True,
            warn_no_results: bool = True,
            alternative_hierarchy: Optional[dict] = None,
    ) -> dict[str, Any]:
        """
        Return the results as a dictionary
        :param include_output:
        :param warn_no_results:
        :param alternative_hierarchy: An alternative hierarchy to use for the results,
        which comes from Scenario.rearrange_results.
        :return:
        """

        # todo params for result_units, extras
        def recursive_transform(node: BasicTreeNode[ScenarioResultNodeData]) -> dict:
            result: dict[str, Any] = {
                "name": node.name,
                **Scenario.wrapper_data_serializer(
                    include_output=include_output,
                    include_method_units=include_method_units,
                    include_extras=include_extras,
                )(node.data),
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
            self.experiment.get_node_module,
        )

        def recursive_convert(
                node_: BasicTreeNode[TechTreeNodeData],
        ) -> BasicTreeNode[ScenarioResultNodeData]:
            output: list[NodeOutput] = []
            results: dict = {}
            if node_.is_leaf:
                calc_data = self.result_tree.find_subnode_by_name(node_.name).data  # type: ignore
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
            recursive_resolve_outputs,  # type: ignore
            experiment=self.experiment,
            depth_first=True,
            cancel_parents_of=set(),
        )

        result_tree.recursive_apply(
            Scenario._propagate_results_upwards,  # type: ignore
            experiment=self.experiment,
            depth_first=True,
            scenario_name=self.name,
        )

        return result_tree

    def get_execution_time(self) -> float:
        return self._execution_time

    def __repr__(self):
        return f"<Scenario '{self.name}'>"

    def describe(self):
        output = f"Scenario '{self.name}'\n"
        # output += json.dumps(self.structural_nodes_outputs, indent=2)
        # todo: the tree instead...
        return output

    @property
    def has_run(self):
        return self._has_run
