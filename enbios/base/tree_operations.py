from typing import Any, TYPE_CHECKING, Callable

from enbios.models.experiment_base_models import (
    ExperimentHierarchyNodeData,
    HierarchyNodeReference,
)

if TYPE_CHECKING:
    from enbios.base.experiment import Experiment

from enbios.generic.enbios2_logging import get_logger
from enbios.generic.tree.basic_tree import BasicTreeNode
from enbios.models.experiment_models import TechTreeNodeData, ScenarioResultNodeData

logger = get_logger(__name__)


def validate_experiment_hierarchy(
    hierarchy: ExperimentHierarchyNodeData,
) -> BasicTreeNode[TechTreeNodeData]:
    # todo allow no output only when there are scenarios...
    tech_tree: BasicTreeNode[TechTreeNodeData] = BasicTreeNode.from_dict(
        hierarchy.model_dump(), dataclass=TechTreeNodeData
    )

    def validate_node_data(node: BasicTreeNode[TechTreeNodeData]) -> Any:
        good_leaf = node.is_leaf and node.data.adapter
        good_internal = not node.is_leaf and node.data.aggregator
        assert good_leaf or good_internal, (
            f"Node should have the leaf properties (id, adapter) "
            f"or non-leaf properties (children, aggregator): "
            f"{node.location_names()})"
        )
        return True

    tech_tree.recursive_apply(validate_node_data, depth_first=True)  # type: ignore
    return tech_tree


def validate_experiment_reference_hierarchy(
    hierarchy: HierarchyNodeReference,
    original_experiment_hierarchy: BasicTreeNode[TechTreeNodeData],
    get_node_aggregator_fcn: Callable,
) -> BasicTreeNode[TechTreeNodeData]:
    tech_tree: BasicTreeNode[TechTreeNodeData] = BasicTreeNode.from_dict(
        hierarchy.model_dump(), dataclass=TechTreeNodeData
    )

    def validate_node_data(node: BasicTreeNode[TechTreeNodeData]) -> Any:
        if node.is_leaf:
            if node.data.adapter:
                logger.warning(
                    "Adapter is not allowed in reference hierarchy and will be ignored"
                )
            orig_node = original_experiment_hierarchy.find_subnode_by_name(node.name)
            if not orig_node:
                raise ValueError(f"Node {node.name} not found in original hierarchy")
            node.set_data(orig_node.data)
        else:
            if not node.data.aggregator:
                orig_node = original_experiment_hierarchy.find_subnode_by_name(node.name)
                if not orig_node:
                    get_node_aggregator_fcn(node).validate_node(
                        node.name, node.data.config
                    )
                    # raise ValueError(f"Node '{node.name}' not found in original hierarchy")
                node.set_data(orig_node.data)
        return True

    tech_tree.recursive_apply(validate_node_data, depth_first=True)  # type: ignore
    return tech_tree


def recursive_resolve_outputs(
    node: BasicTreeNode[ScenarioResultNodeData], experiment: "Experiment", **kwargs
):
    # todo, does this takes default values when an node is not defined
    #  in the scenario?
    if node.is_leaf:
        return
    cancel_parts_of: set = kwargs["cancel_parents_of"]
    if any(child.id in cancel_parts_of for child in node.children):
        cancel_parts_of.add(node.id)

    aggregator = experiment.get_node_aggregator(node)
    node_output = aggregator.aggregate_node_output(node, kwargs.get("scenario_name"))
    if not node_output:
        cancel_parts_of.add(node.id)
    else:
        node.data.output = node_output
