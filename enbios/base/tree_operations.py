import re
from csv import DictReader
from pathlib import Path
from typing import Any, TYPE_CHECKING, Callable, Optional, Iterator

from enbios import PathLike
from enbios.models.models import (
    ExperimentHierarchyNodeData,
    HierarchyNodeReference,
    EnbiosValidationException,
    TechTreeNodeData,
    ScenarioResultNodeData,
)
from enbios.util.flatten_dict.flatten_dict import unflatten

if TYPE_CHECKING:
    from enbios.base.experiment import Experiment

from enbios.generic.enbios2_logging import get_logger
from enbios.generic.tree.basic_tree import BasicTreeNode

logger = get_logger(__name__)


def validate_experiment_hierarchy(
        hierarchy: ExperimentHierarchyNodeData,
) -> BasicTreeNode[TechTreeNodeData]:
    # todo allow no output only when there are scenarios...
    tech_tree: BasicTreeNode[TechTreeNodeData] = BasicTreeNode.from_dict(
        hierarchy.model_dump(), dataclass=TechTreeNodeData
    )

    def validate_node_data(node: BasicTreeNode[TechTreeNodeData]) -> Any:
        good_leaf = node.is_leaf and node.data.adapter is not None
        good_internal = not node.is_leaf and node.data.aggregator is not None
        if not (good_leaf or good_internal):
            raise EnbiosValidationException(
                f"Node should have the leaf properties (adapter) "
                f"or non-leaf properties (children, aggregator): "
                f"{node.location_names()} {node.data})",
                "Treenode-error",
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
                else:
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


def csv2hierarchy(
        csv_file: PathLike,
        level_cols: Optional[list[str]] = (),
        levels_regex: Optional[str] = "^level_\d+$",
) -> dict:
    tree: dict = {}
    reader: Iterator[dict[str, str]] = DictReader(Path(csv_file).open())
    current_node = tree
    _path: Optional[list[int]] = None

    all_node_names = set()

    def create_mode(_row: dict, name: str):
        name = name.strip()
        if name in all_node_names:
            raise ValueError(f"Node '{name}' already exists")
        all_node_names.add(name)
        node = {"name": name, "module": row["module"], "config": {}, "children": []}
        all_node_names.add(name)
        if not row["module"]:
            raise ValueError(f"Module is not specified for node '{name}'")
        config_cols = {col: val.strip() for col, val in _row.items() if col.startswith("config.") and val}
        config = unflatten(config_cols, splitter="dot")
        node.update(config)
        return node

    def insert(_row: dict, _cell: str, _current_node: dict):
        _next_node = create_mode(_row, _cell)
        _current_node["children"].append(_next_node)
        _path.append(len(current_node["children"]) - 1)
        _current_node = _next_node
        return _current_node

    level_cols = level_cols or []
    if not level_cols:
        if levels_regex:
            levels_re = re.compile(levels_regex)
            level_cols = sorted([fn for fn in reader.fieldnames if levels_re.match(fn)])

    for row in reader:
        row_added = False
        for depth, col in enumerate(level_cols):
            cell = row[col]
            if not cell:
                continue
            if row_added:
                raise ValueError(f"row: {row} has duplicate level indicators: '{cell}'")
            if _path is None:  # root node
                tree = create_mode(row, cell)
                _path = []
                current_node = tree
            else:
                if depth - 1 == len(_path):
                    current_node = insert(row, cell, current_node)
                elif depth - 1 < len(_path):
                    _path = _path[: depth - 1]
                    current_node = tree
                    for p in _path:
                        current_node = current_node["children"][p]
                    current_node = insert(row, cell, current_node)
                else:
                    raise IndexError(
                        f"row: {row} has a cell: '{cell}'"
                        f" that is not positioned correctly"
                    )
            row_added = True

    def recursive_node_module_set(node: dict):
        if node["children"]:
            node["aggregator"] = node["module"]
            del node["module"]
            for child in node["children"]:
                recursive_node_module_set(child)
        else:
            node["adapter"] = node["module"]
            del node["module"]
            del node["children"]
        if not node["config"]:
            del node["config"]

    recursive_node_module_set(tree)
    return tree
