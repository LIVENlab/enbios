from dataclasses import dataclass
from typing import Union, Literal, Optional

import bw2data as bd
from bw2data import Method
from bw2data.backends import Activity

from enbios2.generic.tree.basic_tree import BasicTreeNode


@dataclass
class Enbios2BaseTreeNode:
    node_type: Literal["activity", "method", "level"]


@dataclass
class ScenarioTree_MethodNode_Data(Enbios2BaseTreeNode):
    method: Method


@dataclass
class TechnologyTree_ActivityNode_Data(Enbios2BaseTreeNode):
    activity: Activity
    amount: float
    impact_sum: float = 0


@dataclass
class TechnologyTree_LevelNode_Data(Enbios2BaseTreeNode):
    impact_sum: Optional[float] = None


TechnologyTree_Node_Data = Union[TechnologyTree_ActivityNode_Data, TechnologyTree_LevelNode_Data]
ScenarioTree_Node_Data = Union[ScenarioTree_MethodNode_Data, TechnologyTree_LevelNode_Data]


@dataclass
class BW_CalculationSetup:
    name: str
    inv: list[tuple[Activity, float]]
    ia: list[tuple[str]]


def build_technology_tree(raw_tree: BasicTreeNode) -> BasicTreeNode[TechnologyTree_Node_Data]:
    root: BasicTreeNode[TechnologyTree_Node_Data] = raw_tree.copy()
    for activity_node in root.get_leaves():
        _data = activity_node._data
        activity = db.get(_data["code"])
        activity_node.data = TechnologyTree_ActivityNode_Data("activity", activity, _data.get("amount", 1))
    return root


def build_scenario_tree(raw_tree: BasicTreeNode) -> BasicTreeNode[TechnologyTree_Node_Data]:
    root: BasicTreeNode[ScenarioTree_Node_Data] = raw_tree.copy()
    for node in root.iter_all_nodes():
        _data = node._data
        if _data and _data["method_name"]:
            method = bd.Method(_data["method_name"])
            node.data = ScenarioTree_MethodNode_Data("method", method)
    return root


def build_complete_scenario_tree(scenario_tree: BasicTreeNode[ScenarioTree_Node_Data],
                                 technology_tree: BasicTreeNode[TechnologyTree_Node_Data]) -> BasicTreeNode[
    ScenarioTree_Node_Data]:
    """
    Combines the scenario tree with the technology tree. add a copy of eac
    :param scenario_tree:
    :param technology_tree:
    :return:
    """

def create_bw_calculation_setup(scenario_tree: BasicTreeNode[ScenarioTree_Node_Data]) -> BW_CalculationSetup:
    """
    Collects the data for the scenario tree from the bw2 database.
    """
    inventory: list[tuple[Activity, float]] = []
    methods: list[tuple[str]] = []
    for node in scenario_tree.iter_all_nodes():
        _data = node.data
        if not _data:
            node.data = TechnologyTree_LevelNode_Data("level", 0)
            continue
        if _data.node_type == "method":
            node.data: ScenarioTree_MethodNode_Data
            methods.append(_data.method.name)
        if _data.node_type == "activity":
            node.data: TechnologyTree_ActivityNode_Data
            inventory.append((_data.activity, _data.amount))

    return BW_CalculationSetup(scenario_tree.name, inventory, methods)


if __name__ == "__main__":
    # bd.projects
    bd.projects.set_current("ecoi_dbs")
    db = bd.Database("cutoff391")

    tech_root = BasicTreeNode("root")
    activity_node = tech_root.add_child(BasicTreeNode("a1"))
    activity_node._data["code"] = '44a1182a96d51845ecf6840a470d1b95'
    tech_root = build_technology_tree(tech_root)
    # todo, that is not the right approach... since we can do a MultiLCI we dont need to a node per method...

    scenario_tree_root = BasicTreeNode("root")
    method_node = scenario_tree_root.add_child(BasicTreeNode("m1"))
    method_node._data["method_name"] = (
        'CML v4.8 2016 no LT', 'acidification no LT', 'acidification (incl. fate, average Europe total, A&B) no LT')
    scenario_tree_root = build_scenario_tree(scenario_tree_root)

    bw_calc_setup = create_bw_calculation_setup(scenario_tree_root)
