import importlib.metadata

from pint import UnitRegistry

version = importlib.metadata.version('enbios')

from enbios.base.experiment import Experiment
from enbios.base.scenario import Scenario
from enbios.generic.tree.basic_tree import BasicTreeNode
from enbios.bw2.util import report
from enbios.models.experiment_models import (
    ScenarioResultNodeData,
    ResultValue,
    TechTreeNodeData,
)

def get_enbios_ureg() -> UnitRegistry:
    from enbios.base.unit_registry import ureg
    return ureg
