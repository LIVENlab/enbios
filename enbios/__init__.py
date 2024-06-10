import importlib.metadata
import shutil
from pathlib import Path

from pint import UnitRegistry

from enbios.base.experiment import Experiment
from enbios.base.models import TechTreeNodeData, ResultValue, ScenarioResultNodeData
from enbios.base.scenario import Scenario
from enbios.bw2.util import report
from enbios.generic.files import PathLike
from enbios.generic.tree.basic_tree import BasicTreeNode

version = importlib.metadata.version("enbios")

__all__ = [
    "Experiment",
    "Scenario",
    "TechTreeNodeData",
    "ResultValue",
    "ScenarioResultNodeData",
    "report",
    "PathLike",
    "BasicTreeNode",
]


def get_enbios_ureg() -> UnitRegistry:
    from enbios.base.unit_registry import ureg

    return ureg


def copy_demos(destination: PathLike):
    destination = Path(destination)
    demo_path = Path(__file__).parents[1] / "demos"
    if destination.exists():
        raise ValueError(f"Destination path '{destination}' already exists.")
    shutil.copytree(demo_path, destination, dirs_exist_ok=True)
