import inspect
import sys
from abc import ABC, abstractmethod
from importlib import import_module
from pathlib import Path
from typing import Union, Type

from enbios.base.scenario import Scenario
from enbios.generic.tree.basic_tree import BasicTreeNode
from enbios.models.experiment_models import AdapterModel, ExperimentActivityData, ActivityOutput, AggregationModel, \
    ScenarioResultNodeData


class EnbiosAdapter(ABC):

    @abstractmethod
    def validate_config(self):
        pass

    @abstractmethod
    def validate_methods(self):
        pass

    @abstractmethod
    def validate_activity_output(self, activity: ExperimentActivityData, target_output: ActivityOutput):
        pass

    @abstractmethod
    def validate_activity(self, activity: ExperimentActivityData, required_output: bool = False):
        pass

    @abstractmethod
    def get_activity_unit(self, activity_alias: str) -> str:
        pass

    @abstractmethod
    def get_default_output_value(self, activity_alias: str) -> float:
        pass

    @abstractmethod
    def prepare_scenario(self, scenario: Scenario):
        pass

    @abstractmethod
    def run(self):
        pass

    @abstractmethod
    def run_scenario(self, scenario: Scenario):
        pass

    @property
    @abstractmethod
    def activity_indicator(self) -> str:
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        pass


class EnbiosAggregator(ABC):

    @abstractmethod
    def validate_config(self):
        pass

    @abstractmethod
    def validate_node_output(self,node: BasicTreeNode[ScenarioResultNodeData]):
        pass

    @abstractmethod
    def aggregate_results(self,node: BasicTreeNode[ScenarioResultNodeData]):
        pass

    @property
    @abstractmethod
    def node_indicator(self) -> str:
        pass


def load_module(module_path: str):
    _path = Path(module_path)
    # todo are we doing this earlier?
    if not _path.exists():
        raise ValueError(f"Module path '{module_path}' does not exist")
    sys.path.insert(0, _path.parent.as_posix())
    return import_module(_path.stem)


def create_module_object(adapter_model: Union[AdapterModel, AggregationModel], base_class: Type) -> Union[
    EnbiosAdapter, EnbiosAggregator]:
    try:
        adapter_module = load_module(adapter_model.module_path)
    except Exception as err:
        raise ValueError(f"Could not load module '{adapter_model.module_path}' ({err})")
    for inspect_clazz in inspect.getmembers(adapter_module, inspect.isclass):
        # check if cl is subclass of EnbiosAdapter/EnbiosAggregation
        clazz = inspect_clazz[1]
        if any(base.__name__ == base_class.__name__ for base in clazz.__bases__):
            return clazz(adapter_model.config)

    raise ValueError(f"'{Path(adapter_model.module_path).name}' has no class that inherits from '{base_class.__name__}'")


def load_adapter(adapter_model: AdapterModel) -> EnbiosAdapter:
    return create_module_object(adapter_model, EnbiosAdapter)


def load_aggregator(aggregator_model: AggregationModel) -> EnbiosAggregator:
    return create_module_object(aggregator_model, EnbiosAggregator)
