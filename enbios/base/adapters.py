import inspect
import sys
from abc import ABC, abstractmethod
from importlib import import_module
from pathlib import Path

from enbios.base.scenario import Scenario
from enbios.models.experiment_models import AdapterModel, ExperimentActivityData, ActivityOutput


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


def load_adapter(adapter_model: AdapterModel) -> EnbiosAdapter:
    _path = Path(adapter_model.module_path)
    sys.path.insert(0, _path.parent.as_posix())
    adapter_module = import_module(Path(adapter_model.module_path).stem)

    for cl in inspect.getmembers(adapter_module, inspect.isclass):
        # check if cl is subclass of EnbiosAdapter
        if cl[1].__bases__[0].__name__ == EnbiosAdapter.__name__:
            adapter = cl[1](adapter_model.config)
            return adapter

    raise ValueError(f"Adapter '{adapter_model.name}' has no class that inherits from EnbiosAdapter")
