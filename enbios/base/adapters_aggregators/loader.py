import inspect
import sys
from importlib import import_module
from pathlib import Path
from typing import Union, Type

from enbios.base.adapters_aggregators.adapter import EnbiosAdapter
from enbios.base.adapters_aggregators.aggregator import EnbiosAggregator
from enbios.models.experiment_models import AdapterModel, AggregationModel


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
            return clazz()

    raise ValueError(f"'{Path(adapter_model.module_path).name}' has no class that inherits from '{base_class.__name__}'")


def load_adapter(adapter_model: AdapterModel) -> EnbiosAdapter:
    return create_module_object(adapter_model, EnbiosAdapter)


def load_aggregator(aggregator_model: AggregationModel) -> EnbiosAggregator:
    return create_module_object(aggregator_model, EnbiosAggregator)
