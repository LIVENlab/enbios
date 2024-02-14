import inspect
from pathlib import Path
from types import ModuleType
from typing import Union, Type

from enbios.base.adapters_aggregators.adapter import EnbiosAdapter
from enbios.base.adapters_aggregators.aggregator import EnbiosAggregator
from enbios.base.adapters_aggregators.builtin import BUILTIN_ADAPTERS, BUILTIN_AGGREGATORS
from enbios.generic.enbios2_logging import get_logger
from enbios.generic.util import load_module
from enbios.models.experiment_base_models import AdapterModel, AggregationModel

logger = get_logger(__name__)


def create_module_object(
    model_data: Union[AdapterModel, AggregationModel], base_class: Type
) -> Union[EnbiosAdapter, EnbiosAggregator]:
    if model_data.module_path:
        try:
            adapter_module: ModuleType = load_module(model_data.module_path)
        except Exception as err:
            raise ValueError(f"Could not load module '{model_data.module_path}' ({err})")
        # validator makes sure there is no other case
        for inspect_clazz in inspect.getmembers(adapter_module, inspect.isclass):
            # check if cl is subclass of EnbiosAdapter/EnbiosAggregation
            clazz = inspect_clazz[1]
            if any(base.__name__ == base_class.__name__ for base in clazz.__bases__):
                return clazz()
    else:
        if base_class.__name__ == EnbiosAdapter.__name__:
            clazz = BUILTIN_ADAPTERS.get(model_data.adapter_name)
            if not clazz:
                logger.error(
                    f"Could not find built-in adapter with name '{model_data.adapter_name}'. Candidates are: {BUILTIN_ADAPTERS.keys()}"
                )
            else:
                return clazz()
        else:
            assert base_class.__name__ == EnbiosAggregator.__name__
            clazz = BUILTIN_AGGREGATORS.get(model_data.aggregator_name)
            if not clazz:
                logger.error(
                    f"Could not find built-in aggregator with name '{model_data.aggregator_name}'. Candidates are: {BUILTIN_AGGREGATORS.keys()}"
                )
            else:
                return clazz()
    raise ValueError(
        f"'{Path(model_data.module_path).name}' has no class that inherits from '{base_class.__name__}'"
    )


def load_adapter(adapter_model: AdapterModel) -> EnbiosAdapter:
    return create_module_object(adapter_model, EnbiosAdapter)


def load_aggregator(aggregator_model: AggregationModel) -> EnbiosAggregator:
    return create_module_object(aggregator_model, EnbiosAggregator)
