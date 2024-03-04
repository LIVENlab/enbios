import inspect
from pathlib import Path
from types import ModuleType
from typing import Union, Type, Optional, cast

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
        assert model_data.module_path
        try:
            adapter_module: ModuleType = load_module(model_data.module_path)
        except Exception as err:
            raise ValueError(f"Could not load module '{model_data.module_path}' ({err})")
        # validator makes sure there is no other case
        for inspect_clazz in inspect.getmembers(adapter_module, inspect.isclass):
            # check if cl is subclass of EnbiosAdapter/EnbiosAggregation
            clazz: Optional[Type[Union[EnbiosAdapter, EnbiosAggregator]]] = inspect_clazz[
                1
            ]
            assert clazz
            if any(base.__name__ == base_class.__name__ for base in clazz.__bases__):
                return clazz()
        raise ValueError(
            f"'{Path(model_data.module_path).name}' has no class that inherits from '{base_class.__name__}'"
        )
    else:
        if base_class.__name__ == EnbiosAdapter.__name__:
            assert (
                hasattr(model_data, "adapter_name")
                and model_data.adapter_name is not None
            )
            clazz = BUILTIN_ADAPTERS.get(model_data.adapter_name)
            if not clazz:
                raise ValueError(
                    f"Could not find built-in adapter with name '{model_data.adapter_name}'. "
                    f"Candidates are: {BUILTIN_ADAPTERS.keys()}"
                )
            else:
                return clazz()
        else:
            assert base_class.__name__ == EnbiosAggregator.__name__
            assert hasattr(model_data, "aggregator_name")
            clazz = BUILTIN_AGGREGATORS.get(model_data.aggregator_name)
            if not clazz:
                raise ValueError(
                    f"Could not find built-in aggregator with name '{model_data.aggregator_name}'. "
                    f"Candidates are: {BUILTIN_AGGREGATORS.keys()}"
                )
            else:
                return clazz()


def load_adapter(adapter_model: AdapterModel) -> EnbiosAdapter:
    return cast(EnbiosAdapter, create_module_object(adapter_model, EnbiosAdapter))


def load_aggregator(aggregator_model: AggregationModel) -> EnbiosAggregator:
    return cast(
        EnbiosAggregator, create_module_object(aggregator_model, EnbiosAggregator)
    )
