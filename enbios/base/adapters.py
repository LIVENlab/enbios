import inspect
import sys
from importlib import import_module
from pathlib import Path
from typing import Optional, get_type_hints

from enbios.models.experiment_models import Adapter, AdapterFunctions, AdapterModel


def load_adapter(adapter_model: AdapterModel) -> Adapter:
    _path = Path(adapter_model.module_path)
    sys.path.insert(0, _path.parent.as_posix())
    adapter_module = import_module(Path(adapter_model.module_path).stem)

    adapter_name = adapter_model.name if adapter_model.name else adapter_module.__name__

    adapter_functions: dict[str, callable] = {}

    for func_name, function in inspect.getmembers(adapter_module, inspect.isfunction):
        if func_name == adapter_model.activity_validator_function:
            adapter_functions["validate_activity"] = function
            func_types = list(get_type_hints(function).values())
            # print(func_types)
            assert len(func_types) > 0
            assert isinstance(func_types[0], dict.__class__)
        elif func_name == adapter_model.run_function:
            func_types = list(get_type_hints(function).values())
            # print(func_types)
            assert len(
                func_types) > 1, ("run function must have at least one argument of type "
                                  "str (scenario name) and a return type of dict")
            assert func_types[0] == Optional[
                str], "first argument must be of type str (scenario name)"
            assert isinstance(get_type_hints(function)["return"],
                              dict.__class__), "return type must be of type dict"
            adapter_functions["run"] = function

    # todo, why does it only acceept adapter_model as dict...
    return Adapter(
        name=adapter_name,
        model=dict(adapter_model),
        functions=AdapterFunctions(**adapter_functions))
