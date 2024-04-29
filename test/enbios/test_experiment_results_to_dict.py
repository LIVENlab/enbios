import json
from itertools import product
from pathlib import Path
from typing import Generator

import pytest

from enbios import Experiment
from enbios.const import BASE_TEST_DATA_PATH


@pytest.fixture
def results_base_folder() -> Path:
    return BASE_TEST_DATA_PATH / "experiment_json_results"


@pytest.fixture
def temp_file() -> Path:
    return BASE_TEST_DATA_PATH / "temp/temp.json"


def result_data_configs_and_names() -> Generator[tuple[dict[str, bool],str], None, None]:
    bool_param_names = ["include_method_units", "include_output", "include_extras"]
    for args_value_product in list(product([True, False], repeat=len(bool_param_names))):
        kwargs = dict(zip(
            bool_param_names,
            list(args_value_product)
        ))
        yield (kwargs,
               f"t_{'_'.join([str(k) for k, v in kwargs.items() if v])}.json")


def result_data_names() -> Generator[str, None, None]:
    for config_name in result_data_configs_and_names():
        yield config_name[1]


@pytest.mark.parametrize('result_data_config, result_file_name',
                         argvalues=result_data_configs_and_names(), ids=result_data_names())
def test_experiment_to_dict(two_level_experiment_from_pickle: Experiment,
                            result_data_config: dict,
                            result_file_name: str,
                            results_base_folder: Path):
    kwargs: dict[str, bool] = result_data_config
    result_dict = two_level_experiment_from_pickle.result_to_dict(**kwargs)
    assert result_dict == json.load((results_base_folder / result_file_name).open("r", encoding="utf-8"))
