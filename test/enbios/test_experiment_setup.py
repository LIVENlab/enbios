import sys
from logging import getLogger
from pathlib import Path
from typing import Optional

import pytest

from enbios.base.experiment import Experiment
from enbios.const import BASE_TEST_DATA_PATH
from enbios.generic.files import ReadPath
from enbios.models.experiment_models import ExperimentData

try:
    from test.enbios.test_project_fixture import TEST_BW_DATABASE, TEST_BW_PROJECT, BRIGHTWAY_ADAPTER_MODULE_PATH
except ImportError as err:
    getLogger("test-logger").error("Please copy test/enbios/test_project_fixture.py.example to "
                                   "test/enbios/test_project_fixture.py and fill in the values.")
    sys.exit(1)


# This fixture scans a directory and returns all files in the directory.
@pytest.fixture
def experiment_files(tmp_path):
    # todo this is not working
    print(list(Path(BASE_TEST_DATA_PATH / "experiment_instances").glob("*.json")))
    return list((BASE_TEST_DATA_PATH / "experiment_instances").glob("*.json"))


def experiments_data():
    for file in (BASE_TEST_DATA_PATH / "experiment_instances").glob("*.json"):
        yield ReadPath(file).read_data()


def experiment_data_file_names():
    for file in (BASE_TEST_DATA_PATH / "experiment_instances").glob("*.json"):
        yield file.name


def fix_experiment_data(data: dict, bw_project: str, module_path: str, bw_default_database: Optional[str] = None):
    for adapter in data["adapters"]:
        if adapter.get("note") == "brightway-adapter":
            adapter["config"]["bw_project"] = bw_project
            adapter["module_path"] = module_path
            if "bw_default_database" in adapter["config"] and bw_default_database is not None:
                adapter["config"]["bw_default_database"] = bw_default_database


@pytest.mark.parametrize('experiment_data', argvalues=experiments_data(), ids=experiment_data_file_names())
def test_experiment_data(experiment_data):
    if replace_conf := experiment_data.get("config", {}).get("debug_test_replace_bw_config", True):
        if isinstance(replace_conf, list):
            fix_experiment_data(experiment_data, *replace_conf)
        else:
            fix_experiment_data(experiment_data, TEST_BW_PROJECT, BRIGHTWAY_ADAPTER_MODULE_PATH, TEST_BW_DATABASE)
    if not experiment_data.get("config", {}).get("debug_test_is_valid", True):
        with pytest.raises(Exception):
            ExperimentData(**experiment_data)
            Experiment(experiment_data)
            # if exp_model.config.debug_test_run:
            #     exp.run()
    else:
        exp_model = ExperimentData(**experiment_data)
        exp = Experiment(experiment_data)
        if exp_model.config.debug_test_run:
            exp.run()


def test_one_experiment_data():
    filename = "b.json"
    experiment_data = ReadPath(BASE_TEST_DATA_PATH / "experiment_instances" / filename).read_data()
    if replace_conf := experiment_data.get("config", {}).get("debug_test_replace_bw_config", True):
        if isinstance(replace_conf, list):
            fix_experiment_data(experiment_data, *replace_conf)
        else:
            fix_experiment_data(experiment_data, TEST_BW_PROJECT, BRIGHTWAY_ADAPTER_MODULE_PATH, TEST_BW_DATABASE)
    if not experiment_data.get("config", {}).get("debug_test_is_valid", True):
        with pytest.raises(Exception):
            exp_model = ExperimentData(**experiment_data)
            exp = Experiment(experiment_data)
            if exp_model.config.debug_test_run:
                exp.run()
    else:
        exp_model = ExperimentData(**experiment_data)
        exp = Experiment(experiment_data)
        if exp_model.config.debug_test_run:
            exp.run()


def test_csv_setup():
    pass
