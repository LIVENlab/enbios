from pathlib import Path
from typing import Optional

import pytest

from enbios2.base.experiment import Experiment
from enbios2.const import BASE_TEST_DATA_PATH
from enbios2.generic.files import ReadPath
from enbios2.models.experiment_models import ExperimentData
from test.enbios2.test_project_fixture import TEST_BW_DATABASE, TEST_BW_PROJECT


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


def fix_experiment_data(data: dict, bw_project: str, bw_default_database: Optional[str] = None):
    data["bw_project"] = bw_project
    if "bw_default_database" in data and bw_default_database is not None:
        data["bw_default_database"] = bw_default_database


@pytest.mark.parametrize('experiment_data', argvalues=experiments_data(), ids=experiment_data_file_names())
def test_experiment_data(experiment_data):
    if replace_conf := experiment_data.get("config", {}).get("debug_test_replace_bw_config", True):
        if isinstance(replace_conf, list):
            fix_experiment_data(experiment_data, *replace_conf)
        else:
            fix_experiment_data(experiment_data, TEST_BW_PROJECT, TEST_BW_DATABASE)
    if not experiment_data.get("config", {}).get("debug_test_is_valid", True):
        with pytest.raises(Exception):
            exp_model = ExperimentData(**experiment_data)
            exp = Experiment(exp_model)
            if exp_model.config.debug_test_run:
                exp.run()
    else:
        exp_model = ExperimentData(**experiment_data)
        exp = Experiment(exp_model)
        if exp_model.config.debug_test_run:
            exp.run()


def test_one_experiment_data():
    filename = "d.json"
    experiment_data = ReadPath(BASE_TEST_DATA_PATH / "experiment_instances"/ filename).read_data()
    if replace_conf := experiment_data.get("config", {}).get("debug_test_replace_bw_config", True):
        if isinstance(replace_conf, list):
            fix_experiment_data(experiment_data, *replace_conf)
        else:
            fix_experiment_data(experiment_data, TEST_BW_PROJECT, TEST_BW_DATABASE)
    if not experiment_data.get("config", {}).get("debug_test_is_valid", True):
        with pytest.raises(Exception):
            exp_model = ExperimentData(**experiment_data)
            exp = Experiment(exp_model)
            if exp_model.config.debug_test_run:
                exp.run()
    else:
        exp_model = ExperimentData(**experiment_data)
        exp = Experiment(exp_model)
        if exp_model.config.debug_test_run:
            exp.run()


def test_csv_setup():
    pass
