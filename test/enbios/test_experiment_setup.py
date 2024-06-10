import json
import sys
from logging import getLogger
from pathlib import Path

import pytest

from enbios.base.experiment import Experiment
from enbios.const import BASE_TEST_DATA_PATH
from enbios.generic.files import ReadPath
from enbios.base.models import ExperimentData

try:
    from test.enbios.test_project_fixture import TEST_BW_PROJECT, BRIGHTWAY_ADAPTER_MODULE_PATH
except ImportError:
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
        try:
            yield ReadPath(file).read_data()
        except FileNotFoundError as err:
            raise err

def experiments_data_configures():
    for experiment_data in experiments_data():
        if replace_conf := experiment_data.get("config", {}).get("debug_test_replace_bw_config", True):
            if isinstance(replace_conf, list):
                fix_experiment_data(experiment_data, *replace_conf)
            else:
                fix_experiment_data(experiment_data, TEST_BW_PROJECT, BRIGHTWAY_ADAPTER_MODULE_PATH)
        yield experiment_data


def experiment_data_file_names():
    for file in (BASE_TEST_DATA_PATH / "experiment_instances").glob("*.json"):
        yield file.name


def fix_experiment_data(data: dict, bw_project: str, module_path: str):
    for adapter in data["adapters"]:
        if adapter.get("note") == "brightway-adapter":
            adapter["config"]["bw_project"] = bw_project
            adapter["module_path"] = module_path


@pytest.mark.parametrize('experiment_data', argvalues=experiments_data_configures(), ids=experiment_data_file_names())
def test_experiment_data(experiment_data):
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
    filename = "a2x.json"
    experiment_data = ReadPath(BASE_TEST_DATA_PATH / "experiment_instances" / filename).read_data()
    if replace_conf := experiment_data.get("config", {}).get("debug_test_replace_bw_config", True):
        if isinstance(replace_conf, list):
            fix_experiment_data(experiment_data, *replace_conf)
        else:
            fix_experiment_data(experiment_data, TEST_BW_PROJECT, BRIGHTWAY_ADAPTER_MODULE_PATH)
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


def test_env_config(tempfolder: Path):
    import os
    with pytest.raises(Exception):
        del os.environ["CONFIG_FILE"]
        Experiment()

    experiment_data = ReadPath(BASE_TEST_DATA_PATH / "experiment_instances/scenario3.json").read_data()
    fix_experiment_data(experiment_data, TEST_BW_PROJECT, BRIGHTWAY_ADAPTER_MODULE_PATH)
    temp_env_file = Path(tempfolder / "env_config.json")
    json.dump(experiment_data, temp_env_file.open("w", encoding="utf-8"))
    os.environ["CONFIG_FILE"] = temp_env_file.as_posix()
    Experiment()


# def test_run_scenarios_env_setting():
#     # Test case 1: run_scenarios is None
#     experiment = Experiment(experiment_data)
#     assert experiment.config.run_scenarios is None
#
#     # Test case 2: run_scenarios is an empty list
#     experiment_data = ExperimentData(run_scenarios=[])
#     experiment = Experiment(experiment_data)
#     assert experiment.config.run_scenarios == []
#
#     # Test case 3: run_scenarios contains a scenario that does not exist
#     experiment_data = ExperimentData(run_scenarios=["non_existent_scenario"])
#     with pytest.raises(ValueError):
#         experiment = Experiment(experiment_data)
#
#     # Test case 4: run_scenarios contains a scenario that exists
#     experiment_data = ExperimentData(run_scenarios=["existing_scenario"])
#     experiment = Experiment(experiment_data)
#     assert "existing_scenario" in experiment.config.run_scenarios
#
# def test_run_scenarios_env_setting():
#     # Test case 5: RUN_SCENARIOS environment variable is set
#     os.environ["RUN_SCENARIOS"] = "env_scenario"
#     experiment_data = ExperimentData(run_scenarios=["existing_scenario"])
#     experiment = Experiment(experiment_data)
#     assert experiment.config.run_scenarios == ["env_scenario"]
#     del os.environ["RUN_SCENARIOS"]

def test_repr():
    pass


def test_info():
    pass


def test_csv_setup():
    pass
