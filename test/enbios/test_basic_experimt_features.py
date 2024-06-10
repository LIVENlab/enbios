import json
from pathlib import Path

import pytest

from enbios import Experiment


def test_fail_get_structural_node(basic_experiment):
    # get_structural_node should fail (arbitrary node name)
    with pytest.raises(Exception):
        basic_experiment.get_node("arbitrary-node")


#  get_adapter_by_name should return the correct adapter
def test_get_adapter_by_name(basic_experiment):
    # get_adapter_by_name should return the correct adapter
    adapter = basic_experiment.get_adapter_by_name("bw")
    assert adapter is not None
    assert adapter.name() == "brightway-adapter"
    assert adapter == basic_experiment.get_adapter_by_name("brightway-adapter")


#  get_adapter_by_name should raise an exception for invalid adapter name
def test_fail_get_adapter_by_name(basic_experiment):
    # get_adapter_by_name should raise an exception for invalid adapter name
    with pytest.raises(Exception):
        basic_experiment.get_adapter_by_name("invalid-adapter")


#  Retrieving a non-existing structural node should raise an exception
def test_fail_get_non_existing_structural_node(basic_experiment):
    # get_structural_node should fail for a non-existing node name
    with pytest.raises(Exception):
        basic_experiment.get_node("non-existing-node")


#  get_scenario should return the correct scenario
def test_get_scenario(basic_experiment):
    # get_scenario should return the correct scenario
    assert basic_experiment.get_scenario(basic_experiment.DEFAULT_SCENARIO_NAME)


def test_fail_get_scenario(basic_experiment):
    # get_scenario  should fail (arbitrary adapter name)
    with pytest.raises(ValueError):
        assert basic_experiment.get_scenario("non-existing scenario")


def test_scenario_exec_time(experiment_scenario_setup: dict):
    experiment = Experiment(experiment_scenario_setup)
    assert experiment.execution_time
    experiment.run_scenario("scenario1")
    assert experiment.execution_time
    experiment.run_scenario("scenario2")
    assert experiment.execution_time

# def test_scenario_select(experiment_setup):
#     scenario_data = experiment_setup["scenario"]


def test_env_vars(experiment_setup, tempfolder: Path):
    # test fail
    with pytest.raises(Exception):
        _ = Experiment()

    # test success

    import os
    file_path = tempfolder / "setup.json"
    json.dump(experiment_setup["scenario"], file_path.open("w", encoding="utf-8"))
    os.environ["CONFIG_FILE"] = file_path.absolute().as_posix()
    _ = Experiment()
