import csv
import json
import pickle
from dataclasses import asdict
from pathlib import Path
from typing import Generator

import pytest
from bw2data.backends import Activity

from enbios.base.experiment import Experiment, ScenarioResultNodeData
from enbios.bw2.brightway_experiment_adapter import BrightwayAdapter
from enbios.generic.files import ReadPath
from enbios.generic.tree.basic_tree import BasicTreeNode
from enbios.models.experiment_models import ResultValue
from test.enbios.conftest import tempfolder
from test.enbios.test_project_fixture import TEST_BW_PROJECT, TEST_BW_DATABASE, BRIGHTWAY_ADAPTER_MODULE_PATH


@pytest.fixture
def default_method_tuple() -> tuple:
    return 'EDIP 2003 no LT', 'non-renewable resources no LT', 'zinc no LT'


@pytest.fixture
def default_method_str(default_method_tuple) -> str:
    return "_".join(default_method_tuple)


@pytest.fixture
def default_result_score() -> float:
    return 6.16915484407017e-06


@pytest.fixture
def first_activity_config() -> dict:
    return {
        "name": "heat and power co-generation, wood chips, 6667 kW, state-of-the-art 2014",
        "unit": "kilowatt hour",
        "location": "DK",
        "default_output": {
            "unit": "kWh",
            "magnitude": 1
        }
    }


@pytest.fixture
def second_activity_config() -> dict:
    return {
        "name": "concentrated solar power plant construction, solar tower power plant, 20 MW",
        "code": "19978cf531d88e55aed33574e1087d78"
    }


@pytest.fixture
def default_bw_method_name() -> str:
    return "zinc_no_LT"


@pytest.fixture
def bw_adapter_config(default_bw_config: dict, default_method_tuple: tuple, default_bw_method_name: str) -> dict:
    return {
        "module_path": default_bw_config["bw_module_path"],
        "config": {
            "bw_project": default_bw_config["bw_project"],
            "bw_default_database": default_bw_config["bw_default_database"]
        },
        "methods": {
            default_bw_method_name: default_method_tuple
        },
        "note": "brightway-adapter"
    }


@pytest.fixture
def default_bw_activity(default_bw_config, first_activity_config) -> Activity:
    import bw2data
    bw2data.projects.set_current(default_bw_config["bw_project"])
    db = bw2data.Database(default_bw_config["bw_default_database"])
    return next(filter(lambda act: act["unit"] == "kilowatt hour",
                       db.search(
                           first_activity_config["name"],
                           filter={"location": first_activity_config["location"]})))


@pytest.fixture
def experiment_setup(bw_adapter_config, default_result_score: float, first_activity_config: dict,
                     default_bw_method_name: str) -> dict:
    _impact = default_result_score
    return {
        "scenario": {
            "adapters": [
                bw_adapter_config
            ],
            "hierarchy": {
                "name": "root",
                "aggregator": "sum",
                "children": [
                    {
                        "name": "single_activity",
                        "adapter": "bw",
                        "config": first_activity_config,
                    }
                ]
            },
            "config": {
                "run_adapters_concurrently": False
            }
        },
        "expected_result_tree": {'name': 'root',
                                 'children': [
                                     {'name': 'single_activity',
                                      'children': [],
                                      'data':
                                          ScenarioResultNodeData(
                                              output=(
                                                  "kilowatt_hour", 1.0),
                                              adapter="bw",
                                              aggregator=None,
                                              results={
                                                  default_bw_method_name: ResultValue(unit="kilogram",
                                                                                      amount=_impact)})}],
                                 'data': ScenarioResultNodeData(
                                     output=("kilowatt_hour", 1.0),
                                     adapter=None,
                                     aggregator="sum",
                                     results={
                                         default_bw_method_name: ResultValue(unit="kilogram", amount=_impact)})}
    }


@pytest.fixture
def experiment_scenario_setup(bw_adapter_config, first_activity_config):
    return {
        "adapters": [
            bw_adapter_config
        ],
        "hierarchy": {
            "name": "root",
            "aggregator": "sum",
            "children": [
                {
                    "name": "single_activity",
                    "adapter": "bw",
                    "config": first_activity_config,
                }
            ]
        },
        "scenarios": [
            {
                "name": "scenario1",
                "activities": {
                    "single_activity": {
                        "unit": "kWh",
                        "magnitude": 1
                    }
                }
            },
            {
                "name": "scenario2",
                "activities": {
                    "single_activity": {
                        "unit": "MWh",
                        "magnitude": 2
                    }
                }
            }]
    }


@pytest.fixture
def default_bw_config() -> dict:
    return {
        "bw_project": TEST_BW_PROJECT,
        "bw_default_database": TEST_BW_DATABASE,
        "bw_module_path": BRIGHTWAY_ADAPTER_MODULE_PATH
    }


@pytest.fixture
def temp_csv_file(tempfolder: Path) -> Generator[Path, None, None]:
    path = tempfolder / "test_csv.csv"
    if path.exists():
        path.unlink()
    yield path
    path.unlink()


@pytest.fixture
def temp_json_file(tempfolder: Path) -> Generator[Path, None, None]:
    path = tempfolder / "test_json.json"
    if path.exists():
        path.unlink()
    yield path
    path.unlink()


@pytest.fixture
def run_basic_experiment(experiment_setup) -> Experiment:
    scenario_data = experiment_setup["scenario"]
    experiment = Experiment(scenario_data)
    experiment.run()
    return experiment


def test_write_dict(run_basic_experiment: Experiment, tempfolder: Path):
    scenario = run_basic_experiment.get_scenario(Experiment.DEFAULT_SCENARIO_NAME)
    json.dump(scenario.result_to_dict(),
              (tempfolder / "test_json_output.json").open("w"), indent=2)
    json.dump(scenario.result_to_dict(False),
              (tempfolder / "test_json_output_no_output.json").open("w"), indent=2)


@pytest.fixture
def basic_exp_run_result_tree(run_basic_experiment) -> BasicTreeNode[ScenarioResultNodeData]:
    return run_basic_experiment.get_scenario(
        Experiment.DEFAULT_SCENARIO_NAME).result_tree


def test_single_lca_compare(run_basic_experiment: Experiment,
                            basic_exp_run_result_tree: BasicTreeNode[ScenarioResultNodeData],
                            experiment_setup: dict,
                            default_bw_method_name: str,
                            default_method_tuple):
    expected_value = experiment_setup["expected_result_tree"]["data"].results[default_bw_method_name]
    activity = run_basic_experiment.get_activity("single_activity")
    bw_adapter: BrightwayAdapter = run_basic_experiment.get_activity_adapter(activity)
    bw_activity = bw_adapter.activityMap["single_activity"].bw_activity
    regular_score = bw_activity.lca(default_method_tuple).score
    assert regular_score == pytest.approx(expected_value.amount, abs=0)  # abs=1e-6
    assert regular_score == basic_exp_run_result_tree.data.results[default_bw_method_name].amount


def test_simple(basic_exp_run_result_tree, experiment_setup):
    assert basic_exp_run_result_tree.as_dict(True) == experiment_setup[
        "expected_result_tree"]


def test_pickle(run_basic_experiment, tempfolder):
    pickle.dump(run_basic_experiment,
                open(tempfolder / "test_pickle.pickle", "wb"))


def test_temp_load_pickle(tempfolder: Path):
    experiment = pickle.load(open(tempfolder / "test_pickle.pickle", "rb"))
    assert experiment


def test_csv_output(run_basic_experiment, temp_csv_file: Path):
    run_basic_experiment.results_to_csv(temp_csv_file, Experiment.DEFAULT_SCENARIO_NAME,
                                        include_method_units=False)
    assert temp_csv_file.exists()
    csv_data = ReadPath(temp_csv_file).read_data()
    # todo this does not really check the content...
    expected_data = list(
        csv.DictReader(temp_csv_file.open()))
    assert csv_data == expected_data
    pass
    # todo we could try other hierarchies here and include the methods units again


def test_dict_output(run_basic_experiment, tempfolder: Path):
    scenario = run_basic_experiment.get_scenario(Experiment.DEFAULT_SCENARIO_NAME)
    json_data = scenario.result_to_dict()
    expected_json_data = json.load((tempfolder / "test_json_output.json").open())
    assert json_data == expected_json_data
    json_data = scenario.result_to_dict(False)
    expected_json_data = json.load((tempfolder / "test_json_output_no_output.json").open())
    assert json_data == expected_json_data


def test_scaled_demand(experiment_setup: dict, default_bw_method_name: str):
    scale = 3
    scenario_data = experiment_setup["scenario"]
    scenario_data["hierarchy"]["children"][0]["config"]["default_output"] = {"unit": "kWh", "magnitude": scale}
    expected_tree = experiment_setup["expected_result_tree"]
    expected_value = expected_tree["data"].results[default_bw_method_name].amount * scale
    result = Experiment(scenario_data).run()
    assert result[Experiment.DEFAULT_SCENARIO_NAME]["results"][default_bw_method_name]["amount"] == pytest.approx(
        expected_value,
        abs=1e-8)  # 1e-10


def test_scaled_demand_unit(experiment_setup, default_bw_method_name: str):
    scenario_data = experiment_setup["scenario"]
    scenario_data["hierarchy"]["children"][0]["config"]["default_output"] = {"unit": "MWh", "magnitude": 3}
    expected_tree = experiment_setup["expected_result_tree"]
    expected_value = expected_tree["data"].results[default_bw_method_name].amount * 3000
    result = Experiment(scenario_data).run()
    assert result[Experiment.DEFAULT_SCENARIO_NAME]["results"][default_bw_method_name]["amount"] == pytest.approx(
        expected_value, abs=1e-7)


def test_stacked_lca(bw_adapter_config, first_activity_config, second_activity_config):
    # todo this test should be vigorous
    experiment = {
        "adapters": [
            bw_adapter_config
        ],
        "hierarchy": {
            "name": "root",
            "aggregator": "sum",
            "children": [{
                "name": "single_activity",
                "config": first_activity_config,
                "adapter": "bw",
                "output": {
                    "unit": "kWh",
                    "magnitude": 1
                }
            }, {
                "name": "2nd",
                "config": second_activity_config,
                "adapter": "bw",
                "output": {
                    "unit": "unit",
                    "magnitude": 1
                }
            }]
        },
        "scenarios": [
            {
                "activities": {"single_activity": {"unit": "kWh", "magnitude": 3}}
            },
            {
                "activities": {"single_activity": {"unit": "kWh", "magnitude": 4}}
            },
            {
                "activities": {"single_activity": {"unit": "kWh", "magnitude": 5}}
            }
        ]
    }
    exp = Experiment(experiment)
    exp.run()
    pass


def test_scenario(experiment_scenario_setup: dict,
                  experiment_setup: dict,
                  default_bw_method_name: str,
                  temp_csv_file: Path):
    experiment = Experiment(experiment_scenario_setup)
    result = experiment.run()
    assert "scenario1" in result and "scenario2" in result
    expected_value1 = experiment_setup["expected_result_tree"]["data"].results[default_bw_method_name]
    assert result["scenario1"]["results"][default_bw_method_name] == asdict(expected_value1)
    expected_value2 = expected_value1.amount * 2000  # from 1KWh to 2MWh
    assert result["scenario2"]["results"][default_bw_method_name]["amount"] == pytest.approx(
        expected_value2, abs=1e-9)
    #   todo test, complete experiment csv
    experiment.results_to_csv(temp_csv_file)


def test_multi_activity_usage(bw_adapter_config: dict, first_activity_config: dict, experiment_setup,
                              default_bw_config: dict,
                              default_bw_method_name: str,
                              default_method_tuple: tuple[str, ...]):
    scenario = {
        "adapters": [bw_adapter_config],
        "hierarchy": {
            "name": "root",
            "aggregator": "sum",
            "children": [
                {
                    "name": "single_activity",
                    "config": first_activity_config,
                    "adapter": "bw"
                },
                {
                    "name": "single_activity_2",
                    "config": first_activity_config,
                    "adapter": "bw"
                }
            ]
        },
        "scenarios": [
            {
                "name": "scenario1",
                "activities": {
                    "single_activity": {
                        "unit": "kWh",
                        "magnitude": 1
                    }
                }
            },
            {
                "name": "scenario2",
                "activities": {
                    "single_activity": {
                        "unit": "MWh",
                        "magnitude": 2
                    },
                    "single_activity_2": {
                        "unit": "kWh",
                        "magnitude": 20
                    }
                }
            },
            {
                "name": "scenario3",
                "config": {
                    "exclude_defaults": True
                },
                "activities": {
                    "single_activity": {
                        "unit": "kWh",
                        "magnitude": 1
                    }
                }
            },
        ]
    }
    exp = Experiment(scenario)
    exp.run()
    # scenario 1, single_activity
    expected_value1 = experiment_setup["expected_result_tree"]["data"].results['zinc_no_LT']
    assert expected_value1 == exp.scenarios[0].result_tree[0].data.results[default_bw_method_name]
    # scenario 1, total (around double of single_activity)
    assert exp.scenarios[0].result_tree.data.results[default_bw_method_name].amount == expected_value1.amount * 2
    # scenario 2, single_activity
    expected_value2 = expected_value1.amount * 2000  # from 1KWh to 2MWh
    assert exp.scenarios[1].result_tree[0].data.results[default_bw_method_name].amount == pytest.approx(
        expected_value2, abs=1e-10)
    # scenario 2, single_activity_2
    expected_value3 = expected_value1.amount * 20
    assert exp.scenarios[1].result_tree[1].data.results[default_bw_method_name].amount == pytest.approx(
        expected_value3, abs=1e-10)
    # scenario 2, total
    expected_value4 = expected_value2 + expected_value3
    assert exp.scenarios[1].result_tree.data.results[default_bw_method_name].amount == pytest.approx(
        expected_value4, abs=1e-9)
    # scenario 3, exclude_defaults
    sce2_res = exp.scenarios[2].result_tree
    assert len(sce2_res.children) == 1
    assert sce2_res.data.results[default_bw_method_name].amount == expected_value1.amount


def test_lca_distribution(experiment_setup,
                          first_activity_config: dict,
                          second_activity_config):
    scenario_data = experiment_setup["scenario"]
    scenario_data["adapters"][0]["config"]["use_k_bw_distributions"] = 3
    experiment = Experiment(scenario_data)
    # experiment.run()
    scenario_data["hierarchy"]["children"].append(
        {
            "name": "single_activity_2",
            "config": first_activity_config,
            "adapter": "bw"
        }
    )

    experiment = Experiment(scenario_data)
    result = experiment.run()
    pass
