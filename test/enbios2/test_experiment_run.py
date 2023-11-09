import csv
import json
import pickle
from pathlib import Path
from typing import Generator

import pytest
from bw2data.backends import Activity
from deprecated.classic import deprecated

from enbios.base.experiment import Experiment, ScenarioResultNodeData
from enbios.const import BASE_DATA_PATH, BASE_TEST_DATA_PATH
from enbios.generic.files import ReadPath
from enbios.generic.tree.basic_tree import BasicTreeNode
from enbios.models.experiment_models import ExperimentData
from test.enbios2.test_project_fixture import TEST_BW_PROJECT, TEST_BW_DATABASE


@pytest.fixture
def default_method_tuple() -> tuple:
    return 'EDIP 2003 no LT', 'non-renewable resources no LT', 'zinc no LT'


@pytest.fixture
def default_method_str(default_method_tuple) -> str:
    return "_".join(default_method_tuple)


@pytest.fixture
def default_result_score() -> float:
    return 6.169154864577996e-06


@pytest.fixture
def first_activity_id() -> dict:
    return {
        "name": "heat and power co-generation, wood chips, 6667 kW, state-of-the-art 2014",
        "unit": "kilowatt hour",
        "location": "DK"
    }


@pytest.fixture
def second_activity_id() -> dict:
    return {
        "name": "concentrated solar power plant construction, solar tower power plant, 20 MW",
        "code": "19978cf531d88e55aed33574e1087d78"
    }


@pytest.fixture
def default_bw_activity(default_bw_config, first_activity_id) -> Activity:
    import bw2data
    bw2data.projects.set_current(default_bw_config["bw_project"])
    db = bw2data.Database(default_bw_config["bw_default_database"])
    return next(filter(lambda act: act["unit"] == "kilowatt hour",
                       db.search(
                           first_activity_id["name"],
                           filter = {"location": first_activity_id["location"]})))


@pytest.fixture
def experiment_setup(default_bw_config, default_bw_activity, default_method_tuple,
                     default_method_str: str,
                     default_result_score: float):
    _impact = default_result_score
    return {
        "scenario": {
            "bw_project": default_bw_config["bw_project"],
            "bw_default_database": default_bw_config["bw_default_database"],
            "activities": {
                "single_activity": {
                    "id": {
                        "name": default_bw_activity["name"],
                        "unit": default_bw_activity["unit"],
                        "location": default_bw_activity["location"]
                    },
                    "output": [
                        "kWh",
                        1
                    ]
                }
            },
            "methods": [
                {
                    "id": default_method_tuple
                }
            ],
            "hierarchy": {
                "energy": [
                    "single_activity"
                ]
            }
        },
        "expected_result_tree": {'name': 'root',
                                 'children': [{'name': 'energy',
                                               'children': [
                                                   {'name': 'single_activity',
                                                    'children': [],
                                                    'data':
                                                        ScenarioResultNodeData(
                                                            output = (
                                                                "kilowatt_hour", 1.0),
                                                            bw_activity = default_bw_activity,
                                                            results = {
                                                                default_method_str: _impact})}],
                                               'data': ScenarioResultNodeData(
                                                   output = ("kilowatt_hour", 1.0),
                                                   results = {
                                                       default_method_str: _impact})}],
                                 'data': ScenarioResultNodeData(
                                     output = ("kilowatt_hour", 1.0),
                                     results = {
                                         default_method_str: _impact})}
    }


@pytest.fixture
def experiment_scenario_setup(default_bw_config, default_bw_activity, first_activity_id,
                              default_method_tuple, default_method_str: str):
    return {
        "bw_project": default_bw_config["bw_project"],
        "bw_default_database": default_bw_config["bw_default_database"],
        "activities": {
            "single_activity": {
                "id": first_activity_id
            }
        },
        "methods": [
            {
                "id": default_method_tuple
            }
        ],
        "hierarchy": {
            "energy": [
                "single_activity"
            ]
        },
        "scenarios": {
            "scenario1": {
                "activities": {
                    "single_activity": [
                        "kWh",
                        1
                    ]
                }
            },
            "scenario2": {
                "activities": {
                    "single_activity": [
                        "MWh",
                        2
                    ]
                }
            }
        }
    }


@deprecated(reason = "Use test_project_fixture")
@pytest.fixture
def default_bw_config() -> dict:
    return {
        "bw_project": TEST_BW_PROJECT,
        "bw_default_database": TEST_BW_DATABASE
    }


@pytest.fixture
def temp_csv_file() -> Generator[Path, None, None]:
    path = BASE_DATA_PATH / "temp/test_csv.csv"
    if path.exists():
        path.unlink()
    yield path
    path.unlink()


@pytest.fixture
def temp_json_file() -> Generator[Path, None, None]:
    path = BASE_DATA_PATH / "temp/test_json.json"
    if path.exists():
        path.unlink()
    yield path
    path.unlink()


@pytest.fixture
def run_basic_experiment(experiment_setup) -> Experiment:
    scenario_data = experiment_setup["scenario"]
    experiment = Experiment(ExperimentData(**scenario_data))
    experiment.run()
    return experiment


@pytest.fixture
def basic_exp_run_result_tree(run_basic_experiment) -> BasicTreeNode[
    ScenarioResultNodeData]:
    return run_basic_experiment.get_scenario(
        Experiment.DEFAULT_SCENARIO_ALIAS).result_tree


def test_single_lca_compare(run_basic_experiment: Experiment,
                            basic_exp_run_result_tree,
                            experiment_setup,
                            default_method_tuple,
                            default_method_str):
    expected_value = experiment_setup["expected_result_tree"]["data"].results[
        default_method_str]
    bw_activity = run_basic_experiment._activities["single_activity"].bw_activity
    regular_score = bw_activity.lca(default_method_tuple).score
    assert regular_score == pytest.approx(expected_value, abs = 1e-6)
    assert regular_score == basic_exp_run_result_tree.data.results[default_method_str]
    # assert regular_score == result["default scenario"]["data"][default_method_str]


def test_simple(basic_exp_run_result_tree, experiment_setup):
    assert basic_exp_run_result_tree.as_dict(True) == experiment_setup[
        "expected_result_tree"]


def test_pickle(run_basic_experiment):
    pickle.dump(run_basic_experiment,
                open(BASE_DATA_PATH / "temp/test_pickle.pickle", "wb"))


def test_temp_load_pickle():
    experiment = pickle.load(open(BASE_DATA_PATH / "temp/test_pickle.pickle", "rb"))
    assert experiment


def test_csv_output(run_basic_experiment, temp_csv_file):
    run_basic_experiment.results_to_csv(temp_csv_file, Experiment.DEFAULT_SCENARIO_ALIAS,
                                        include_method_units = False)
    assert temp_csv_file.exists()
    csv_data = ReadPath(temp_csv_file).read_data()
    expected_data = list(
        csv.DictReader((
                               BASE_TEST_DATA_PATH / "experiment_instances_run" / "test_csv_output.csv").open()))
    assert csv_data == expected_data
    # todo we could try other hierarchies here and include the methods units again


def test_dict_output(run_basic_experiment):
    json_data = run_basic_experiment.get_scenario(
        Experiment.DEFAULT_SCENARIO_ALIAS).result_to_dict()
    expected_json_data = json.load((
                                           BASE_TEST_DATA_PATH / "experiment_instances_run" / "test_json_output.json").open())
    assert json_data == expected_json_data
    json_data = run_basic_experiment.scenarios[0].result_to_dict(False)
    expected_json_data = json.load(
        (
                BASE_TEST_DATA_PATH / "experiment_instances_run" / "test_json_output_no_output.json").open())
    assert json_data == expected_json_data


def test_scaled_demand(experiment_setup, default_method_str: str):
    scale = 3
    scenario_data = experiment_setup["scenario"]
    scenario_data["activities"]["single_activity"]["output"] = ["kWh", scale]
    expected_tree = experiment_setup["expected_result_tree"]
    expected_value = expected_tree["data"].results[default_method_str] * scale
    result = Experiment(ExperimentData(**scenario_data)).run()
    assert result[Experiment.DEFAULT_SCENARIO_ALIAS]._data.results[
               default_method_str] == pytest.approx(
        expected_value, abs = 1e-10)


def test_scaled_demand_unit(experiment_setup, default_method_str: str):
    scenario_data = experiment_setup["scenario"]
    scenario_data["activities"]["single_activity"]["output"] = ["MWh", 3]
    expected_tree = experiment_setup["expected_result_tree"]
    expected_value = expected_tree["data"].results[default_method_str] * 3000
    result = Experiment(ExperimentData(**scenario_data)).run()
    # print(result["default scenario"]["data"][method])
    # print(result["default scenario"]["data"][method] / expected_value)
    assert result[Experiment.DEFAULT_SCENARIO_ALIAS]._data.results[
               default_method_str] == pytest.approx(
        expected_value, abs = 1e-7)


def test_stacked_lca(default_bw_config, default_method_tuple, first_activity_id,
                     second_activity_id):
    """
    {
        "id": ["Cumulative Exergy Demand (CExD)", "energy resources: renewable, solar", "exergy content"]
    }
    """
    # todo this test should be vigorous
    experiment = {
        "bw_project": default_bw_config["bw_project"],
        "bw_default_database": default_bw_config["bw_default_database"],
        "activities": {
            "single_activity": {
                "id": first_activity_id,
                "output": [
                    "kWh",
                    1
                ]
            },
            "2nd": {
                "id": second_activity_id,
                "output": [
                    "unit",
                    1
                ]
            }
        },
        "methods": [
            {
                "id": default_method_tuple
            }
        ],
        "hierarchy": {
            "energy": [
                "single_activity",
                "2nd"
            ]
        },
        "scenarios": [
            {
                "activities": {"single_activity": ["kWh", 3]}
            },
            {
                "activities": {"single_activity": ["kWh", 4]}
            },
            {
                "activities": {"single_activity": ["kWh", 5]}
            }
        ]
    }
    Experiment(ExperimentData(**experiment)).run()


def test_scenario(experiment_scenario_setup: dict,
                  experiment_setup,
                  default_bw_config: dict,
                  default_method_tuple,
                  default_method_str: str,
                  temp_csv_file: Path):
    experiment = Experiment(ExperimentData(**experiment_scenario_setup))
    result = experiment.run()
    assert "scenario1" in result and "scenario2" in result
    expected_value1 = experiment_setup["expected_result_tree"]["data"].results[
        default_method_str]
    assert result["scenario1"].data.results[default_method_str] == expected_value1
    expected_value2 = expected_value1 * 2000  # from 1KWh to 2MWh
    assert result["scenario2"].data.results[default_method_str] == pytest.approx(
        expected_value2, abs = 1e-9)
    #   todo test, complete experiment csv
    experiment.results_to_csv(temp_csv_file)


#    print(temp_csv_file.read_text())

def test_multi_activity_usage(experiment_setup, default_bw_config: dict,
                              default_method_tuple: tuple[str, ...]):
    scenario = {
        "bw_project": default_bw_config["bw_project"],
        "bw_default_database": default_bw_config["bw_default_database"],
        "activities": {
            "single_activity": {
                "id": {
                    "name": "heat and power co-generation, wood chips, 6667 kW, state-of-the-art 2014",
                    "unit": "kilowatt hour",
                    "location": "DK"
                }
            },
            "single_activity_2": {
                "id": {
                    "name": "heat and power co-generation, wood chips, 6667 kW, state-of-the-art 2014",
                    "unit": "kilowatt hour",
                    "location": "DK"
                }
            }
        },
        "methods": [
            {
                "id": default_method_tuple
            }
        ],
        "scenarios": {
            "scenario1": {
                "activities": {
                    "single_activity": [
                        "kWh",
                        1
                    ]
                }
            },
            "scenario2": {
                "activities": {
                    "single_activity": [
                        "MWh",
                        2
                    ],
                    "single_activity_2": [
                        "kWh",
                        20
                    ]
                }
            }
        }
    }
    exp = Experiment(scenario)
    exp.run()
    method_str = "_".join(default_method_tuple)
    expected_value1 = experiment_setup["expected_result_tree"]["data"].results[method_str]
    assert expected_value1 == exp.scenarios[0].result_tree[0]._data.results[method_str]
    # scenario 2, single_activity
    expected_value2 = expected_value1 * 2000  # from 1KWh to 2MWh
    assert exp.scenarios[1].result_tree[0]._data.results[method_str] == pytest.approx(
        expected_value2, abs = 1e-12)
    # scenario 2, single_activity_2
    expected_value3 = expected_value1 * 20
    assert exp.scenarios[1].result_tree[1]._data.results[method_str] == pytest.approx(
        expected_value3, abs = 1e-14)
    # scenario 2, total
    expected_value4 = expected_value2 + expected_value3
    assert exp.scenarios[1].result_tree._data.results[method_str] == pytest.approx(
        expected_value4, abs = 1e-12)


def test_lca_distribution(experiment_setup,
                          default_method_tuple,
                          second_activity_id,
                          default_method_str):
    scenario_data = experiment_setup["scenario"]
    scenario_data["config"] = {
        "use_k_bw_distributions": 3,
    }
    experiment = Experiment(ExperimentData(**scenario_data))
    # experiment.run()
    # result["default scenario"].children
    pass
    scenario_data["activities"]["2nd"] = {
        "id": second_activity_id
    }
    scenario_data["hierarchy"]["energy"].append("2nd")

    experiment = Experiment(ExperimentData(**scenario_data))
    # result = experiment.run()
    pass
    # todo passes, but the activities have such high differences, that it is hard
    # to see that they are summed up.
    scenario_data["scenarios"] = [
        {"activities": {"single_activity": ["kWh", 3],
                        "2nd": ["unit", 2]}

         }
    ]
    experiment = Experiment(ExperimentData(**scenario_data))
    result = experiment.scenarios[0].run()
    pass
