import pickle
from pathlib import Path

import pytest
from bw2data.backends import Activity
from deprecated.classic import deprecated

from enbios2.base.experiment import Experiment, ScenarioResultNodeData
from enbios2.const import BASE_DATA_PATH
from enbios2.generic.files import ReadPath
from enbios2.generic.tree.basic_tree import BasicTreeNode
from enbios2.models.experiment_models import ExperimentData
from test.enbios2.test_project_fixture import TEST_BW_PROJECT, TEST_BW_DATABASE


@pytest.fixture
def default_method_str() -> str:
    return 'EDIP 2003 no LT_non-renewable resources no LT_zinc no LT'


@pytest.fixture
def default_method_tuple() -> tuple:
    return 'EDIP 2003 no LT', 'non-renewable resources no LT', 'zinc no LT'


@pytest.fixture
def default_result_score() -> float:
    return 6.169154864577996e-06
    # return 6.292564044222079e-06


@pytest.fixture
def default_bw_activity(default_bw_config) -> Activity:
    import bw2data
    bw2data.projects.set_current(default_bw_config["bw_project"])
    db = bw2data.Database(default_bw_config["bw_default_database"])
    return next(filter(lambda act: act["unit"] == "kilowatt hour", db.search("heat and power co-generation, wood chips, 6667 kW, state-of-the-art 2014",
                     filter={"location": "DK"})))


@pytest.fixture
def scenario_run_basic1(default_bw_config, default_bw_activity, default_method_tuple, default_method_str: str, default_result_score: float):
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
                                                            output=("kilowatt_hour", 1.0),
                                                            bw_activity=default_bw_activity,
                                                            results={
                                                                default_method_str: _impact})}],
                                               'data': ScenarioResultNodeData(
                                                   output=("kilowatt_hour", 1.0),
                                                   results={
                                                       default_method_str: _impact})}],
                                 'data': ScenarioResultNodeData(
                                     output=("kilowatt_hour", 1.0),
                                     results={
                                         default_method_str: _impact})}
    }


@deprecated(reason="Use test_project_fixture")
@pytest.fixture
def default_bw_config() -> dict:
    return {
        "bw_project": TEST_BW_PROJECT,
        "bw_default_database": TEST_BW_DATABASE
    }


@pytest.fixture
def temp_csv_file() -> Path:
    path = BASE_DATA_PATH / "temp/test_csv.csv"
    if path.exists():
        path.unlink()
    return path


def test_single_lca_compare(scenario_run_basic1, default_method_tuple, default_method_str):
    experiment = Experiment(ExperimentData(**scenario_run_basic1["scenario"]))
    result = experiment.run()
    expected_value = scenario_run_basic1["expected_result_tree"]["data"].results[default_method_str]
    bw_activity = experiment._activities["single_activity"].bw_activity
    regular_score = bw_activity.lca(default_method_tuple).score
    assert regular_score == pytest.approx(expected_value, abs=1e-6)
    assert regular_score == result[Experiment.DEFAULT_SCENARIO_ALIAS].data.results[default_method_str]
    # assert regular_score == result["default scenario"]["data"][default_method_str]


def test_simple(scenario_run_basic1):
    scenario_data = scenario_run_basic1["scenario"]
    experiment = Experiment(ExperimentData(**scenario_data))
    results = experiment.run()[Experiment.DEFAULT_SCENARIO_ALIAS]
    assert results.as_dict(True) == scenario_run_basic1["expected_result_tree"]


def test_pickle(scenario_run_basic1):
    scenario_data = scenario_run_basic1["scenario"]
    experiment = Experiment(ExperimentData(**scenario_data))
    experiment.run()
    pickle.dump(experiment, open(BASE_DATA_PATH / "temp/test_pickle.pickle", "wb"))


def test_temp_load_pickle():
    experiment = pickle.load(open(BASE_DATA_PATH / "temp/test_pickle.pickle", "rb"))
    assert experiment


def test_csv_output(scenario_run_basic1, temp_csv_file, default_method_str, default_result_score):
    scenario_data = scenario_run_basic1["scenario"]
    experiment = Experiment(ExperimentData(**scenario_data))
    experiment.run()
    experiment.results_to_csv(temp_csv_file, Experiment.DEFAULT_SCENARIO_ALIAS, include_method_units=False)
    assert temp_csv_file.exists()
    csv_data = ReadPath(temp_csv_file).read_data()
    expected_data = [{'lvl0': 'root',
                      'lvl1': '',
                      'lvl2': '',
                      'unit': 'kilowatt_hour',
                      'amount': '1.0',
                      default_method_str: str(default_result_score)},
                     {'lvl0': '',
                      'lvl1': 'energy',
                      'lvl2': '',
                      'unit': 'kilowatt_hour',
                      'amount': '1.0',
                      default_method_str: str(default_result_score)},
                     {'lvl0': '',
                      'lvl1': '',
                      'lvl2': 'single_activity',
                      'unit': 'kilowatt_hour',
                      'amount': '1.0',
                      default_method_str: str(default_result_score)}]
    assert csv_data == expected_data
    # todo we could try other hierarchies here and include the methods units again


def test_scaled_demand(scenario_run_basic1, default_method_str: str):
    scale = 3
    scenario_data = scenario_run_basic1["scenario"]
    scenario_data["activities"]["single_activity"]["output"] = ["kWh", scale]
    expected_tree = scenario_run_basic1["expected_result_tree"]
    expected_value = expected_tree["data"].results[default_method_str] * scale
    result = Experiment(ExperimentData(**scenario_data)).run()
    # print(result["default scenario"]["data"][method])
    assert result[Experiment.DEFAULT_SCENARIO_ALIAS].data.results[default_method_str] == pytest.approx(
        expected_value, abs=1e-10)


def test_scaled_demand_unit(scenario_run_basic1, default_method_str: str):
    scenario_data = scenario_run_basic1["scenario"]
    scenario_data["activities"]["single_activity"]["output"] = ["MWh", 3]
    expected_tree = scenario_run_basic1["expected_result_tree"]
    expected_value = expected_tree["data"].results[default_method_str] * 3000
    result = Experiment(ExperimentData(**scenario_data)).run()
    # print(result["default scenario"]["data"][method])
    # print(result["default scenario"]["data"][method] / expected_value)
    assert result[Experiment.DEFAULT_SCENARIO_ALIAS].data.results[default_method_str] == pytest.approx(
        expected_value, abs=1e-7)


def test_stacked_lca():
    """
    ,
            {
                "id": ["Cumulative Exergy Demand (CExD)", "energy resources: renewable, solar", "exergy content"]
            }
    """
    # todo this test should be vigorous
    experiment = {
        "bw_project": "ecoinvent",
        "bw_default_database": "cutoff_3.9.1_default",
        "activities": {
            "single_activity": {
                "id": {
                    "name": "heat and power co-generation, wood chips, 6667 kW, state-of-the-art 2014",
                    "unit": "kilowatt hour",
                    "location": "DK"
                },
                "output": [
                    "kWh",
                    1
                ]
            },
            "2nd": {
                "id": {
                    "name": "concentrated solar power plant construction, solar tower power plant, 20 MW",
                    "code": "19978cf531d88e55aed33574e1087d78",
                    "database": "cutoff_3.9.1_default"
                },
                "output": [
                    "unit",
                    1
                ]
            }
        },
        "methods": [
            {
                "id": ['EDIP 2003 no LT', 'non-renewable resources no LT', 'zinc no LT']
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


def test_scenario(scenario_run_basic1: dict, default_bw_config: dict, default_method_tuple, default_method_str: str):
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

    result = Experiment(ExperimentData(**scenario)).run()
    assert "scenario1" in result and "scenario2" in result
    expected_value1 = scenario_run_basic1["expected_result_tree"]["data"].results[default_method_str]
    assert result["scenario1"].data.results[default_method_str] == expected_value1
    expected_value2 = expected_value1 * 2000  # from 1KWh to 2MWh
    assert result["scenario2"].data.results[default_method_str] == pytest.approx(expected_value2, abs=1e-9)


def test_multi_activity_usage(scenario_run_basic1, default_bw_config: dict, default_method_tuple: tuple[str, ...]):
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
    expected_value1 = scenario_run_basic1["expected_result_tree"]["data"].results[method_str]
    assert expected_value1 == exp.scenarios[0].result_tree[0].data.results[method_str]
    # scenario 2, single_activity
    expected_value2 = expected_value1 * 2000  # from 1KWh to 2MWh
    assert exp.scenarios[1].result_tree[0].data.results[method_str] == pytest.approx(expected_value2, abs=1e-12)
    # scenario 2, single_activity_2
    expected_value3 = expected_value1 * 20
    assert exp.scenarios[1].result_tree[1].data.results[method_str] == pytest.approx(expected_value3, abs=1e-14)
    # scenario 2, total
    expected_value4 = expected_value2 + expected_value3
    assert exp.scenarios[1].result_tree.data.results[method_str] == pytest.approx(expected_value4, abs=1e-12)
