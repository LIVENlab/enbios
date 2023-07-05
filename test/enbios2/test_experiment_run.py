import pytest

from enbios2.base.experiment import Experiment
from enbios2.models.experiment_models import ExperimentData
from test.enbios2.test_project_fixture import TEST_BW_PROJECT, TEST_BW_DATABASE


@pytest.fixture
def scenario_run_basic1(default_bw_config):
    return {
        "scenario": {
            "bw_project": default_bw_config["bw_project"],
            "bw_default_database": default_bw_config["bw_default_database"],
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
                }
            },
            "methods": [
                {
                    "id": ('EDIP 2003 no LT', 'non-renewable resources no LT', 'zinc no LT')
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
                                                    'data': {
                                                        'EDIP 2003 no LT_non-renewable resources no LT_zinc no LT': 6.169154556662401e-06}}],
                                               'data': {
                                                   'EDIP 2003 no LT_non-renewable resources no LT_zinc no LT': 6.169154556662401e-06}}],
                                 'data': {
                                     'EDIP 2003 no LT_non-renewable resources no LT_zinc no LT': 6.169154556662401e-06}}
    }


@pytest.fixture
def default_bw_config() -> dict:
    return {
        "bw_project": TEST_BW_PROJECT,
        "bw_default_database": TEST_BW_DATABASE
    }


@pytest.fixture
def default_method_str() -> str:
    return 'EDIP 2003 no LT_non-renewable resources no LT_zinc no LT'


@pytest.fixture
def default_method_tuple() -> tuple:
    return 'EDIP 2003 no LT', 'non-renewable resources no LT', 'zinc no LT'


def test_single_lca_compare(scenario_run_basic1, default_method_tuple, default_method_str):
    experiment = Experiment(ExperimentData(**scenario_run_basic1["scenario"]))
    result = experiment.run()
    bw_activity = experiment.activitiesMap["single_activity"].bw_activity
    expected_value = scenario_run_basic1["expected_result_tree"]["data"][default_method_str]
    regular_score = bw_activity.lca(default_method_tuple).score
    assert regular_score == pytest.approx(expected_value, abs=1e-6)
    # assert regular_score == result["default scenario"]["data"][default_method_str]


def test_simple(scenario_run_basic1):
    scenario_data = scenario_run_basic1["scenario"]
    experiment = Experiment(ExperimentData(**scenario_data))
    result = experiment.run()

    assert result["default scenario"] == scenario_run_basic1["expected_result_tree"]


def test_scaled_demand(scenario_run_basic1, default_method_str: str):
    scale = 3
    scenario_data = scenario_run_basic1["scenario"]
    scenario_data["activities"]["single_activity"]["output"] = ["kWh", scale]
    expected_tree = scenario_run_basic1["expected_result_tree"]
    expected_value = expected_tree["data"][default_method_str] * scale
    result = Experiment(ExperimentData(**scenario_data)).run()
    # print(result["default scenario"]["data"][method])
    assert result["default scenario"]["data"].results[default_method_str] == pytest.approx(
        expected_value, abs=1e-10)


def test_scaled_demand_unit(scenario_run_basic1, default_method_str: str):
    scenario_data = scenario_run_basic1["scenario"]
    scenario_data["activities"]["single_activity"]["output"] = ["MWh", 3]
    expected_tree = scenario_run_basic1["expected_result_tree"]
    expected_value = expected_tree["data"][default_method_str] * 3000
    result = Experiment(ExperimentData(**scenario_data)).run()
    # print(result["default scenario"]["data"][method])
    # print(result["default scenario"]["data"][method] / expected_value)
    assert result["default scenario"]["data"].results[default_method_str] == pytest.approx(expected_value, abs=1e-7)


def test_scenario(scenario_run_basic1: dict, default_bw_config: dict, default_method_str: str):
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
                "id": ('EDIP 2003 no LT', 'non-renewable resources no LT', 'zinc no LT')
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
    expected_value1 = scenario_run_basic1["expected_result_tree"]["data"][default_method_str]
    assert result["scenario1"]["data"].results[default_method_str] == expected_value1
    expected_value2 = expected_value1 * 2000  # from 1KWh to 2MWh
    assert result["scenario2"]["data"].results[default_method_str] == pytest.approx(expected_value2, abs=1e-9)
