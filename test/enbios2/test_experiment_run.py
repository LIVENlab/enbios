import pytest

from enbios2.base.experiment import Experiment
from enbios2.models.experiment_models import ExperimentData


@pytest.fixture
def scenario_run_basic1():
    return {
        "scenario": {
            "bw_project": "uab_bw_ei39",
            "activities_config": {
                "default_database": "ei391"
            },
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
                                                    'data': {('EDIP 2003 no LT',
                                                              'non-renewable resources no LT',
                                                              'zinc no LT'): 6.169154556662401e-06}}],
                                               'data': {('EDIP 2003 no LT',
                                                         'non-renewable resources no LT',
                                                         'zinc no LT'): 6.169154556662401e-06}}],
                                 'data': {('EDIP 2003 no LT',
                                           'non-renewable resources no LT',
                                           'zinc no LT'): 6.169154556662401e-06}}
    }


@pytest.fixture
def default_method() -> tuple:
    return 'EDIP 2003 no LT', 'non-renewable resources no LT', 'zinc no LT'


def test_single_lca_compare(scenario_run_basic1, default_method):
    experiment = Experiment(ExperimentData(**scenario_run_basic1["scenario"]))
    bw_activity = experiment.activitiesMap["single_activity"].bw_activity
    assert bw_activity.lca(default_method).score == scenario_run_basic1["expected_result_tree"]["data"][default_method]


def test_simple(scenario_run_basic1, default_method):
    scenario_data = scenario_run_basic1["scenario"]
    experiment = Experiment(ExperimentData(**scenario_data))
    result = experiment.run()
    # print(result["default scenario"]["data"][('EDIP 2003 no LT', 'non-renewable resources no LT', 'zinc no LT')])
    assert result["default scenario"] == scenario_run_basic1["expected_result_tree"]


def test_scaled_demand(scenario_run_basic1, default_method: tuple):
    scale = 3
    scenario_data = scenario_run_basic1["scenario"]
    scenario_data["activities"]["single_activity"]["output"] = ["kWh", scale]
    expected_tree = scenario_run_basic1["expected_result_tree"]
    expected_value = expected_tree["data"][default_method] * scale
    result = Experiment(ExperimentData(**scenario_data)).run()
    # print(result["default scenario"]["data"][method])
    assert result["default scenario"]["data"][default_method] == pytest.approx(expected_value, abs=1e-10)


def test_scaled_demand_unit(scenario_run_basic1, default_method):
    scenario_data = scenario_run_basic1["scenario"]
    scenario_data["activities"]["single_activity"]["output"] = ["MWh", 3]
    expected_tree = scenario_run_basic1["expected_result_tree"]
    expected_value = expected_tree["data"][default_method] * 3000
    result = Experiment(ExperimentData(**scenario_data)).run()
    # print(result["default scenario"]["data"][method])
    # print(result["default scenario"]["data"][method] / expected_value)
    assert result["default scenario"]["data"][default_method] == pytest.approx(expected_value, abs=1e-5)


def test_scenario(scenario_run_basic1, default_method):
    scenario = {
        "bw_project": "uab_bw_ei39",
        "activities_config": {
            "default_database": "ei391"
        },
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
    expected_value1 = scenario_run_basic1["expected_result_tree"]["data"][default_method]
    assert result["scenario1"]["data"][default_method] == expected_value1
    expected_value2 = expected_value1 * 2000  # from 1KWh to 2MWh
    assert result["scenario2"]["data"][default_method] == pytest.approx(expected_value2, abs=1e-9)
