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


def test_simple(scenario_run_basic1):
    scenario_data = scenario_run_basic1["scenario"]
    result = Experiment(ExperimentData(**scenario_data)).run()
    # print(result["default scenario"]["data"][('EDIP 2003 no LT', 'non-renewable resources no LT', 'zinc no LT')])
    assert result["default scenario"] == scenario_run_basic1["expected_result_tree"]


def test_scaled_demand(scenario_run_basic1):
    scale = 3
    scenario_data = scenario_run_basic1["scenario"]
    scenario_data["activities"]["single_activity"]["output"] = ["kWh", scale]
    expected_tree = scenario_run_basic1["expected_result_tree"]
    method = ('EDIP 2003 no LT', 'non-renewable resources no LT', 'zinc no LT')
    expected_value = expected_tree["data"][method] * scale
    result = Experiment(ExperimentData(**scenario_data)).run()
    # print(result["default scenario"]["data"][method])
    assert result["default scenario"]["data"][method] == pytest.approx(expected_value, abs=1e-10)


def test_scaled_demand_unit(scenario_run_basic1):
    scenario_data = scenario_run_basic1["scenario"]
    scenario_data["activities"]["single_activity"]["output"] = ["MWh", 3]
    expected_tree = scenario_run_basic1["expected_result_tree"]
    method = ('EDIP 2003 no LT', 'non-renewable resources no LT', 'zinc no LT')
    expected_value = expected_tree["data"][method] * 3000
    result = Experiment(ExperimentData(**scenario_data)).run()
    # print(result["default scenario"]["data"][method])
    # print(result["default scenario"]["data"][method] / expected_value)
    assert result["default scenario"]["data"][method] == pytest.approx(expected_value, abs=1e-5)
