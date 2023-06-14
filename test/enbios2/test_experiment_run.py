import pytest

from enbios2.base.experiment import Experiment
from enbios2.models.experiment_models import ExperimentData


def test_simple():
    data = {
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
                    "MWh",
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
    }

    expected_result_tree = {'name': 'root',
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

    result = Experiment(ExperimentData(**data)).run()
    assert result["default scenario"] == expected_result_tree

    method = ('EDIP 2003 no LT', 'non-renewable resources no LT', 'zinc no LT')
    # change the output
    data["activities"]["single_activity"]["output"] = ["MWh", 3]
    expected_value = result["default scenario"]["data"][method] * 3
    # run again, and compare with some tolerance
    result = Experiment(ExperimentData(**data)).run()
    assert result["default scenario"]["data"][method] == pytest.approx(expected_value, abs=1e-10)
