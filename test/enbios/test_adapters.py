from pathlib import Path
from typing import cast

import pytest

from enbios.base.adapters_aggregators.builtin import SimpleAssignmentAdapter
from enbios.base.experiment import Experiment
from enbios.const import BASE_TEST_DATA_PATH


def test_simple_assignment_adapter():
    data = {
        "adapters": [
            {
                "adapter_name": "simple-assignment-adapter",
                "config": {},
                "methods": {
                    "test": "co2"
                }
            }
        ],
        "hierarchy": {
            "name": "root",
            "aggregator": "sum",
            "children": [
                {
                    "name": "test",
                    "adapter": "assign",
                    "config": {
                        "output_unit": "kg",
                        "default_output": {
                            "unit": "kg",
                            "magnitude": 1.2
                        },
                        "default_impacts": {
                            "test": {
                                "unit": "co2",
                                "magnitude": 31.254
                            }
                        }
                    }
                }
            ]
        }
    }

    exp = Experiment(data)
    res = exp.run()
    rearrange = exp.scenarios[0].result_to_dict(alternative_hierarchy={
        "name": "root",
        "children": [
            {
                "name": "middle",
                "aggregator": "sum",
                "children": [
                    {
                        "name": "test",
                    }]
            }
        ]
    })
    print(rearrange)
    assert rearrange


def simple_assignment_adapter_test_files():
    for file in (BASE_TEST_DATA_PATH / "simple_assignments_adapter").glob("*.csv"):
        yield file


def simple_assignment_adapter_test_files_names():
    for file in (BASE_TEST_DATA_PATH / "simple_assignments_adapter").glob("*.csv"):
        yield file.name


@pytest.mark.parametrize('adapter_csv_file', argvalues=simple_assignment_adapter_test_files(),
                         ids=simple_assignment_adapter_test_files_names())
def test_simple_assignment_adapter_csv(adapter_csv_file: Path):
    for i in range(1, 3):
        data = {
            "adapters": [
                {
                    "adapter_name": "simple-assignment-adapter",
                    "config": {
                        "source_csv_file": adapter_csv_file
                    },
                    "methods": {
                        "test": "co2"
                    }
                }
            ],
            "hierarchy": {
                "name": "root",
                "aggregator": "sum",
                "children": [
                    {
                        "name": "n1",
                        "adapter": "assign",
                        "config": {}
                    }
                ]
            }
        }

        exp = Experiment(data)
        print(cast(SimpleAssignmentAdapter, exp.get_adapter_by_name("simple-assignment-adapter")).nodes)

    # res = exp.run()
    # rearrange = exp.scenarios[0].result_to_dict(alternative_hierarchy={
    #     "name": "root",
    #     "children": [
    #         {
    #             "name": "middle",
    #             "aggregator":"sum",
    #             "children": [
    #                 {
    #                     "name": "test",
    #                 }]
    #         }
    #     ]
    # })
    # print(rearrange)
    # assert rearrange
