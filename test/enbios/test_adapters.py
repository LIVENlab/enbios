import json
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
    for file in sorted((BASE_TEST_DATA_PATH / "simple_assignments_adapter/inputs").glob("*.csv")):
        yield file


def simple_assignment_adapter_test_files_names():
    for file in sorted((BASE_TEST_DATA_PATH / "simple_assignments_adapter/inputs").glob("*.csv")):
        yield file.stem


def run_test_with_file(adapter_csv_file: Path):
    data = {
        "adapters": [
            {
                "adapter_name": "simple-assignment-adapter",
                "config": {
                    "source_csv_file": adapter_csv_file
                },
                "methods": {
                    "co2": "kg"
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
        },
        "scenarios": [
            {"name": "sc1", "nodes": {}},
            {"name": "sc2", "nodes": {}}
        ]
    }

    if adapter_csv_file.stem.endswith("_x"):
        with pytest.raises(Exception):
            exp = Experiment(data)
    else:
        exp = Experiment(data)
        nodes = cast(SimpleAssignmentAdapter, exp.get_adapter_by_name("simple-assignment-adapter")).nodes
        result_dict = {n: v.model_dump() for n, v in nodes.items()}
        print(json.dumps(result_dict, indent=2))
        comparisson_file = Path(
            BASE_TEST_DATA_PATH / f"simple_assignments_adapter/validate/{adapter_csv_file.stem}.json")
        comparisson_data = json.load(comparisson_file.open())
        assert result_dict == comparisson_data
        results = exp.run()
        pass


@pytest.mark.parametrize('adapter_csv_file', argvalues=simple_assignment_adapter_test_files(),
                         ids=simple_assignment_adapter_test_files_names())
def test_simple_assignment_adapter_csv(adapter_csv_file: Path):
    run_test_with_file(adapter_csv_file)


def test_simple_assignment_adapter_with_csv():
    run_test_with_file(BASE_TEST_DATA_PATH / "simple_assignments_adapter/inputs/simple_assignment7.csv")
