import csv
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

            ]
        },
        "scenarios": [
            # nodes are added below
            {"name": "sc1", "nodes": {

            }},
            {"name": "sc2", "nodes": {
            }}
        ]
    }

    if adapter_csv_file.stem.endswith("_x"):
        with pytest.raises(Exception):
            exp = Experiment(data)
    else:
        pass
        # add n2 to the hierarchy & scenarios, if it exists
        # to keep the insertion order, we first do a list, then a set,
        # and turn back to sorted list (based on initial index)
        all_nodes = list(r['node_name'] for r in list(csv.DictReader(adapter_csv_file.open())))
        all_nodes = list(sorted(set(all_nodes), key=all_nodes.index))
        for node in all_nodes:
            data["hierarchy"]["children"].append(  # type: ignore
                {
                    "name": node,
                    "adapter": "assign",
                    "config": {}
                })
        for scenario in data['scenarios']:
            scenario["nodes"].update({n: {} for n in all_nodes})  # type: ignore

        exp = Experiment(data)
        nodes = cast(SimpleAssignmentAdapter, exp.get_adapter_by_name("simple-assignment-adapter")).nodes
        result_dict = {n: v.model_dump() for n, v in nodes.items()}
        print(json.dumps(result_dict, indent=2))
        nodes_comparison_file = Path(
            BASE_TEST_DATA_PATH / f"simple_assignments_adapter/validate/{adapter_csv_file.stem}.json")
        nodes_comparison_data = json.load(nodes_comparison_file.open())
        assert result_dict == nodes_comparison_data
        results = exp.run()
        results_comparisson_file = Path(
            BASE_TEST_DATA_PATH / f"simple_assignments_adapter/results/{adapter_csv_file.stem}.json")
        result_comparison_data = json.load(results_comparisson_file.open())
        assert results == result_comparison_data


@pytest.mark.parametrize('adapter_csv_file', argvalues=simple_assignment_adapter_test_files(),
                         ids=simple_assignment_adapter_test_files_names())
def test_simple_assignment_adapter_csv(adapter_csv_file: Path):
    run_test_with_file(adapter_csv_file)


def test_simple_assignment_adapter_with_csv():
    run_test_with_file(BASE_TEST_DATA_PATH / "simple_assignments_adapter/inputs/simple_assignment7.csv")
