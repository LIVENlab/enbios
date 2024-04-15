import csv
import json
import shutil
import traceback
from pathlib import Path
from typing import cast

import pytest

from enbios.base.adapters_aggregators.builtin.assignment_adapter import AssignmentAdapter
from enbios.base.experiment import Experiment
from enbios.const import BASE_TEST_DATA_PATH


def assignment_adapter_test_files():
    for file in sorted((BASE_TEST_DATA_PATH / "assignment_adapter/inputs").glob("*.csv")):
        yield file


def assignment_adapter_test_files_names():
    for file in sorted((BASE_TEST_DATA_PATH / "assignment_adapter/inputs").glob("*.csv")):
        yield file.stem


def run_test_with_file(adapter_csv_file: Path):
    data = {
        "adapters": [
            {
                "adapter_name": "assignment-adapter",
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
    if adapter_csv_file.stem.endswith("_x"):
        with pytest.raises(Exception):
            try:
                exp = Experiment(data)
            except Exception as err:
                traceback.print_exc()
                raise err
    else:

        exp = Experiment(data)
        nodes = cast(AssignmentAdapter, exp.get_adapter_by_name("assignment-adapter")).nodes
        result_dict = {n: v.model_dump() for n, v in nodes.items()}
        print(json.dumps(result_dict, indent=2))
        nodes_comparison_file = Path(
            BASE_TEST_DATA_PATH / f"assignment_adapter/validate/{adapter_csv_file.stem}.json")
        nodes_comparison_data = json.load(nodes_comparison_file.open())
        assert result_dict == nodes_comparison_data
        results = exp.run()
        results_comparison_file = Path(
            BASE_TEST_DATA_PATH / f"assignment_adapter/results/{adapter_csv_file.stem}.json")
        result_comparison_data = json.load(results_comparison_file.open())
        assert results == result_comparison_data

@pytest.mark.parametrize('adapter_csv_file', argvalues=assignment_adapter_test_files(),
                         ids=assignment_adapter_test_files_names())
def test_assignment_adapter_csv(adapter_csv_file: Path):
    run_test_with_file(adapter_csv_file)


def test_assignment_adapter_with_csv():
    run_test_with_file(BASE_TEST_DATA_PATH / "assignment_adapter/inputs/assignment8.csv")
