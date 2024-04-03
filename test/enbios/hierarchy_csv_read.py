import json
from pathlib import Path

import pytest

from enbios.base.tree_operations import csv2hierarchy
from enbios.const import BASE_TEST_DATA_PATH


def hierarchy_test_files():
    for file in sorted((BASE_TEST_DATA_PATH / "hierarchy_csvs/inputs").glob("*.csv")):
        yield file


def hierarchy_test_files_names():
    for file in sorted((BASE_TEST_DATA_PATH / "hierarchy_csvs/inputs").glob("*.csv")):
        yield file.stem


def run_test_with_file(hierarchy_csv_file):
    if hierarchy_csv_file.stem.endswith("_x"):
        with pytest.raises(Exception):
            csv2hierarchy(hierarchy_csv_file)
    else:
        tree = csv2hierarchy(hierarchy_csv_file)
        # print(json.dumps(tree, indent=2))
        nodes_comparison_file = Path(
            BASE_TEST_DATA_PATH / f"hierarchy_csvs/validate/{hierarchy_csv_file.stem}.json")
        nodes_comparison_data = json.load(nodes_comparison_file.open())
        assert tree == nodes_comparison_data

@pytest.mark.parametrize('hierarchy_csv_file', argvalues=hierarchy_test_files(),
                         ids=hierarchy_test_files_names())
def test_csv_tree(hierarchy_csv_file):
    run_test_with_file(hierarchy_csv_file)



def test_simple_assignment_adapter_with_csv():
    run_test_with_file(BASE_TEST_DATA_PATH / "hierarchy_csvs/inputs/hierarchy2.csv")