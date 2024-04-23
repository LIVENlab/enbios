from csv import DictReader
from itertools import product
from pathlib import Path

import pytest

from enbios import Experiment
from enbios.const import BASE_TEST_DATA_PATH


@pytest.fixture
def results_base_folder() -> Path:
    return BASE_TEST_DATA_PATH / "experiment_csv_results"


@pytest.fixture
def temp_file() -> Path:
    return BASE_TEST_DATA_PATH / "temp/temp.csv"


def compare_csv_files(test_file: Path, validation_file: Path):
    test_reader = DictReader(test_file.open(encoding="utf-8"))
    validation_reader = DictReader(validation_file.open(encoding="utf-8"))

    assert list(test_reader.fieldnames) == list(validation_reader.fieldnames)

    test_data = list(test_reader)
    validation_data = list(validation_reader)
    assert len(test_data) == len(validation_data)

    for test_row, validation_row in zip(test_data, validation_data):
        for k, v in test_row.items():
            if k.startswith("results_"):
                assert v == pytest.approx(validation_row[k], 0.001)
        assert test_row == validation_row


def test_base_crash_test(two_level_experiment_from_pickle: Experiment, clear_temp_files, temp_file: Path):
    bool_param_names = ["include_method_units", "include_output", "flat_hierarchy", "include_extras",
                        "repeat_parent_name"]
    for args_value_product in list(product([True, False], repeat=len(bool_param_names))):
        kwargs = dict(zip(
            bool_param_names,
            list(args_value_product)
        ))
        file_name = f"t_{'_'.join([str(k) for k, v in kwargs.items() if v])}.csv"
        two_level_experiment_from_pickle.results_to_csv(temp_file.parent / file_name, **kwargs)


def test_normal_csv(two_level_experiment_from_pickle: Experiment, clear_temp_files, results_base_folder: Path,
                    temp_file):
    two_level_experiment_from_pickle.results_to_csv(temp_file)
    compare_csv_files(temp_file, results_base_folder / "test_normal_csv.csv")


def test_no_extras_csv(two_level_experiment_from_pickle: Experiment, clear_temp_files, results_base_folder: Path,
                       temp_file: Path):
    two_level_experiment_from_pickle.results_to_csv(temp_file, include_extras=False)
    compare_csv_files(temp_file, results_base_folder / "test_no_extras_csv.csv")


def test_single_scenario_csv(two_level_experiment_from_pickle: Experiment, clear_temp_files, results_base_folder: Path,
                             temp_file: Path):
    two_level_experiment_from_pickle.results_to_csv(temp_file, scenario_name="default scenario", include_extras=True)
    compare_csv_files(temp_file, results_base_folder / "test_single_scenario_csv.csv")


def test_single_scenario_no_extras_csv(two_level_experiment_from_pickle: Experiment, clear_temp_files,
                                       results_base_folder: Path,
                                       temp_file: Path):
    two_level_experiment_from_pickle.results_to_csv(temp_file, scenario_name="default scenario", include_extras=False)
    compare_csv_files(temp_file, results_base_folder / "test_single_scenario_no_extras_csv.csv")


def test_no_method_units_csv(two_level_experiment_from_pickle: Experiment, clear_temp_files, results_base_folder: Path,
                             temp_file: Path):
    two_level_experiment_from_pickle.results_to_csv(temp_file, include_method_units=False)
    compare_csv_files(temp_file, results_base_folder / "test_no_method_units_csv.csv")


def test_flat_hierarchy_csv(two_level_experiment_from_pickle: Experiment, clear_temp_files, results_base_folder: Path,
                            temp_file: Path):
    two_level_experiment_from_pickle.results_to_csv(temp_file, flat_hierarchy=True)
    compare_csv_files(temp_file, results_base_folder / "test_flat_hierarchy_csv.csv")


def test_repeat_parent_name(two_level_experiment_from_pickle: Experiment, clear_temp_files, results_base_folder: Path,
                            temp_file: Path):
    two_level_experiment_from_pickle.results_to_csv(temp_file, repeat_parent_name=True)
    compare_csv_files(temp_file, results_base_folder / "test_repeat_parent_name.csv")


def test_level_names(two_level_experiment_from_pickle: Experiment, clear_temp_files, results_base_folder: Path,
                     temp_file: Path):
    two_level_experiment_from_pickle.results_to_csv(temp_file, level_names=["root", "technology", "node"])
    compare_csv_files(temp_file, results_base_folder / "test_level_names.csv")


def test_alternative_hierarchy_csv(two_level_experiment_from_pickle: Experiment, clear_temp_files,
                                   results_base_folder: Path, temp_file: Path):
    alt_hierarchy = {
        "name": "root",
        "aggregator": "sum",
        "children": two_level_experiment_from_pickle.structural_nodes_names,
    }

    two_level_experiment_from_pickle.results_to_csv(temp_file, alternative_hierarchy=alt_hierarchy)
