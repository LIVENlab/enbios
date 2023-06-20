from pathlib import Path

import pytest

from enbios2.base.experiment import Experiment
from enbios2.const import BASE_TEST_DATA_PATH
from enbios2.generic.files import DataPath, ReadPath
from enbios2.models.experiment_models import ExperimentData


# This fixture scans a directory and returns all files in the directory.
@pytest.fixture
def experiment_files(tmp_path):
    # todo this is not working
    print(list(Path(BASE_TEST_DATA_PATH / "experiment_instances").glob("*.json")))
    return list((BASE_TEST_DATA_PATH / "experiment_instances").glob("*.json"))


# @pytest.mark.parametrize("experiment_files", indirect=["experiment_file"])
def test_experiment_config():
    experiment_files = list(Path(BASE_TEST_DATA_PATH / "experiment_instances").glob("*.json"))
    for file in experiment_files:
        try:
            data = ReadPath(file).read_data()
            # print(data)
            exp = ExperimentData(**data)
            Experiment(exp)
        except Exception as e:
            print()
            print(file)
            print(e)

