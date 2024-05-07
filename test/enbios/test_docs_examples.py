from enbios import Experiment
from enbios.const import BASE_TEST_DATA_PATH


def test_simple_example():
    experiment_config_file = BASE_TEST_DATA_PATH / "docs_data/simple_example.json"
    exp = Experiment(experiment_config_file)
    # exp.run()
