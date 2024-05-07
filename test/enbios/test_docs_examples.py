import json

from enbios import Experiment
from enbios.const import BASE_TEST_DATA_PATH


def test_simple_example():
    experiment_config_file = BASE_TEST_DATA_PATH / "docs_data/simple_example.json"
    exp = Experiment(experiment_config_file)
    result = exp.run(results_as_dict=True)
    res_fp = BASE_TEST_DATA_PATH / "docs_data/gen/simple_example_result.json"
    json.dump(result, res_fp.open("w", encoding="utf-8"), ensure_ascii=False, indent=2)
