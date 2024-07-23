import json
import pickle
from csv import DictReader
from pathlib import Path

import bw2data
import pytest

from enbios.base.experiment import Experiment
from enbios.const import BASE_TEST_DATA_PATH
from enbios import ResultValue, ScenarioResultNodeData, BasicTreeNode
from enbios.base.models import NodeOutput
from test.enbios.test_project_fixture import TEST_BW_PROJECT, BRIGHTWAY_ADAPTER_MODULE_PATH, TEST_ECOINVENT_DB, \
    BRIGHTWAY_ADAPTER_MODULE_NAME


@pytest.fixture(scope="module")
def tempfolder() -> Path:
    path = BASE_TEST_DATA_PATH / "temp"
    path.mkdir(parents=True, exist_ok=True)
    return path


@pytest.fixture()
def clear_temp_files(tempfolder):
    for f in tempfolder.glob("*"):
        f.unlink()


@pytest.fixture
def default_method_tuple() -> tuple:
    return 'ReCiPe 2016 v1.03, midpoint (E)', 'climate change', 'global warming potential (GWP1000)'


@pytest.fixture
def default_bw_config() -> dict:
    return {
        "bw_project": TEST_BW_PROJECT,
        "bw_module_path": BRIGHTWAY_ADAPTER_MODULE_PATH,
        "adapter_name": BRIGHTWAY_ADAPTER_MODULE_NAME,
        "ecoinvent_db": TEST_ECOINVENT_DB
    }

@pytest.fixture
def basic_exp_run_result_tree(run_basic_experiment) -> BasicTreeNode[ScenarioResultNodeData]:
    return run_basic_experiment.get_scenario(
        Experiment.DEFAULT_SCENARIO_NAME).result_tree

@pytest.fixture
def set_bw_default_project(default_bw_config):
    bw2data.projects.set_current(default_bw_config["bw_project"])


@pytest.fixture
def default_bw_method_name() -> str:
    return "GWP1000"


@pytest.fixture
def default_result_score() -> float:
    return 1.5707658207779422


@pytest.fixture
def bw_adapter_config(default_bw_config: dict, default_method_tuple: tuple, default_bw_method_name: str) -> dict:
    return {
        "module_path": default_bw_config["bw_module_path"],
        "config": {
            "bw_project": default_bw_config["bw_project"]
        },
        "methods": {
            default_bw_method_name: default_method_tuple
        },
        "note": "brightway-adapter"
    }


@pytest.fixture
def first_activity_config() -> dict:
    return {
        "name": "heat and power co-generation, wood chips, 6667 kW, state-of-the-art 2014",
        "unit": "kilowatt hour",
        # "code": 'b9d74efa4fd670b1977a3471ec010737',
        "location": "DK",
        "default_output": {
            "unit": "kWh",
            "magnitude": 1
        }
    }


@pytest.fixture
def experiment_setup(bw_adapter_config, default_result_score: float, first_activity_config: dict,
                     default_bw_method_name: str) -> dict:
    _impact = default_result_score
    return {
        "scenario": {
            "adapters": [
                bw_adapter_config
            ],
            "hierarchy": {
                "name": "root",
                "aggregator": "sum",
                "children": [
                    {
                        "name": "single_activity",
                        "adapter": "bw",
                        "config": first_activity_config,
                    }
                ]
            },
            "config": {
                "run_adapters_concurrently": False
            }
        },
        "expected_result_tree": {'name': 'root',
                                 'children': [
                                     {'name': 'single_activity',
                                      'children': [],
                                      'data':
                                          ScenarioResultNodeData(
                                              output=[NodeOutput(
                                                  unit="kilowatt_hour", magnitude=1.0)],
                                              adapter="bw",
                                              aggregator=None,
                                              extras={'bw_activity_code': 'b9d74efa4fd670b1977a3471ec010737'},
                                              results={
                                                  default_bw_method_name: ResultValue(unit="kg CO2-Eq",
                                                                                      magnitude=_impact)})}],
                                 'data': ScenarioResultNodeData(
                                     output=[NodeOutput(
                                         unit="kilowatt_hour", magnitude=1.0)],
                                     adapter=None,
                                     extras={},
                                     aggregator="sum",
                                     results={
                                         default_bw_method_name: ResultValue(unit="kg CO2-Eq", magnitude=_impact)},
                                     output_aggregation=[[0]])}
    }



@pytest.fixture
def experiment_scenario_setup(bw_adapter_config, first_activity_config):
    return {
        "adapters": [
            bw_adapter_config
        ],
        "hierarchy": {
            "name": "root",
            "aggregator": "sum",
            "children": [
                {
                    "name": "single_activity",
                    "adapter": "bw",
                    "config": first_activity_config,
                }
            ]
        },
        "scenarios": [
            {
                "name": "scenario1",
                "nodes": {
                    "single_activity": {
                        "unit": "kWh",
                        "magnitude": 1
                    }
                }
            },
            {
                "name": "scenario2",
                "nodes": {
                    "single_activity": {
                        "unit": "MWh",
                        "magnitude": 2
                    }
                }
            }]
    }

@pytest.fixture
def basic_experiment(experiment_setup) -> Experiment:
    scenario_data = experiment_setup["scenario"]
    return Experiment(scenario_data)


@pytest.fixture
def run_basic_experiment(basic_experiment) -> Experiment:
    basic_experiment.run()
    return basic_experiment


@pytest.fixture
def test_network_project_db_name() -> tuple[str, str]:
    return "test_network", "db"


@pytest.fixture
def create_test_network(test_network_project_db_name):
    import bw2data
    try:
        from bw_tools.network_build import build_network
    except ImportError:
        raise AssertionError("Cannot import bw_tools.network_build")

    project_name, db_name = test_network_project_db_name
    try:
        bw2data.projects.delete_project(project_name, True)
    except ValueError:
        pass

    bw2data.projects.set_current(project_name)

    db = bw2data.Database(db_name)
    db.register()

    activities = list(DictReader((BASE_TEST_DATA_PATH / "test_networks/activities1.csv").open(encoding="utf-8")))
    exchanges = list(DictReader((BASE_TEST_DATA_PATH / "test_networks/exchanges1.csv").open(encoding="utf-8")))
    build_network(db, {
        "activities": activities,
        "exchanges": exchanges
    })

    yield
    try:
        bw2data.projects.delete_project(project_name, True)
    except ValueError:
        pass


@pytest.fixture
def two_level_experiment_config(bw_adapter_config: dict, default_bw_config: dict) -> dict:
    bw_adapter_config["methods"]['WCP'] = ('ReCiPe 2016 v1.03, midpoint (E)',
                                           'water use',
                                           'water consumption potential (WCP)')
    bw_adapter_config["adapter_name"] = default_bw_config["adapter_name"]
    del bw_adapter_config["module_path"]
    config = json.load((BASE_TEST_DATA_PATH / "experiment_configs/two_level_two_methods.json").open(encoding="utf-8"))
    config["adapters"].append(bw_adapter_config)
    return config


@pytest.fixture
def two_level_experiment_from_pickle(two_level_experiment_config: dict) -> Experiment:
    exp_pickle = BASE_TEST_DATA_PATH / "pickles/two_level_experiment.pickle"
    # need to do the import here, otherwise pickle loader fails
    from enbios.bw2 import brightway_experiment_adapter
    brightway_experiment_adapter.logger.setLevel("INFO")
    try:
        if exp_pickle.exists():
            return pickle.load(exp_pickle.open("rb"))
    except Exception as err:
        raise err
    print("running experiment...")
    exp = Experiment(two_level_experiment_config)
    exp.run()
    pickle.dump(exp, exp_pickle.open("wb"))
    return exp
