from csv import DictReader
from pathlib import Path

import bw2data
import pytest

from enbios.base.experiment import Experiment
from enbios.const import BASE_TEST_DATA_PATH
from enbios.models.experiment_base_models import NodeOutput
from enbios.models.experiment_models import ScenarioResultNodeData, ResultValue
from test.enbios.test_project_fixture import TEST_BW_PROJECT, BRIGHTWAY_ADAPTER_MODULE_PATH, TEST_ECOINVENT_DB


@pytest.fixture(scope="module")
def tempfolder() -> Path:
    path = BASE_TEST_DATA_PATH / "temp"
    path.mkdir(parents=True, exist_ok=True)
    return path


@pytest.fixture
def default_method_tuple() -> tuple:
    return 'ReCiPe 2016 v1.03, midpoint (E)', 'climate change', 'global warming potential (GWP1000)'


@pytest.fixture
def default_bw_config() -> dict:
    return {
        "bw_project": TEST_BW_PROJECT,
        "bw_module_path": BRIGHTWAY_ADAPTER_MODULE_PATH,
        "ecoinvent_db": TEST_ECOINVENT_DB
    }


@pytest.fixture
def set_bw_default_project(default_bw_config):
    bw2data.projects.set_current(default_bw_config["bw_project"])


@pytest.fixture
def default_bw_method_name() -> str:
    return "GWP1000"


@pytest.fixture
def default_result_score() -> float:
    return 1.5707658207848347


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
                                              results={
                                                  default_bw_method_name: ResultValue(unit="kg CO2-Eq",
                                                                                      magnitude=_impact)})}],
                                 'data': ScenarioResultNodeData(
                                     output=[NodeOutput(
                                         unit="kilowatt_hour", magnitude=1.0)],
                                     adapter=None,
                                     aggregator="sum",
                                     results={
                                         default_bw_method_name: ResultValue(unit="kg CO2-Eq", magnitude=_impact)})}
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
    except ImportError as err:
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
