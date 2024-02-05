import json

import bw2data
import pytest
from bw2calc import LCA
from bw2data.backends import ActivityDataset
from pint import UndefinedUnitError

from enbios.base.experiment import Experiment
from enbios.bw2.brightway_experiment_adapter import BrightwayAdapter
from enbios.const import BASE_TEST_DATA_PATH
from test.enbios.conftest import experiment_setup


def test_basic(basic_experiment):
    print(basic_experiment)


def test_bw_config(default_bw_config):
    assert default_bw_config["bw_project"] in bw2data.projects
    bw2data.projects.set_current(default_bw_config["bw_project"])
    assert default_bw_config["ecoinvent_db"] in bw2data.databases


def test_get_activity_db_code(default_bw_config, experiment_setup):
    activity_db_code = {
        "name": 'electricity production, hard coal',
        "adapter": "bw",
        "config": {
            "database": default_bw_config["ecoinvent_db"],
            "code": 'cfa79d34d94d122a4fd35786da2c6d4e'
        }

    }

    experiment_setup["scenario"]["hierarchy"]["children"].append(activity_db_code)
    Experiment(experiment_setup["scenario"])


def test_get_activity_code(experiment_setup):
    activity_code = {
        "name": 'electricity production, hard coal2',
        "adapter": "bw",
        "config": {
            "code": 'cfa79d34d94d122a4fd35786da2c6d4e'
        }
    }

    experiment_setup["scenario"]["hierarchy"]["children"].append(activity_code)
    Experiment(experiment_setup["scenario"])


def test_get_activity_name_no_loc(experiment_setup):
    activity_name_no_loc = {
        "name": 'market for petroleum refinery',
        "adapter": "bw",
        "config": {
            "name": 'market for petroleum refinery'
        }
    }

    experiment_setup["scenario"]["hierarchy"]["children"].append(activity_name_no_loc)
    Experiment(experiment_setup["scenario"])


def test_get_activity_activity_multiple_candidates(experiment_setup):
    activity_multiple_candidates = {
        "name": 'electricity production, wind, 1-3MW turbine, onshore',
        "adapter": "bw",
        "config": {
            "name": 'electricity production, wind, 1-3MW turbine, onshore'
        }
    }

    experiment_setup["scenario"]["hierarchy"]["children"].append(activity_multiple_candidates)

    with pytest.raises(ValueError):
        Experiment(experiment_setup["scenario"])


def test_get_activity_activity_activity_does_not_exist(experiment_setup):
    activity_does_exists = {
        "name": 'wood bottles',
        "adapter": "bw",
        "config": {
            "name": 'wood bottles'
        }
    }

    experiment_setup["scenario"]["hierarchy"]["children"].append(activity_does_exists)

    with pytest.raises(ValueError):
        Experiment(experiment_setup["scenario"])


def test_bw_config_invalid_distribution(experiment_setup):
    experiment_setup["scenario"]["adapters"][0]["config"]["use_k_bw_distributions"] = 0

    with pytest.raises(ValueError):
        Experiment(experiment_setup["scenario"])


def test_bw_config_invalid_no_project(experiment_setup):
    experiment_setup["scenario"]["adapters"][0]["config"]["bw_project"] = "I dont exists......."

    with pytest.raises(ValueError):
        Experiment(experiment_setup["scenario"])


def test_bw_invalid_method(experiment_setup):
    experiment_setup["scenario"]["adapters"][0]["methods"]["missing_method"] = ("IPCC", "Does not exist...")

    with pytest.raises(ValueError):
        Experiment(experiment_setup["scenario"])


def test_bw_node_output_invalid_undefined(experiment_setup):
    experiment_setup["scenario"]["hierarchy"]["children"][0]["config"]["default_output"] = {
        "unit": "box",
        "magnitude": 1
    }
    with pytest.raises(UndefinedUnitError):
        Experiment(experiment_setup["scenario"])


def test_get_method_unit(experiment_setup):
    exp = Experiment(experiment_setup["scenario"])
    method = list((experiment_setup["scenario"]["adapters"][0]["methods"].keys()))[0]
    assert exp.adapters[0].get_method_unit(method)


def test_get_config_schema():
    schemas = BrightwayAdapter.get_config_schemas()
    assert schemas is not None


def test_run_store_data(experiment_setup):
    experiment_setup["scenario"]["adapters"][0]["config"]["store_lca_object"] = True
    experiment_setup["scenario"]["adapters"][0]["config"]["store_raw_results"] = True
    exp = Experiment(experiment_setup["scenario"])
    result = exp.run()


def test_run_exclude_defaults(experiment_setup):
    experiment_setup["scenario"]["hierarchy"]["children"].append({
        "name": "2nd",
        "adapter": "bw",
        "config": {
            "name": "heat and power co-generation, wood chips, 6667 kW, state-of-the-art 2014",
            "unit": "kilowatt hour",
            "location": "DK"
        }})

    experiment_setup["scenario"]["scenarios"] = [{
        "name": "scenario1",
        "nodes": {
            experiment_setup["scenario"]["hierarchy"]["children"][0]["name"]: ["kWh", 5]
        },
        "config": {"exclude_defaults": True}
    }]
    exp = Experiment(experiment_setup["scenario"])
    exp.run()


def test_run_use_distribution(experiment_setup, default_bw_method_name):
    experiment_setup["scenario"]["adapters"][0]["config"]["use_k_bw_distributions"] = 2
    result = Experiment(experiment_setup["scenario"]).run()
    assert len(result["default scenario"]["results"][default_bw_method_name]["multi_magnitude"]) == 2
    assert all(v != 0.0 for v in result["default scenario"]["results"][default_bw_method_name]["multi_magnitude"])


def regionalization_setup(experiment_setup: dict):
    # experiment_setup["scenario"]["hierarchy"]["children"][0]["config"]["enb_location"] = ("ES", "cat")
    experiment_setup["scenario"]["hierarchy"]["children"] = [
        {'name': 'electricity production, wind, 1-3MW turbine, onshore',
         'adapter': 'brightway-adapter',
         'config': {'code': 'ed3da88fc23311ee183e9ffd376de89b',
                    "enb_location": ("ES", "cat"),
                    'default_output': {'unit': 'kilowatt_hour', 'magnitude': 3}}},
        {'name': 'electricity production, wind, 1-3MW turbine, offshore',
         'adapter': 'brightway-adapter',
         'config': {'code': '6ebfe52dc3ef5b4d35bb603b03559023',
                    "enb_location": ("ES", "cat")}}]

    bw_adapter = experiment_setup["scenario"]["adapters"][0]
    bw2data.projects.set_current(bw_adapter["config"]["bw_project"])

    cats = json.load((BASE_TEST_DATA_PATH / "catalan_activities.json").open())

    # noinspection PyUnresolvedReferences
    with ActivityDataset._meta.database.atomic():
        for a in ActivityDataset.select().where(ActivityDataset.type == "process"):
            if a.code in cats:
                a.data["enb_location"] = ("ES", "cat")
            else:
                a.data["enb_location"] = ("OTHER", "rest")
            a.save()
    bw_adapter["config"]["simple_regionalization"] = {"run_regionalization": True,
                                                      "select_regions": {"ES", "OTHER"},
                                                      "set_node_regions": {}}  # ,

    bw_adapter["methods"] = {
        "GWP1000": (
            "ReCiPe 2016 v1.03, midpoint (H)",
            "climate change",
            "global warming potential (GWP1000)",
        ),
        "FETP": (
            "ReCiPe 2016 v1.03, midpoint (H)",
            "ecotoxicity: freshwater",
            "freshwater ecotoxicity potential (FETP)",
        ),
        "HTPnc": (
            "ReCiPe 2016 v1.03, midpoint (H)",
            "human toxicity: non-carcinogenic",
            "human toxicity potential (HTPnc)",
        ),
    }


def test_regionalization(experiment_setup):
    regionalization_setup(experiment_setup)
    try:
        exp = Experiment(experiment_setup["scenario"])
        regio_res = exp.run()
    except Exception as e:
        print(e)
        return
    finally:
        # noinspection PyUnresolvedReferences
        with ActivityDataset._meta.database.atomic():
            for a in ActivityDataset.select().where(ActivityDataset.type == "process"):
                if "enb_location" in a.data:
                    del a.data["enb_location"]
                    a.save()

    experiment_setup["scenario"]["adapters"][0]["config"]["simple_regionalization"] = {"run_regionalization": False}
    exp = Experiment(experiment_setup["scenario"])
    res = exp.run()
    # print(res)

    method_total_result = {}
    for method_regio, result in regio_res["default scenario"]["results"].items():
        method, region = method_regio.split(".")
        # print(result)
        method_total_result[method] = method_total_result.get(method, 0) + result["magnitude"]
    for method, result in res["default scenario"]["results"].items():
        # print(method_total_result[method], result["magnitude"], method_total_result[method] - result["magnitude"])
        assert method_total_result[method] == pytest.approx(result["magnitude"], abs=1e-15)


def test_regionalization_distribution(experiment_setup):
    regionalization_setup(experiment_setup)
    experiment_setup["scenario"]["adapters"][0]["config"]["use_k_bw_distributions"] = 2
    try:
        exp = Experiment(experiment_setup["scenario"])
        regio_res = exp.run()
        print(json.dumps(regio_res, indent=2))
    except Exception as e:
        print(e)
    finally:
        # noinspection PyUnresolvedReferences
        with ActivityDataset._meta.database.atomic():
            for a in ActivityDataset.select().where(ActivityDataset.type == "process"):
                if "enb_location" in a.data:
                    del a.data["enb_location"]
                    a.save()
    pass


def test_nonlinear_methods1(experiment_setup: dict,
                            default_bw_method_name: str,
                            default_method_tuple: tuple[str, ...]):
    adapter_def = experiment_setup["scenario"]["adapters"][0]
    # 1. test to check get_defaults_from_original
    no_nonlinear_method_name = default_bw_method_name + "_normal"
    adapter_def["methods"][no_nonlinear_method_name] = adapter_def["methods"][default_bw_method_name]
    # cf value that won't have any impact anyway...
    adapter_def["config"]["nonlinear_characterization"] = {"methods": {
        default_bw_method_name: {
            "functions": {
                ('biosphere3', '38a622c6-f086-4763-a952-7c6b3b1c42ba'): lambda x: 0
            },
            "get_defaults_from_original": True
        }}}
    experiment = Experiment(experiment_setup["scenario"])
    result = experiment.run()["default scenario"]['results']
    assert result[default_bw_method_name]["magnitude"] == result[no_nonlinear_method_name]["magnitude"]
    # compare with bw LCA
    bw_adapter: BrightwayAdapter = experiment.adapters[0]
    bw_activity = bw_adapter.activityMap["single_activity"].bw_activity
    regular_score = bw_activity.lca(default_method_tuple).score
    assert result[default_bw_method_name]["magnitude"] == pytest.approx(regular_score, abs=1e-7)


def test_nonlinear_methods2(set_bw_default_project,
                            experiment_setup: dict,
                            default_bw_method_name: str,
                            default_method_tuple: tuple[str, ...]):
    # test 2: normal LCA with some demand * 2 compared to demand * 1 and non-linear func (values * 2)
    biosphere_cfs = bw2data.Method(default_method_tuple).load()
    nonlinear_cfs = {
        tuple(key): lambda v: v * 2
        for key, _ in biosphere_cfs
    }
    adapter_def = experiment_setup["scenario"]["adapters"][0]
    adapter_def["config"]["nonlinear_characterization"] = {"methods": {
        default_bw_method_name: {
            "functions": nonlinear_cfs,
            "get_defaults_from_original": False
        }}}

    experiment = Experiment(experiment_setup["scenario"])
    result = experiment.run()["default scenario"]['results']

    bw_adapter: BrightwayAdapter = experiment.adapters[0]
    bw_activity = bw_adapter.activityMap["single_activity"].bw_activity
    lca = bw_activity.lca(default_method_tuple, 2)
    double_score = lca.score
    assert result[default_bw_method_name]["magnitude"] == pytest.approx(double_score, abs=1e-15)
    # check characterized_inventory
    pass


def test_nonlinear_methods3(set_bw_default_project,
                            experiment_setup: dict,
                            default_bw_method_name: str,
                            default_method_tuple: tuple[str, ...]):
    # todo finnish
    biosphere_cfs = bw2data.Method(default_method_tuple).load()
    arbitrary_cfs = [
        lambda v: v * v, lambda v: 1, lambda v: 1
    ]
    nonlinear_cfs = {
        tuple(cf[0]): arbitrary_cfs[idx]
        for idx, cf in enumerate(biosphere_cfs)
    }
    adapter_def = experiment_setup["scenario"]["adapters"][0]
    adapter_def["config"]["nonlinear_characterization"] = {"methods": {
        default_bw_method_name: {
            "functions": nonlinear_cfs,
            "get_defaults_from_original": False
        }}}

    experiment = Experiment(experiment_setup["scenario"])
    result = experiment.run()["default scenario"]['results']

    bw_adapter: BrightwayAdapter = experiment.adapters[0]
    bw_activity = bw_adapter.activityMap["single_activity"].bw_activity
    lca = LCA({bw_activity: 1}, method=default_method_tuple)
    lca.lci()

    # get the summed inventory, which are basically all biosphere activities
    # Total Biosphere Demand Vector
    summed_inventory = lca.inventory.sum(1)
    pass

def test_regionlized_nonlinear_characterization(self):
    pass
