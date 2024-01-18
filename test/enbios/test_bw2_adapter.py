import json

import bw2data
import pytest
from bw2data.backends import ActivityDataset
from pint import UndefinedUnitError

from enbios.base.experiment import Experiment
from enbios.bw2.brightway_experiment_adapter import BrightwayAdapter
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
    Experiment(experiment_setup["scenario"]).run()


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
        "activities": {
            experiment_setup["scenario"]["hierarchy"]["children"][0]["name"]: ["kWh", 5]
        },
        "config": {"exclude_defaults": True}
    }]
    exp = Experiment(experiment_setup["scenario"])

    exp.run()


def test_run_use_distribution(experiment_setup):
    experiment_setup["scenario"]["adapters"][0]["config"]["use_k_bw_distributions"] = 2
    Experiment(experiment_setup["scenario"]).run()


def test_regionalization(experiment_setup):
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

    bw2data.projects.set_current(experiment_setup["scenario"]["adapters"][0]["config"]["bw_project"])
    with ActivityDataset._meta.database.atomic():
        for a in ActivityDataset.select().where(ActivityDataset.type == "process"):
            if "enb_location" in a.data:
                del a.data["enb_location"]  # = ("ES", "cat")
                a.save()
    experiment_setup["scenario"]["adapters"][0]["config"]["simple_regionalization"] = {"run_regionalization": True,
                                                                                       "select_regions": {"ES"},
                                                                                       "set_node_regions": {}}  # ,
    cats = json.load(open("catalan.json"))
    for c in cats:
        experiment_setup["scenario"]["adapters"][0]["config"]["simple_regionalization"]["set_node_regions"][c] = (
        "ES", "cat")

    experiment_setup["scenario"]["adapters"][0]["methods"] = {
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
    exp = Experiment(experiment_setup["scenario"])

    regio_res = exp.run()

    c = 0
    bw2data.projects.set_current(experiment_setup["scenario"]["adapters"][0]["config"]["bw_project"])
    with ActivityDataset._meta.database.atomic():
        for a in ActivityDataset.select().where(ActivityDataset.type == "process"):
            if "enb_location" in a.data:
                if a.data["enb_location"] == ("ES", "cat"):
                    c += 1
    print("cats", c)
                # del a.data["enb_location"]  # = ("ES", "cat")
                # a.save()

    print(regio_res)

    experiment_setup["scenario"]["adapters"][0]["config"]["simple_regionalization"] = {"run_regionalization": False}
    exp = Experiment(experiment_setup["scenario"])
    res = exp.run()
    print(res)
