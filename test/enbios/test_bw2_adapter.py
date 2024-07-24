import json
import math
from typing import cast
from csv import DictReader
from uuid import uuid4

import bw2data
import pytest
from bw2calc import LCA
from bw2data.backends import ActivityDataset
from pint import UndefinedUnitError

from bw_tools import mermaid_diagram
from bw_tools.network_build import build_network
from enbios.base.experiment import Experiment
from enbios.bw2.brightway_experiment_adapter import BrightwayAdapter
from enbios.const import BASE_TEST_DATA_PATH
from test.enbios.conftest import experiment_setup # noqa: F401


def test_basic(basic_experiment):
    print(basic_experiment)


def test_bw_config(default_bw_config):
    assert default_bw_config["bw_project"] in bw2data.projects
    bw2data.projects.set_current(default_bw_config["bw_project"])
    assert default_bw_config["ecoinvent_db"] in bw2data.databases


def test_get_activity_db_code(default_bw_config, experiment_setup): # noqa: F811
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


def test_get_activity_code(experiment_setup):  # noqa: F811
    activity_code = {
        "name": 'electricity production, hard coal2',
        "adapter": "bw",
        "config": {
            "code": 'cfa79d34d94d122a4fd35786da2c6d4e'
        }
    }

    experiment_setup["scenario"]["hierarchy"]["children"].append(activity_code)
    Experiment(experiment_setup["scenario"])


def test_get_activity_name_no_loc(experiment_setup):  # noqa: F811
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
    _ = exp.run()


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


def  regionalization_setup(experiment_setup: dict):
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

    cats = json.load((BASE_TEST_DATA_PATH / "regio_activities.json").open())

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


def test_regionlized_clear_locations(
        test_network_project_db_name: tuple[str, str], bw_adapter_config: dict, create_test_network):
    project_name, db_name = test_network_project_db_name
    db = bw2data.Database(db_name)

    bw_adapter_config["config"]["bw_project"] = project_name

    # simple method
    method_id = ('IPCC',)
    ipcc = bw2data.Method(method_id)
    ipcc.write([
        (db.get("co2").key, {'amount': 1})
    ])
    ipcc.metadata = ipcc.metadata | {"unit": "kg CO2-Eq"}

    bw_adapter_config["methods"] = {"ipcc": ("IPCC",)}
    bw_adapter_config["config"]["simple_regionalization"] = {
        "run_regionalization": True,
        "set_node_regions": {
            "energy1": ["ES"],
            "energy2": ["ES"]
        },
        "select_regions": ["ES"]
    }
    exp_config = {
        "adapters": [
            bw_adapter_config
        ],
        "hierarchy": {
            "name": "root",
            "aggregator": "sum",
            "children": [
                {
                    "name": "product",
                    "config": {
                        "code": "product",
                        "enb_location": ["ES"]
                    },
                    "adapter": "bw"
                }
            ]
        }
    }
    exp = Experiment(exp_config)
    exp.run()

    assert 3 == len([a for a in db if a.get("enb_location") is not None])
    # just set 1, and set location clearing.
    bw_adapter_config["config"]["simple_regionalization"]["set_node_regions"] = {
        "energy1": ["ES"]
    }
    bw_adapter_config["config"]["simple_regionalization"]["clear_all_other_node_regions"] = True
    exp = Experiment(exp_config)
    exp.run()
    assert 2 == len([a for a in db if a.get("enb_location") is not None])


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
    bw_adapter: BrightwayAdapter = cast(BrightwayAdapter,experiment.adapters[0])
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
        tuple(key): lambda v, cf_=cf: v * (cf_ * 2)
        for key, cf in biosphere_cfs
    }
    adapter_def = experiment_setup["scenario"]["adapters"][0]
    adapter_def["config"]["nonlinear_characterization"] = {"methods": {
        default_bw_method_name: {
            "functions": nonlinear_cfs,
            "get_defaults_from_original": False
        }}}

    experiment = Experiment(experiment_setup["scenario"])
    result = experiment.run()["default scenario"]['results']

    bw_adapter: BrightwayAdapter = cast(BrightwayAdapter,experiment.adapters[0])
    bw_activity = bw_adapter.activityMap["single_activity"].bw_activity
    lca = bw_activity.lca(default_method_tuple, 2)
    double_score = lca.score
    assert result[default_bw_method_name]["magnitude"] == pytest.approx(double_score, abs=1e-5)
    # check characterized_inventory
    pass


def test_nonlinear_methods3(set_bw_default_project,
                            experiment_setup: dict,
                            default_bw_method_name: str,
                            default_method_tuple: tuple[str, ...]):
    # todo finnish
    biosphere_cfs = bw2data.Method(default_method_tuple).load()
    # define 3 method functions
    arbitrary_cfs = [
        lambda v: v * v, lambda v: 1, lambda v: 1 * math.pi
    ]
    # fill up a list of non-linear cfs by cycling through the arbitrary cfs
    nonlinear_cfs = {
        tuple(cf[0]): arbitrary_cfs[idx % len(arbitrary_cfs)]
        for idx, cf in enumerate(biosphere_cfs)
    }
    adapter_def = experiment_setup["scenario"]["adapters"][0]
    adapter_def["config"]["nonlinear_characterization"] = {"methods": {
        default_bw_method_name: {
            "functions": nonlinear_cfs,
            "get_defaults_from_original": False
        }}}

    experiment = Experiment(experiment_setup["scenario"])
    _ = experiment.run()["default scenario"]['results']

    bw_adapter: BrightwayAdapter = cast(BrightwayAdapter,experiment.adapters[0])
    bw_activity = bw_adapter.activityMap["single_activity"].bw_activity
    lca = LCA({bw_activity: 1}, method=default_method_tuple)
    lca.lci()

    # get the summed inventory, which are basically all biosphere activities
    # Total Biosphere Demand Vector
    _ = lca.inventory.sum(1)
    pass


def test_regionlized_nonlinear_characterization(test_network_project_db_name: tuple[str, str], create_test_network):

    project_name, db_name = test_network_project_db_name
    bw2data.projects.set_current(project_name)
    db = bw2data.Database(db_name)
    all_activities = list(db)
    _ = mermaid_diagram.create_diagram(all_activities, False)

    # some basic method
    method_id = ('IPCC',)
    ipcc = bw2data.Method(method_id)
    ipcc.write([
        (db.get("co2").key, {'amount': 1})
    ])
    ipcc.metadata = ipcc.metadata | {"unit": "kg CO2-Eq"}
    # method end
    # test basic lca
    product = db.get("product")
    lca = LCA({product: 1}, method_id)
    lca.lci()
    lca.lcia()
    score = lca.score
    assert score == 330.0
    # test lca end

    activities = {a["code"]: a for a in list(db)}
    activity_locations_data = json.load(
        (BASE_TEST_DATA_PATH / "test_networks/activity_locations.json").open(encoding="utf-8"))
    for loc_data in activity_locations_data[:1]:
        for code, loc in loc_data.items():
            activity = activities[code]
            activity["enb_location"] = tuple(loc)
            activity.save()

    experiment_data = json.load(
        (BASE_TEST_DATA_PATH / "individual_setups/bw_adapter_regionalized.json").open(encoding="utf-8"))

    exp = Experiment(experiment_data)
    res = exp.run()
    assert res["default scenario"]["results"]["ipcc.CAT"]["magnitude"] == pytest.approx(110, abs=1e-10)


    waste_method_id = ('IPCC', 'waste')
    waste_method = bw2data.Method(waste_method_id)
    waste_method.write([
        (db.get("waste").key, {'amount': 1})
    ])
    waste_method.metadata = waste_method.metadata | {"unit": "kg"}
    lca = LCA({product: 1}, waste_method_id)
    lca.lci()
    lca.lcia()
    score = lca.score
    assert score == 208.0
    experiment_data["adapters"][0]["methods"] = {"waste": ['IPCC', 'waste']}
    experiment_data["adapters"][0]["config"]["simple_regionalization"]["select_regions"] = ["CAT", "ES", "ARA", "EU"]
    exp = Experiment(experiment_data)

    res = exp.run()
    expected_results = {'waste.CAT': 24, 'waste.ES': 132, 'waste.ARA': 104, 'waste.EU': 148}
    for waste_loc, res in res["default scenario"]["results"].items():
        assert expected_results[waste_loc] == pytest.approx(res["magnitude"], abs=1e-10)

    def custom_waste_cf(v: float) -> float:
        if v < 100:
            return v
        elif v < 120:
            return v * 1.5
        else:
            return v * 3

    experiment_data["adapters"][0]["config"]["nonlinear_characterization"] = {"methods": {
        "waste": {
            "functions": {
                (db_name, "waste"): custom_waste_cf
            },
            "get_defaults_from_original": False
        }}}
    exp = Experiment(experiment_data)
    res = exp.run()
    # # cat: * 24*1, ES: 132 * 3, ARA: 104 * 1.5, EU: 148 * 3
    # """
    # {
    # 'waste.CAT': {'unit': 'kg', 'magnitude': 24.0},
    # 'waste.ES': {'unit': 'kg', 'magnitude': 184.0},
    # 'waste.EU': {'unit': 'kg', 'magnitude': 200.0},
    # 'waste.ARA': {'unit': 'kg', 'magnitude': 156.0}}
    # """
    expected_results = {'waste.CAT': 24, 'waste.ES': 396, 'waste.ARA': 156.0, 'waste.EU': 444}
    for waste_loc, res in res["default scenario"]["results"].items():
        assert expected_results[waste_loc] == pytest.approx(res["magnitude"], abs=1e-10)


@pytest.fixture()
def random_test_project() -> str:
    project_name = str(uuid4())
    bw2data.projects.create_project(project_name)
    bw2data.projects.set_current(project_name)
    yield project_name
    bw2data.projects.delete_project(project_name, True)


def test_indendent_node_methods(random_test_project: str):
    db = bw2data.Database("test-db")
    db.register()
    nw = """name,code,unit,type
product,product,unit,process
energy,energy,MW,process
co2,co2,kg,emission"""

    con = """input,output,type,amount
product,energy,technosphere,2
product,co2,biosphere,4
energy,co2,biosphere,1"""
    build_network(db, {
        "activities": list(DictReader(nw.split("\n"))),
        "exchanges": list(DictReader(con.split("\n")))
    })

    method1 = ('method1',)
    bw_method1 = bw2data.Method(method1)
    bw_method1.write([
        (db.get("co2").key, {'amount': 1})
    ])
    bw_method1.metadata = bw_method1.metadata | {"unit": "kg"}

    method2 = ('method2',)
    bw_method2 = bw2data.Method(method2)
    bw_method2.write([
        (db.get("co2").key, {'amount': 2})
    ])
    bw_method2.metadata = bw_method2.metadata | {"unit": "kg"}

    lca: LCA = LCA({db.get("product"): 1}, method1)
    lca.lci()
    lca.lcia()
    assert 6 == lca.score

    exp = Experiment({
        "adapters": [
            {
                "adapter_name": "brightway-adapter",
                "config": {"bw_project": random_test_project},
                "methods": {
                    "method1": ("method1",),
                    "method2": ("method2",)
                },
            }
        ],
        "hierarchy": {
            "name": "root",
            "aggregator": "sum",
            "children": [
                {
                    "name": "product",
                    "adapter": "bw",
                    "config": {"code": "product"}
                },
                {
                    "name": "energy",
                    "adapter": "bw",
                    "config": {"code": "energy", "default_output": ["MW",2]}
                }
            ]
        }
    })
    _ = exp.run()


def test_all_codes_unique():
    project_name = "test-project_" + str(uuid4())
    print(project_name)
    bw2data.projects.create_project(project_name)
    bw2data.projects.set_current(project_name)
    db = bw2data.Database("test-db")
    db.register()
    for i in range(4):
        code_name = "act" + str(i)
        db.new_activity(code_name, name=code_name).save()
    BrightwayAdapter.assert_all_codes_unique()

    db2 = bw2data.Database("test-db2")
    db2.register()
    code_name = "act1"
    db2.new_activity(code_name, name=code_name).save()

    with pytest.raises(ValueError):
        BrightwayAdapter.assert_all_codes_unique(True)
    bw2data.projects.delete_project(project_name, True)
