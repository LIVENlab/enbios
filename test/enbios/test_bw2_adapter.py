import bw2data
import pytest
from bw2data.backends import ActivityDataset, Activity
from pint import UndefinedUnitError
from tqdm import tqdm

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
    experiment_setup["scenario"]["hierarchy"]["children"][0]["config"]["enb_location"] = ("ES", "cat")
    # bw2data.projects.set_current(experiment_setup["scenario"]["adapters"][0]["config"]["bw_project"])
    # with ActivityDataset._meta.database.atomic():
    #     for a in ActivityDataset.select().where(ActivityDataset.type == "process"):
    #         a.data["enb_location"] = ("ES", "cat")
    #         a.save()
    experiment_setup["scenario"]["adapters"][0]["config"]["simple_regionalization"] = {"run_regionalization": True,
                                                                                       "select_regions": {"ES"},
                                                                                       "set_node_regions": {
                                                                                           'b9d74efa4fd670b1977a3471ec010737': (
                                                                                               'ES', 'cat'),
                                                                                           '7705f0e1-5b14-44f4-b330-1245b5c7fc08': (
                                                                                               'ES', 'cat'),
                                                                                           '92391c8c6958ada25b22935e3fa6f06f': (
                                                                                               'ES', 'cat'),
                                                                                           '8285b545c95ff0a2d17eb2bc57cbeaa2': (
                                                                                               'ES', 'cat'),
                                                                                           '08b928d8-1812-4e0b-b057-4bcaaba24865': (
                                                                                               'ES', 'cat'),
                                                                                           'b0546417-3064-4878-bd6f-2da75cefdf63': (
                                                                                               'ES', 'cat'),
                                                                                           '2cbb504a-ce2f-40e9-9d38-e130e95a1242': (
                                                                                               'ES', 'cat'),
                                                                                           '237a5f15-8119-472a-8988-88b7ecb42405': (
                                                                                               'ES', 'cat'),
                                                                                           '20664d0e-24e3-4daa-8c5c-2ade6e0c2723': (
                                                                                               'ES', 'cat'),
                                                                                           '1bb6a502-3ff9-4a79-835c-5588b855f1f5': (
                                                                                               'ES', 'cat'),
                                                                                           '485c39270d3568d82b9b61e9823b4978': (
                                                                                               'ES', 'cat'),
                                                                                           'ffcaa146be28caec479e80630f82994c': (
                                                                                               'ES', 'cat'),
                                                                                           'f9f66d598e98619fccec69712f571261': (
                                                                                               'ES', 'cat'),
                                                                                           '6a903634-c97f-4c49-a7c0-88f0e6ac7a23': (
                                                                                               'ES', 'cat'),
                                                                                           'ebfe261d-ab0d-4ade-8743-183c8c6bdcc6': (
                                                                                               'ES', 'cat'),
                                                                                           '198ce8e3-f05a-4bec-9f7f-325347453326': (
                                                                                               'ES', 'cat'),
                                                                                           'e2fe6ab166bc1dfffdd1680ac38eb899': (
                                                                                               'ES', 'cat'),
                                                                                           '34837fe3-4332-493c-86bb-95a207d9c234': (
                                                                                               'ES', 'cat'),
                                                                                           'e43a270f-4f88-4789-a0b8-7aba56677743': (
                                                                                               'ES', 'cat'),
                                                                                           '2287c8c121103584f07c0c3199220d51': (
                                                                                               'ES', 'cat'),
                                                                                           '7c09916a-b6b5-47f5-bdf6-30d2511beb82': (
                                                                                               'ES', 'cat'),
                                                                                           '77c46f0e-c8c0-4bc3-992c-2a9725d49f70': (
                                                                                               'ES', 'cat'),
                                                                                           '175baa64-d985-4c5e-84ef-67cc3a1cf952': (
                                                                                               'ES', 'cat'),
                                                                                           '39946c56-cdf6-4a22-9ac9-1cd333b65533': (
                                                                                               'ES', 'cat'),
                                                                                           '5ad58fcc-e9ba-4155-a3c9-e4ffb3065a6f': (
                                                                                               'ES', 'cat'),
                                                                                           '748f22a9-eba4-4726-bef5-92c7442ce189': (
                                                                                               'ES', 'cat'),
                                                                                           '4a9e1a0ac89a66b2fe1565cbf2628d9c': (
                                                                                               'ES', 'cat'),
                                                                                           '9d1efa17-070a-4602-a65f-daf5056b0647': (
                                                                                               'ES', 'cat'),
                                                                                           'ef9e2b6a0815008d49a77388e7c5b0e8': (
                                                                                               'ES', 'cat'),
                                                                                           'd2d27f474d08a94b07e13c672ccb527b': (
                                                                                               'ES', 'cat'),
                                                                                           '8d1f644cfb4f2d1e4d69030fe28803e8': (
                                                                                               'ES', 'cat'),
                                                                                           'bdacb58853d6014daecc0de37210bddd': (
                                                                                               'ES', 'cat'),
                                                                                           'd068f3e2-b033-417b-a359-ca4f25da9731': (
                                                                                               'ES', 'cat'),
                                                                                           '1af44724-172c-462b-b277-bb4b2fd32c33': (
                                                                                               'ES', 'cat'),
                                                                                           'baf58fc9-573c-419c-8c16-831ac03203b9': (
                                                                                               'ES', 'cat'),
                                                                                           '6dc1b46f-ee89-4495-95c4-b8a637bcd6cb': (
                                                                                               'ES', 'cat'),
                                                                                           '2494c0b3057d063a4094256b87e9e82e': (
                                                                                               'ES', 'cat'),
                                                                                           '9afa0173-ecbd-4f2c-9c5c-b3128a032812': (
                                                                                               'ES', 'cat'),
                                                                                           '09872080-d143-4fb1-a3a5-647b077107ff': (
                                                                                               'ES', 'cat'),
                                                                                           '9990b51b-7023-4700-bca0-1a32ef921f74': (
                                                                                               'ES', 'cat'),
                                                                                           'a912f450-5233-489b-a2e9-8c029fab480f': (
                                                                                               'ES', 'cat'),
                                                                                           '10571d92-ea97-4ac6-adb6-b25893a631cb': (
                                                                                               'ES', 'cat'),
                                                                                           '36e53653-1338-42c7-816c-f6667809e0b1': (
                                                                                               'ES', 'cat'),
                                                                                           'bfa26b15-5340-441e-acb4-0bb19a4028d3': (
                                                                                               'ES', 'cat'),
                                                                                           'a850e6de-a007-432f-be7f-ce6e2cf1f2ae': (
                                                                                               'ES', 'cat'),
                                                                                           '7a59e8d5-cd11-4ee7-b1ca-30979d2b0b3a': (
                                                                                               'ES', 'cat'),
                                                                                           'bc19ef73b6837d308acc64fbcc91ceef': (
                                                                                               'ES', 'cat'),
                                                                                           '73ed05cc-9727-4abf-9516-4b5c0fe54a16': (
                                                                                               'ES', 'cat'),
                                                                                           'f4d0a2c8-efef-4188-85da-5801097389a2': (
                                                                                               'ES', 'cat'),
                                                                                           '88cde01c-df69-40bb-9b14-6eac71bea5b8': (
                                                                                               'ES', 'cat'),
                                                                                           '230d8a0a-517c-43fe-8357-1818dd12997a': (
                                                                                               'ES', 'cat'),
                                                                                           'aa567547-9821-456a-8412-e726735eeb29': (
                                                                                               'ES', 'cat'),
                                                                                           '247ac273-60fa-4e21-9408-793f75fa1d37': (
                                                                                               'ES', 'cat'),
                                                                                           '98eb1d16-9d7a-4716-9be4-1449341a832f': (
                                                                                               'ES', 'cat'),
                                                                                           '8c52f40c-69b7-4538-8923-b371523c71f5': (
                                                                                               'ES', 'cat'),
                                                                                           '37090bb658283ba9898f9fac13be3430': (
                                                                                               'ES', 'cat'),
                                                                                           'aac39edf-68c5-4f38-9aa9-17e3ca265109': (
                                                                                               'ES', 'cat')}
                                                                                       }
    exp = Experiment(experiment_setup["scenario"])
    regio_res = exp.run()

    experiment_setup["scenario"]["adapters"][0]["config"]["simple_regionalization"] = {"run_regionalization": False,
                                                                                       "select_regions": {"ES"},
                                                                                       "set_node_regions": {
                                                                                           'b9d74efa4fd670b1977a3471ec010737': (
                                                                                               'ES', 'cat'),
                                                                                           '7705f0e1-5b14-44f4-b330-1245b5c7fc08': (
                                                                                               'ES', 'cat'),
                                                                                           '92391c8c6958ada25b22935e3fa6f06f': (
                                                                                               'ES', 'cat'),
                                                                                           '8285b545c95ff0a2d17eb2bc57cbeaa2': (
                                                                                               'ES', 'cat'),
                                                                                           '08b928d8-1812-4e0b-b057-4bcaaba24865': (
                                                                                               'ES', 'cat'),
                                                                                           'b0546417-3064-4878-bd6f-2da75cefdf63': (
                                                                                               'ES', 'cat'),
                                                                                           '2cbb504a-ce2f-40e9-9d38-e130e95a1242': (
                                                                                               'ES', 'cat'),
                                                                                           '237a5f15-8119-472a-8988-88b7ecb42405': (
                                                                                               'ES', 'cat'),
                                                                                           '20664d0e-24e3-4daa-8c5c-2ade6e0c2723': (
                                                                                               'ES', 'cat'),
                                                                                           '1bb6a502-3ff9-4a79-835c-5588b855f1f5': (
                                                                                               'ES', 'cat'),
                                                                                           '485c39270d3568d82b9b61e9823b4978': (
                                                                                               'ES', 'cat'),
                                                                                           'ffcaa146be28caec479e80630f82994c': (
                                                                                               'ES', 'cat'),
                                                                                           'f9f66d598e98619fccec69712f571261': (
                                                                                               'ES', 'cat'),
                                                                                           '6a903634-c97f-4c49-a7c0-88f0e6ac7a23': (
                                                                                               'ES', 'cat'),
                                                                                           'ebfe261d-ab0d-4ade-8743-183c8c6bdcc6': (
                                                                                               'ES', 'cat'),
                                                                                           '198ce8e3-f05a-4bec-9f7f-325347453326': (
                                                                                               'ES', 'cat'),
                                                                                           'e2fe6ab166bc1dfffdd1680ac38eb899': (
                                                                                               'ES', 'cat'),
                                                                                           '34837fe3-4332-493c-86bb-95a207d9c234': (
                                                                                               'ES', 'cat'),
                                                                                           'e43a270f-4f88-4789-a0b8-7aba56677743': (
                                                                                               'ES', 'cat'),
                                                                                           '2287c8c121103584f07c0c3199220d51': (
                                                                                               'ES', 'cat'),
                                                                                           '7c09916a-b6b5-47f5-bdf6-30d2511beb82': (
                                                                                               'ES', 'cat'),
                                                                                           '77c46f0e-c8c0-4bc3-992c-2a9725d49f70': (
                                                                                               'ES', 'cat'),
                                                                                           '175baa64-d985-4c5e-84ef-67cc3a1cf952': (
                                                                                               'ES', 'cat'),
                                                                                           '39946c56-cdf6-4a22-9ac9-1cd333b65533': (
                                                                                               'ES', 'cat'),
                                                                                           '5ad58fcc-e9ba-4155-a3c9-e4ffb3065a6f': (
                                                                                               'ES', 'cat'),
                                                                                           '748f22a9-eba4-4726-bef5-92c7442ce189': (
                                                                                               'ES', 'cat'),
                                                                                           '4a9e1a0ac89a66b2fe1565cbf2628d9c': (
                                                                                               'ES', 'cat'),
                                                                                           '9d1efa17-070a-4602-a65f-daf5056b0647': (
                                                                                               'ES', 'cat'),
                                                                                           'ef9e2b6a0815008d49a77388e7c5b0e8': (
                                                                                               'ES', 'cat'),
                                                                                           'd2d27f474d08a94b07e13c672ccb527b': (
                                                                                               'ES', 'cat'),
                                                                                           '8d1f644cfb4f2d1e4d69030fe28803e8': (
                                                                                               'ES', 'cat'),
                                                                                           'bdacb58853d6014daecc0de37210bddd': (
                                                                                               'ES', 'cat'),
                                                                                           'd068f3e2-b033-417b-a359-ca4f25da9731': (
                                                                                               'ES', 'cat'),
                                                                                           '1af44724-172c-462b-b277-bb4b2fd32c33': (
                                                                                               'ES', 'cat'),
                                                                                           'baf58fc9-573c-419c-8c16-831ac03203b9': (
                                                                                               'ES', 'cat'),
                                                                                           '6dc1b46f-ee89-4495-95c4-b8a637bcd6cb': (
                                                                                               'ES', 'cat'),
                                                                                           '2494c0b3057d063a4094256b87e9e82e': (
                                                                                               'ES', 'cat'),
                                                                                           '9afa0173-ecbd-4f2c-9c5c-b3128a032812': (
                                                                                               'ES', 'cat'),
                                                                                           '09872080-d143-4fb1-a3a5-647b077107ff': (
                                                                                               'ES', 'cat'),
                                                                                           '9990b51b-7023-4700-bca0-1a32ef921f74': (
                                                                                               'ES', 'cat'),
                                                                                           'a912f450-5233-489b-a2e9-8c029fab480f': (
                                                                                               'ES', 'cat'),
                                                                                           '10571d92-ea97-4ac6-adb6-b25893a631cb': (
                                                                                               'ES', 'cat'),
                                                                                           '36e53653-1338-42c7-816c-f6667809e0b1': (
                                                                                               'ES', 'cat'),
                                                                                           'bfa26b15-5340-441e-acb4-0bb19a4028d3': (
                                                                                               'ES', 'cat'),
                                                                                           'a850e6de-a007-432f-be7f-ce6e2cf1f2ae': (
                                                                                               'ES', 'cat'),
                                                                                           '7a59e8d5-cd11-4ee7-b1ca-30979d2b0b3a': (
                                                                                               'ES', 'cat'),
                                                                                           'bc19ef73b6837d308acc64fbcc91ceef': (
                                                                                               'ES', 'cat'),
                                                                                           '73ed05cc-9727-4abf-9516-4b5c0fe54a16': (
                                                                                               'ES', 'cat'),
                                                                                           'f4d0a2c8-efef-4188-85da-5801097389a2': (
                                                                                               'ES', 'cat'),
                                                                                           '88cde01c-df69-40bb-9b14-6eac71bea5b8': (
                                                                                               'ES', 'cat'),
                                                                                           '230d8a0a-517c-43fe-8357-1818dd12997a': (
                                                                                               'ES', 'cat'),
                                                                                           'aa567547-9821-456a-8412-e726735eeb29': (
                                                                                               'ES', 'cat'),
                                                                                           '247ac273-60fa-4e21-9408-793f75fa1d37': (
                                                                                               'ES', 'cat'),
                                                                                           '98eb1d16-9d7a-4716-9be4-1449341a832f': (
                                                                                               'ES', 'cat'),
                                                                                           '8c52f40c-69b7-4538-8923-b371523c71f5': (
                                                                                               'ES', 'cat'),
                                                                                           '37090bb658283ba9898f9fac13be3430': (
                                                                                               'ES', 'cat'),
                                                                                           'aac39edf-68c5-4f38-9aa9-17e3ca265109': (
                                                                                               'ES', 'cat')}
                                                                                       }
    exp = Experiment(experiment_setup["scenario"])
    res = exp.run()
    pass