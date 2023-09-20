from projects.seed.MixUpdater.util.modify_background import ModifyBackground
import bw2data as bd
import unittest
import pandas as pd
from projects.seed.MixUpdater.const.const import bw_project, bw_db
import json
from enbios2.base.experiment import Experiment
from enbios2.generic.files import DataPath
from projects.seed.MixUpdater.util.modify_background import ModifyBackground
from projects.seed.MixUpdater.util.recrusive_dict_changer import inventoryModify

from projects.seed.MixUpdater.const.const import bw_project, bw_db

bd.projects.set_current(bw_project)  # Select your project
ei = bd.Database(bw_db)  # Select your db

json_general = r'C:/Users/Administrator/PycharmProjects/enbios2/projects/seed/Data/enbios_input_3.json'

with open(json_general, 'r') as file:
    general = json.load(file)

scenarios = list(general['scenarios'].keys())
# reduce for testing
scenarios = scenarios[:2]


class TestCalliopeMixer(unittest.TestCase):
    """
    In this test we're checking that enbios runs and in every iteration the db is different

    """

    def setUp(self) -> None:
        pass

    def test_DB_Change(self):

        exchange_to_check = None
        counter = 0
        result_1 = None
        result_2 = None

        for scen in scenarios:

            template = inventoryModify(scen)
            ModifyBackground(template, 'bf585acd91a45979fe0fdfd2616ed600')  # Market for electricity, high voltage, PT
            input = DataPath("C:/Users/Administrator/PycharmProjects/enbios2/projects/seed/Data/enbios_input_3.json")
            input_data = input.read_data()
            exp = Experiment(input_data)
            exp.run_scenario(scen)

            act_check = ei.get_node("f_test")  # Check the future market for electricity
            for ex in act_check.exchanges():
                while counter < 1:
                    # Get the first exchange. Get the name (input) and the amount
                    exchange_to_check = str(ex.input)
                    result_1 = ex['amount']
                    counter += 1

                else:
                    # get the same exchange as before
                    if str(ex.input) == exchange_to_check:
                        name2=ex.input
                        result_2 = ex['amount']
                    else:
                        continue

        self.assertNotEqual(result_1, result_2, f"Results from {exchange_to_check} with {result_1}, is equal to {result_2} from {name2}")



