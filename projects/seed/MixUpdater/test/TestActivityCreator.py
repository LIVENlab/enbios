from projects.seed.MixUpdater.util.activity_creator import InventoryFromExcel
import bw2data as bd
import unittest
import pandas as pd
from projects.seed.MixUpdater.const.const import bw_project,bw_db


bd.projects.set_current(bw_project)            # Select your project
ei = bd.Database(bw_db)        # Select your db

path = r"C:\Users\Administrator\PycharmProjects\enbios2\projects\seed\MixUpdater\Intermediate_data\template_market.csv"
df=pd.read_csv(path)
code_to_check = df['Activity_code'][0]



class TestActivityCreator(unittest.TestCase):
    def setUp(self) -> None:
        InventoryFromExcel(path)

    def test_add_activity(self):
        """
        Check if Inventory from excel creates an activity in the database
        :return:
        """

        act=ei.get_node(code_to_check)
        self.assertIsNotNone(act,"La activitdad no se ha agregado correctamente")
    def test_exchanges(self):

        """
        See if the upstream of one activity of the df goes to the activity created
        """
        act=ei.get_node(code_to_check)
        name=act['name']
        act_df = df.loc[df['Act_to'] == name]
        code_to_check_upstream=act_df['Activity_code'].iloc[0]
        act_to_upstream=ei.get_node(code_to_check_upstream)
        for element in list(act_to_upstream.upstream()):
            element=str(element)
            if name in element:
                word_found= True
                break
        self.assertTrue(word_found,f"Word {name} not in {act_to_upstream['name']}")
    def test_return_code(self):
        """
        Inventory from Excel returns a list with the code of the activity created.
        Check if so
        """
        check_code=InventoryFromExcel(path)
        theoric_code=df['Activity_code'].iloc[0]
        self.assertEqual(code_to_check,theoric_code,f"Codes {theoric_code},{check_code} are not equal")

    def test_LCA(self):
        """
        If the activity creation is well-defined, the LCA of the functional unit should be equal to the sum of its inputs
        Let's test it
        """
        df=pd.read_csv(r'C:\Users\Administrator\PycharmProjects\enbios2\projects\seed\MixUpdater\Intermediate_data\template_market_4test.csv')
        # Create the activities in the db
        activ_code=InventoryFromExcel(df)

        activity_codes = [code for code in df['Activity_code'] if str(code) != 'f_test_test']
        method= ('CML v4.8 2016', 'climate change', 'global warming potential (GWP100)')
        results_simple = {}
        for act in activity_codes:
            activity = ei.get_node(act)
            name = activity['name']
            results_simple[str(name)] = {}
            lca_obj = activity.lca(amount=1, method=method)
            results_simple[name]['result'] = lca_obj.score

        total_result = sum(item['result'] for item in results_simple.values())

        # Do it for the general

        results_global = {}

        activity = ei.get_node(act)
        name = activity['name']
        results_global[str(name)] = {}
        lca_obj = activity.lca(amount=1, method=method)
        results_global[name]['result'] = lca_obj.score

        total_result_global = sum(item['result'] for item in results_simple.values())
        self.assertEqual(total_result,total_result_global,'Results are not equal')


        pass

if __name__=='__main__':

    unittest.main()

    pass