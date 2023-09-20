from projects.seed.MixUpdater.util.modify_background import ModifyBackground
import bw2data as bd
import unittest
import pandas as pd
from projects.seed.MixUpdater.const.const import bw_project,bw_db


bd.projects.set_current(bw_project)            # Select your project
ei = bd.Database(bw_db)        # Select your db
path = r"C:\Users\Administrator\PycharmProjects\enbios2\projects\seed\MixUpdater\Intermediate_data\template_market.csv"
df=pd.read_csv(path)
code_to_check = df['Activity_code'][0]




class TestModifyBackground(unittest.TestCase):
    def setUp(self) -> None:
        """
              Creamos algunas actividaddes falsas que tengan como input otra determinada actividad determinada actividad.
              Esa actividad tambien la creamos y modificamos
              :return:
              """
        try:
            new_act = ei.new_activity(name='act_tier1', code='act_tier1', )
            new_act.save()

            new_act2 = ei.new_activity(name='act_tier2', code='act_tier2', )
            new_act2.save()

            exchange1 = new_act.new_exchange(input=new_act2, amount=10, type="technosphere")
            exchange1.save()


        except bd.errors.DuplicateNode:
            new_act = ei.get_node('act_tier1')
            new_act.delete()
            new_act = ei.new_activity(name='act_tier1', code='act_tier1')
            new_act.save()
            new_act2 = ei.get_node('act_tier2')
            new_act2.delete()
            new_act2 = ei.new_activity(name='act_tier2', code='act_tier2')
            new_act2.save()
            exchange1 = new_act.new_exchange(input=new_act2, amount=10, type="technosphere")
            exchange1.save()



    def test_BackgroundChange(self):
        #before_func
        new_act2 = ei.get_node('act_tier2')
        upstream_before = list(new_act2.upstream())

        ModifyBackground(path,'act_tier2')
        upstream_after=list(new_act2.upstream())
        self.assertNotEqual(str(upstream_before),str(upstream_after),'Upstreams are the same')

        pass
    def CheckAmounts(self): # TODO
        pass
