import bw2data as bd
import pandas as pd
import typing
from projects.seed.MixUpdater.const.const import bw_project,bw_db
from projects.seed.MixUpdater.util.updater.recrusive_dict_changer import inventoryModify

from pathlib import Path
from decimal import Decimal, getcontext


bd.projects.set_current(bw_project)            # Select your project
ei = bd.Database(bw_db)



class Updater():
    def __init__(self,enbios_data,template):
        self.template=template
        self.enbios_data=enbios_data
        pass


    def inventoryModify(self,scenario: str) -> pd.DataFrame:

        """
        This function updates the values of the template inventory.
        ** Update
        :param scenario: str --> scenario to modify
        :return: pandas Dataframe --> df with the market for electricity modified
        """

        df=self.template
        dict=self.enbios_data
        subdict = dict['scenarios'][scenario]['activities']

        for key in subdict.keys():
            name = key
            amount = subdict[key][1]
            for index, row in df.iterrows():
                if row['Activity name'] == name:
                    df.loc[df['Activity name'] == name, 'Amount'] = amount

        df_gruoped = df.groupby('Activity_code')['Amount'].transform('sum')
        df['Amount'] = df_gruoped
        df = df.drop_duplicates(subset='Amount')

        getcontext().prec = 50

        sum_of_column = sum(map(Decimal, df['Amount'][1:]))
        print(sum_of_column)

        df['Amount'] = [1] + [Decimal(x) / sum_of_column for x in df['Amount'][1:]]

        print('Check total', sum(map(Decimal, df['Amount'][1:])))
        # TODO: reference of one


        #TODO CHECK
        self.template=df
        return df



    def exchange_updater(self,code):

        """"
        Opens the bw activity and update the results
        """
        market = ei.get_node(code)
        df=self.template

        # Eval exchanges from market

        for index, row in df.iterrows():
            code_iter = row['Activity_code']
            amount = row['Amount']
            act_iter = ei.get_node(code_iter)
            name = act_iter['name']

            for ex in market.exchanges():
                if name in str(ex.input):
                    old_am = ex['amount']
                    ex['amount'] = float(amount)
                    ex.save()
                    print(f"Amount modified in exchange {name}, moved from {old_am} to {ex['amount']}")

                else:
                    pass






