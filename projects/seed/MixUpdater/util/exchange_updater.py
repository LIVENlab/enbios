"""
Created on Thu Sep  7 20:49:53 2023

@author: Alexander de Tomás (ICTA-UAB)
        -LexPascal
"""
import pathlib
import bw2data as bd
import pandas as pd
import typing
from projects.seed.MixUpdater.const.const import bw_project,bw_db
from projects.seed.MixUpdater.util.recrusive_dict_changer import inventoryModify
bd.projects.set_current(bw_project)            # Select your project
ei = bd.Database(bw_db)        # Select your db


df_test= inventoryModify('0')
code='f_test'

def exchange_updater(df : pd.DataFrame ,code : str):

    """"
    Define a function that opens the "Future market for electricity" and update the results
    """
    market=ei.get_node(code)

    # Eval exchanges from market

    for index,row in df.iterrows():
        code_iter=row['Activity_code']
        amount=row['Amount']
        act_iter=ei.get_node(code_iter)
        name=act_iter['name']

        for ex in market.exchanges():
            if name in str(ex.input):
                old_am=ex['amount']
                ex['amount']=float(amount)
                ex.save()
                print(f"Amount modified in exchange {name}, moved from {old_am} to {ex['amount']}")

            else:
                pass

if __name__=='__main__':
    exchange_updater(df_test,code)

