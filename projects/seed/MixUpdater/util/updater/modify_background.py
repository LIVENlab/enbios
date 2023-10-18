"""
Created on Tue Sep  5 09:31:22 2023

@author: Alexander de Tomás (ICTA-UAB)
        -LexPascal
"""
import bw2data as bd
from projects.seed.MixUpdater.util.activity_creator import InventoryFromExcel
from projects.seed.MixUpdater.const.const import bw_project,bw_db

bd.projects.set_current(bw_project)            # Select your project
myei = bd.Database(bw_db)        # Select your db


# Create a copy of the database to don't mess it up
"""

try:
    ei.copy('db_experiments') #fes una copia de la db sencera
    myei=bd.Database('db_experiments')
except AssertionError:
    myei=bd.Database('db_experiments')
"""

def ModifyBackground(data, activity_code : str):
    """
    :param data: csv containing the inventory you want to create
    :param activity_code: db code of the activity you want to change // replace
    :return:
    """
    # Create a new activity
    # We're creating a new market for electricity.csv in 2050

    new_activity_code=InventoryFromExcel(data)
    new_activity=myei.get_node(new_activity_code)     #
    act_to_change=myei.get_node(activity_code)        # Activity you want to change ('Classic market for electricity')

    print('upstream before this function')
    for element in new_activity.upstream():
        print(element)


    for exchange in act_to_change.upstream():
        #replace and save
        exchange.input = new_activity
        exchange.save()

        print('an exchange has been changed', exchange)

    print('upstream after the execution of this function')

    for element in new_activity.upstream():
        print(element)
    pass

