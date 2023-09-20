"""
Created on Thu Sep  7 10:17:35 2023

@author: Alexander de Tomás (ICTA-UAB)
        -LexPascal

We're creating a new inventory for hydrogen combustion using CHP.

We're assuming a wide development of hydrogen in 2050, a 400MW CHP power plant can be operated using hydrogen

--> Required: Preexisting Market for hydrogen in the db

"""
from projects.seed.MixUpdater.util.activity_creator import InventoryFromExcel
import bw2data as bd
from projects.seed.MixUpdater.const.const import bw_project,bw_db
bd.projects.set_current(bw_project)            # Select your project
ei = bd.Database(bw_db)        # Select your db

"""
We're going to use the CHP plant 400MW in Portugal as a Proxy where we're going to change the input of natural gas for hydrogen
"""



chp=ei.get_node(code='a0d0ab65435d0487d33b4be142483d76')

#we want to replace the following exhcange: Exchange: 0.11608779637830263 cubic meter 'market for natural gas, high pressure


#Create a copy, to avoid problems in the db


try:
    chp_copy=chp.copy(name='CHP_HYDROGEN_2050',code='CHP_hydrogen_2050')
    chp_copy.save()
except:
    chp_copy=ei.get_node('CHP_hydrogen_2050')
    chp_copy.delete()
    chp_copy=chp.copy(name='CHP_HYDROGEN_2050',code='CHP_hydrogen_2050')
    chp_copy.save()


# Import the market for hydrogen
market_hydrogen=ei.get_node(code='market_hydrogen_2050_A')

# Replace the exchange of interest
for ex in chp_copy.technosphere():
    if ex.input['name'] =='market for natural gas, high pressure':
        ex.delete()
        print(ex.input['name'], 'has been deleted')
        # Include market for hydrogen as a new input
        exchange=chp_copy.new_exchange(input=market_hydrogen, amount=0.036, type='technosphere')
        exchange.save()

# Remove the biosphere flows of the operation and include stoichiometric water vapor

for bio in chp_copy.biosphere():
    name=str(bio.input)
    if "Water" in name and 'air' in name:
        bio['amount']=0.31133
        bio.save()
    else:
        bio.delete()

for ex in list(chp_copy.biosphere()):
    print(ex)