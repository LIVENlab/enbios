import pandas as pd

from projects.seed.MixUpdater.util.activity_creator import InventoryFromExcel
import bw2data as bd
from projects.seed.MixUpdater.const.const import bw_project,bw_db
bd.projects.set_current(bw_project)            # Select your project
ei = bd.Database(bw_db)

inventory_AWE=pd.read_csv(r'C:\Users\Administrator\PycharmProjects\enbios2\projects\seed\MixUpdater\Inventories\AWE.csv',delimiter=';',on_bad_lines='skip')


inventory_PEM=pd.read_csv(r'C:\Users\Administrator\PycharmProjects\enbios2\projects\seed\MixUpdater\Inventories\PEM_2020.csv',on_bad_lines='skip',delimiter=';')

inventory_market=pd.read_csv(r'C:\Users\Administrator\PycharmProjects\enbios2\projects\seed\MixUpdater\Inventories\market_for_hydrogen.csv',on_bad_lines='skip',delimiter=';')


a=InventoryFromExcel(inventory_AWE)
b=InventoryFromExcel(inventory_PEM)
c=InventoryFromExcel(inventory_market)
