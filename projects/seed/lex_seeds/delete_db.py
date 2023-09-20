#
#
# bw2data.projects.set_current("ecoinvent")
# seeds_db = bw2data.Database("seeds")
#
# for act in seeds_db:
#     print(act)


"""
first experiment
"""
from logging import getLogger

import bw2data
import bw2data as bd
from bw2data.backends import ExchangeDataset

from enbios2.bw2.util import report

getLogger("peewee").setLevel("ERROR")

# data_path = DataPath("temp/seeds")
# processors_path=data_path / 'base_file_simplified.xlsx'
# calliope=data_path / 'flow_out_sum_modified.csv'
# dict_path=data_path /'dict.json'
#
#
# data=openpyxl.load_workbook(processors_path)
# # processors=data.active
# bd.projects.set_current('ecoinvent')
report()
seeds_db = database= bw2data.Database('seeds')
# print(seeds_db)
seeds_db.delete_data()
seeds_db.delete_instance()
delete_exchanges = 0
for exc in list(ExchangeDataset.select().where(ExchangeDataset.input_database == "seeds" | ExchangeDataset.output_database == "seeds")):
    delete_exchanges += exc.delete_instance()

print(delete_exchanges)
#
main_db =database=bw2data.Database('cutoff_3.9.1_default')

main_db.depends


# seeds_db.delete()

#
# print(bd.projects.dir)
#
# processors=pd.read_excel(processors_path, sheet_name='BareProcessors simulation')
#
# activities_cool={}
# for index,row in processors.iterrows():
#     code=str(row['BW_DB_FILENAME'])
#
#     print('im parsing', str(row['Processor']), code)
#     try:
#         act=database.get_node(code)
#
#     except UnknownObject:
#         print(row['Processor'],'has an unknown object')
#         pass
#
#     name=act['name']
#     unit=act['unit']
#     alias=str(row['Processor'])+'_'+str(row['@SimulationCarrier'])
#     print(alias)
#
#
#     activities_cool[alias]={
#         'name': name,
#         'code': code,
#     }
#
# activities_cool

