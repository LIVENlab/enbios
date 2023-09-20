""""
This experiment includes subregions and generates the enbios object only once
"""

import json

from enbios2.base.experiment import Experiment
from enbios2.generic.files import DataPath
from projects.seed.MixUpdater.util.recrusive_dict_changer import inventoryModify
from projects.seed.MixUpdater.util.exchange_updater import exchange_updater
import bw2data as bd
from projects.seed.MixUpdater.const.const import bw_project,bw_db
bd.projects.set_current(bw_project)            # Select your project
ei = bd.Database(bw_db)
#
json_general=r'C:/Users/Administrator/PycharmProjects/enbios2/projects/seed/Data/enbios_input_subregions.json'


with open(json_general,'r') as file:
    general=json.load(file)


scenarios=list(general['scenarios'].keys())
#reduce for testing
#scenarios= scenarios[:2]
# TODO change
input = DataPath("C:/Users/Administrator/PycharmProjects/enbios2/projects/seed/Data/enbios_input_subregions.json")

input_data = input.read_data()

exp = Experiment(input_data)
for scen in scenarios:
    template=inventoryModify(scen)
    exchange_updater(template,'f_test')

    exp.run_scenario(scen)

    path_save_csv=r'C:\Users\Administrator\PycharmProjects\enbios2\projects\seed\MixUpdater\results\subregion' + '/'+str(scen)+'.csv'
    path_save_json=r'C:\Users\Administrator\PycharmProjects\enbios2\projects\seed\MixUpdater\results\subregion' + '/'+str(scen)+'.json'

    result=exp.result_to_dict()
    with open(path_save_json,'w') as outfile:
      json.dump(result,outfile, indent=4)

    exp.results_to_csv(path_save_csv, scen)





