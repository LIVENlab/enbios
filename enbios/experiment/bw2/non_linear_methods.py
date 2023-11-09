import bw2data

from enbios.ecoinvent import get_ecoinvent_dataset_index
from enbios.base.db_models import EcoinventDataset
from enbios.bw2 import set_bw_current_project


print(get_ecoinvent_dataset_index())

set_bw_current_project(EcoinventDataset._V391, EcoinventDataset._SM_CUTOFF)
# bw2data.DataStore
print(bw2data.databases)