import bw2data

from enbios2.ecoinvent import get_ecoinvent_dataset_index
from enbios2.base.db_models import EcoinventDataset
from enbios2.bw2 import set_bw_current_project


print(get_ecoinvent_dataset_index())

set_bw_current_project(EcoinventDataset._V391, EcoinventDataset._SM_CUTOFF)
# bw2data.DataStore
print(bw2data.databases)