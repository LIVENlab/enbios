import bw2data as bd
import pandas as pd
import time
from tqdm import tqdm
from projects.seed.MixUpdater.const.const import bw_project,bw_db


bd.projects.set_current(bw_project)            # Select your project
ei = bd.Database(bw_db)        # Select your db

all_methods=list(bd.methods)
results=[n for n in all_methods if 'ReCiPe 2016 v1.03, midpoint (H)' in str(n) ]
for element in results:
  print(element)

"""


for ex in new_act.exchanges():
  print(ex.input)
  if 'act_tier2' in str(ex.input):
    print('amount is at the start:', ex['amount'])
    ex['amount']=2
    ex.save()
print(list(new_act.exchanges()),'modified')

"""



