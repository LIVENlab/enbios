"""
Created on Wed Sep  6 12:19:18 2023

@author: Alexander de Tomás (ICTA-UAB)
        -LexPascal
"""

import bw2data as bd
from projects.seed.MixUpdater.const.const import bw_project, bw_db
import json

bd.projects.set_current(bw_project)  # Select your project
ei = bd.Database(bw_db)  # Select your DB


