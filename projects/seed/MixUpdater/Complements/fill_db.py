"""
This file applies some functions and modifications in order to include the new market for electricity

"""
import bw2data as bd
from projects.seed.MixUpdater.const.const import bw_project,bw_db
from projects.seed.MixUpdater.util.template_electricity import get_list,template_market_4_electricity
from projects.seed.MixUpdater.util.modify_background import ModifyBackground
bd.projects.set_current(bw_project)            # Select your project
ei = bd.Database(bw_db)        # Select your db

aa = get_list("hierarchy", "Energysystem", "Generation", "Electricity_generation")
cosa = template_market_4_electricity(aa, Location='PT', Activity_name="Future market for electricity",
                                     Activity_code="f_test",
                                     Reference_product="electricity production, 2050 in Portugal", Unit='kWh',
                                     Database=bw_db)


# Add the activity in the db & modify the classic background
ModifyBackground(cosa,"bf585acd91a45979fe0fdfd2616ed600")

# With this piece of code, we already have the future market for electricity in the db

#now you can include hydrogen