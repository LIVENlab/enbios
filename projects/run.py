
"""
Workflow
"""
from projects.seed.MixUpdater.util.update_experiment import UpdaterExperiment

"""
-LexPascal
"""

# 1: Create an instance of the class UpdaterExperiment.
"""
You need to pass 4 parameters:
    *path to the "flow_out_sum"
    *path to the base_file
    *bw project name
    *bw db name
-If the project or db not in bw, it will create one.
"""
enbios_mod = UpdaterExperiment(r'C:\Users\altz7\PycharmProjects\enbios__git\projects\seed\MixUpdater\data\flow_out_sum.csv',
                       r'C:\Users\altz7\PycharmProjects\enbios__git\projects\seed\MixUpdater\data\base_file_simplified_v2.xlsx',
                       'Seeds_exp4', 'db_experiments')


# 2. Preprocess the data
"""
This function does some basic adaptation of the data, such as unit conversions, cleaning of technologies etc
"""
enbios_mod.preprocess()

# 3. Create the input for enbios
"""
ENBIOS uses a dictionary which contains activities, scenarios and dendrogram.
This function automatically creates the dictionary based on the different information defined in the mother file
    * You can specify smaller vers if you want to reduce the number of scenarios for testing purposes 
"""
enbios_mod.data_for_ENBIOS(smaller_vers=2)

# 4. Create the template for the new market for electricity
"""
The main goal of this class is to run ENBIOS updating the electricity mix depending on each scenario
This function creates a template of the new market for electricity with amounts=1.

    * -The first parameter, set here as a "Electricity generation", is the name of the dendrogram right before the electricity activities
    * -Location: place of the market
    * -Reference product: Reference product of your activity
    *-Units
    * You can also add (optionally) the activity name and code for the database
"""

enbios_mod.template_electricity('Electricity_generation', Location='PT',
                        Reference_product='electricity production, 2050 in Portugal test', Units='kWh')


# 5. Run

"""
You can also run the simulation without the modification of the db with self.normal_run()
Once the process is finished, you can explore in the console the results (self.Experiment.results), and export them to dict
"""
enbios_mod.updater_run()