
"""
Workflow
"""
from projects.seed.MixUpdater.util.update_experiment import UpdaterExperiment

tr = UpdaterExperiment(r'C:\Users\altz7\PycharmProjects\enbios__git\projects\seed\MixUpdater\data\flow_out_sum.csv',
                       r'C:\Users\altz7\PycharmProjects\enbios__git\projects\seed\MixUpdater\data\base_file_simplified_v2.xlsx',
                       'Seeds_exp4', 'db_experiments')
tr.preprocess()
tr.data_for_ENBIOS(smaller_vers=2)
tr.template_electricity('Electricity_generation', Location='PT',
                        Reference_product='electricity production, 2050 in Portugal test', Units='kWh')
tr.updater_run()