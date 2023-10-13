import json
import pandas as pd

from enbios2.base.experiment import Experiment
from projects.seed.MixUpdater.Complements.Calliope_input_cleaner_full_subregions.cleaner import preprocess_calliope
from projects.seed.MixUpdater.Complements.ENBIOS_unit_adapter import unit_adapter


class UpdaterExperiment(Experiment):
    def __init__(self):
        self.techs=[]
        self.scenarios=[]
        self.template_electricity_market = pd.DataFrame()
        pass

    #TODO: project checker

    def preprocess(self,cal_file,moth_file):      #TODO: Give feedback on what's doing behind
        """
        cal_file: str path to flow_out_sum data

        moth_file: path file to excel data (check examples)

        returns:
            -pd.DataFrame: modified Calliope file with the following changes:
                -Unit adaptation
                -Scaling according to the conversion factor
                -Filtered activities contained in the mother file
        ___________________________
        """
        preprocessed=preprocess_calliope(cal_file,moth_file)[1]
        preprocessed_units=unit_adapter(preprocessed,moth_file)
        self.preprocessed = preprocessed_units
        self.techs = preprocessed_units['techs'].unique().tolist() #give some info
        self.scenarios = preprocessed_units['scenarios'].unique().tolist()


    def data_for_ENBIOS(self):
        raise NotImplementedError('Not implemented yet')
        """
        Transform the data into enbios like dictionary
        """

    def template_electricity(self):
        pass


if __name__=='__main__':
    tr=UpdaterExperiment()
    tr.preprocess(r'C:\Users\altz7\PycharmProjects\enbios__git\projects\seed\MixUpdater\Complements\Calliope_input_cleaner_full_subregions\flow_out_sum.csv',r'C:\Users\altz7\PycharmProjects\enbios__git\projects\seed\Data\base_file_simplified.xlsx')


