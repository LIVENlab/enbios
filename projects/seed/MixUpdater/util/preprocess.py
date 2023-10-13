import json
import pandas as pd

from enbios2.base.experiment import Experiment
from projects.seed.MixUpdater.Complements.Calliope_input_cleaner_full_subregions.cleaner import preprocess_calliope
from projects.seed.MixUpdater.Complements.ENBIOS_unit_adapter import unit_adapter
from projects.seed.MixUpdater.util.enbios_input import SoftLinkCalEnb




class UpdaterExperiment(SoftLinkCalEnb):
    def __init__(self,caliope,mother):


        self.calliope=caliope
        self.mother=mother
        self.techs=[]
        self.scenarios=[]
        self.preprocessed=None
        self.template_electricity_market = pd.DataFrame()
        self.SofLink=None

        pass

    # Create an instance for the Softlink


    def preprocess(self):      #TODO: Give feedback on what's doing behind
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
        preprocessed=preprocess_calliope(self.calliope,self.mother)
        self.preprocessed_1=preprocessed
        preprocessed_units=unit_adapter(preprocessed,self.mother)
        self.preprocessed = preprocessed_units
        self.techs = preprocessed_units['techs'].unique().tolist() #give some info
        self.scenarios = preprocessed_units['scenarios'].unique().tolist()


    def data_for_ENBIOS(self):
        self.SofLink=SoftLinkCalEnb(self.preprocessed,self.mother)
        self.SoftLink.run()


        """
               Transform the data into enbios like dictionary
        """

        raise NotImplementedError('Not implemented yet')



    def template_electricity(self):
        raise NotImplementedError('Not implemented yet. Working on it')
        pass


if __name__=='__main__':
    tr=UpdaterExperiment(r'C:\Users\Alex\PycharmProjects\pythonProject\enbios_2\projects\seed\MixUpdater\Complements\Calliope_input_cleaner_full_subregions\flow_out_sum.csv',r'C:\Users\Alex\PycharmProjects\pythonProject\enbios_2\projects\seed\MixUpdater\Complements\base_file_simplified.xlsx')
    tr.preprocess()
    tr.run()


