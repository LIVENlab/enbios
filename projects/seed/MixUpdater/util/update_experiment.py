import dataclasses
import json
import pandas as pd
import bw2data as bd
from enbios2.base.experiment import Experiment

from projects.seed.MixUpdater.util.preprocess.cleaner import preprocess_calliope
from projects.seed.MixUpdater.util.preprocess.ENBIOS_unit_adapter import unit_adapter
from projects.seed.MixUpdater.util.preprocess.SoftLink import SoftLinkCalEnb
import bw2io as bi
from projects.seed.MixUpdater.const import const
from dataclasses import dataclass
from projects.seed.MixUpdater.util.updater.exchange_updater import exchange_updater
from projects.seed.MixUpdater.util.updater.recrusive_dict_changer import inventoryModify
from projects.seed.MixUpdater.util.preprocess.template_market_4_electricity import Market_for_electricity


@dataclass
class UpdaterExperiment(SoftLinkCalEnb):

    def __init__(self,caliope : str | pd.DataFrame, mother_file: [str],project : [str],database):
        """

        @param caliope: path to the caliope data (flow_out_sum.csv)
        @type caliope: either str path or pd.Dataframe
        @param mother_file: path to the mother file
        @type mother_file: str
        @param project: project name in bw
        @type project: str
        @param database: db name in bw
        @type database: str
        """
        self.default_market = None
        self.project=project
        self.calliope=caliope
        self.mother=mother_file
        self.techs=[]
        self.scenarios=[]
        self.preprocessed=None
        self.template_electricity_market = None
        self.SoftLink=None
        self.input=None
        self.database=database

        #Check project and db
        self.BW_project_and_DB()





    def BW_project_and_DB(self):
        """
        Check the BW project and database. Also allow for the creation of a new one
        """
        projects=list(bd.projects)

        if self.project not in str(projects):
            ans=input(f'Project {self.project} not in projects. Want to create a new project? (y/n)')
            if ans =='y':
                database=input('Enter the DB name that you want to create:')
                spolds=input('Enter path to the spold files ("str"):')
                self.create_BW_project(self.project, database,spolds)
                const.bw_project=self.project
                const.bw_db=database
            else:
                raise Warning('Please, create a project before continue')

        bd.projects.set_current(self.project)
        if self.database not in list(bd.databases):
            print(list(bd.databases))
            raise Warning(f"database {self.database} not in bw databases")
        print('Project and Database existing...')

    @staticmethod
    def create_BW_project(project,database,spolds):
        """
        Create a new project and database
        """
        bd.projects.set_current(project)
        bi.bw2setup()
        ei=bi.SingleOutputEcospold2Importer(spolds,database,use_mp=False)
        ei.apply_strategies()
        ei.write_database()
        pass

    def preprocess(self):
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



    def data_for_ENBIOS(self, path_save=None):
        """
        Transform the data into enbios like dictionary
        """

        self.SoftLink=SoftLinkCalEnb(self.preprocessed,self.mother)
        self.SoftLink.run(path_save)
        self.enbios2_data = self.SoftLink.enbios2_data



    @property
    def tree(self):
        """
        make the tree accesible
        """
        return self.enbios2




    def template_electricity(self, final_key,Location='Undefined',Reference_product=None,
        Activity_name='Future_market_for_electricity', Activity_code='FM4E'
        ,Units=None):
        """
        This function creates the template activity for the market for electricity using the data of enbios.
        It gets all the activities with _electicity_ in the alias

        @param final_key: Key of the enbios dictionary opening the electricity activities
        @type final_key:  str
        @param Location:
        @param Activity_name: Save the market under this name in SQL database
        @param Activity_code: "
        @param Reference_product:
        @type Reference_product:
        @param Units: units of the activity
        """
        market_class=Market_for_electricity(self.enbios2_data)
        self.electricity_activities = market_class.get_list(final_key)
        self.default_market = market_class.template_market_4_electricity(Location,
                                                   Activity_name,
                                                   Activity_code,
                                                   Reference_product,
                                                   Units)



    def run(self):
        # TODO: Implement
        general=self.enbios2_data

        scenarios = list(general['scenarios'].keys())
        # reduce for testing
        # scenarios= scenarios[:2]
        # TODO change
        exp = Experiment(general)
        for scen in scenarios:
            print(scen)
            """
            
            template = inventoryModify(scen)
            exchange_updater(template, 'f_test')

            exp.run_scenario(scen)

            result = exp.result_to_dict()
            with open(path_save_json, 'w') as outfile:
                json.dump(result, outfile, indent=4)

            exp.results_to_csv(path_save_csv, scen)
       
        """


if __name__=='__main__':
    tr=UpdaterExperiment(r'C:\Users\altz7\PycharmProjects\enbios__git\projects\seed\MixUpdater\data\flow_out_sum.csv',r'C:\Users\altz7\PycharmProjects\enbios__git\projects\seed\MixUpdater\data\base_file_simplified.xlsx','Seeds_exp4','db_experiments')
    tr.preprocess()
    tr.data_for_ENBIOS()
    tr.template_electricity('Electricity_generation', Location='PT', Reference_product='electricity production, 2050 in Portugal test',Units='kWh')
    tr.run()

