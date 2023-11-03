
import json
from typing import Union,Optional
import pandas as pd
import bw2data as bd
from enbios2.base.experiment import Experiment
import os
from projects.seed.MixUpdater.util.preprocess.cleaner import Cleaner
from projects.seed.MixUpdater.util.preprocess.SoftLink import SoftLinkCalEnb
import bw2io as bi
from projects.seed.MixUpdater.const import const
from dataclasses import dataclass
from projects.seed.MixUpdater.util.preprocess.template_market_4_electricity import Market_for_electricity
from projects.seed.MixUpdater.util.updater.background_updater import Updater
import time

@dataclass
class UpdaterExperiment():

    def __init__(self, caliope : Union[str, pd.DataFrame], mother_file: [str], project : [str], database):
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
        self.default_market = None #
        self.project=project
        self.calliope=caliope
        self.mother=mother_file
        self.techs=[]
        self.scenarios=[]
        self.preprocessed=None
        self.preprocessed_starter=None
        self.template_electricity_market = None
        self.SoftLink=None
        self.input=None
        self.database=database
        self.template_code=None
        self.exp=None

        #Check project and db
        self.BW_project_and_DB()

    @staticmethod
    def timer(func):
        def wrapper(*args, **kwargs):
            start = time.time()
            func(*args,**kwargs)
            end = time.time()
            print(f'function {func.__name__} executed in {end - start} seconds')

        return wrapper


    def BW_project_and_DB(self):
        """
        Check the BW project and database.
        It also allows for the creation of a new one
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

    @timer
    def preprocess(self,subregions : [bool,Optional] = False):
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

        # Create an instance of the Cleaner class
        cleaner=Cleaner(self.calliope,self.mother,subregions)
        self.preprocessed_starter=cleaner.preprocess_data()
        self.preprocessed_units=cleaner.adapt_units()
        self.exluded_techs_and_regions=cleaner.techs_region_not_included
        pass

       # preprocessed_units=unit_adapter(self.preprocessed_starter, self.mother)
        #self.preprocessed = preprocessed_units
        #self.techs = preprocessed_units['techs'].unique().tolist() #give some info
        #self.scenarios = preprocessed_units['scenarios'].unique().tolist()



    def data_for_ENBIOS(self, path_save=None,smaller_vers=None):
        """
        Transform the data into enbios like dictionary
        """

        # TODO: Consider saving it in a default folder rather than texting it

        self.SoftLink=SoftLinkCalEnb(self.preprocessed,self.mother,smaller_vers)
        self.SoftLink.run(path_save)
        self.enbios2_data = self.SoftLink.enbios2_data
        self.save_json_data(self.enbios2_data, path_save)



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
        self.template_code=Activity_code


    def classic_run(self):

        general_path=self.path_saved
        self.exp = Experiment(general_path)
        self.exp.run()
        result=self.exp.result_to_dict()



    def updater_run(self):


        # check if template created

        if self.template_code is None:
            raise TypeError(
                f'An error occurred. The template for electricity is {self.template_code}. Please, consider running {self.template_electricity.__name__} before')

        general = self.enbios2_data
        general_path = self.path_saved
        scenarios = list(general['scenarios'].keys())
        try:
            exp = Experiment(general_path)
        except Exception as e:
            # Generally the exception is the unspecificEcoinvent error from ENBIOS
            from enbios2.base.unit_registry import ecoinvent_units_file_path
            text_to_write = 'unspecificEcoinventUnit = []'
            # Abre el archivo en modo escritura ('w')
            with open(ecoinvent_units_file_path, 'w') as file:
                file.write(text_to_write)
            print(f'error {e} covered and solved')
            exp=Experiment(general_path)
            pass



        updater=Updater(general,self.default_market)

        for scenario in scenarios:
            print(f'parsing scenario {scenario}')

            template=updater.inventoryModify(scenario)
            self.template_electricity_market=template # update the table
            updater.exchange_updater(self.template_code)

            exp.run_scenario(scenario)


        pass


    def save_json_data(self,data, path):
        if path is not None:
            try:
                with open(path, 'w') as file:
                    json.dump(data, indent=4)
                self.path_saved=path
            except FileNotFoundError:
                raise FileNotFoundError(f'Path {path} does not exist. Please check it')
        else:
            current=os.path.dirname(os.path.abspath(__file__))
            folder_path=os.path.join(os.path.dirname(current),'Default')

            print(current)

            os.makedirs(folder_path,exist_ok=True)
            file_path=os.path.join(folder_path,'data_enbios.json')
            print(file_path)

            with open(file_path, 'w') as file:
                json.dump(data, file,indent=4)
            print(f'Data for enbios saved in {file_path}')
            self.path_saved=file_path







if __name__=='__main__':
    tr=UpdaterExperiment(r'C:\Users\altz7\PycharmProjects\enbios__git\projects\seed\MixUpdater\data\flow_out_sum.csv',r'C:\Users\altz7\PycharmProjects\enbios__git\projects\seed\MixUpdater\data\base_file_simplified.xlsx','Seeds_exp4','db_experiments')
    tr.preprocess()
    tr.data_for_ENBIOS()
    tr.template_electricity('Electricity_generation', Location='PT', Reference_product='electricity production, 2050 in Portugal test',Units='kWh')
    tr.run()


