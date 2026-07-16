"""
@LexPascal
"""
import json
import os
from pathlib import Path
import time
from typing import Union, Optional, List
import pandas as pd
import bw2data as bd
import bw2io as bi
import warnings
import logging
from pathlib import Path
from Sparks_functions.util.preprocess.cleaner import Cleaner
from Sparks_functions.util.preprocess.SoftLink import SoftLinkCalEnb
from Sparks_functions.util.develop_basefile import Support
from Sparks_functions.generic.generic_dataclass import BaseFileActivity
from Sparks_functions.const import const
from Sparks_functions.util.logger.logger_config import setup_logging

setup_logging(verbosity="INFO")

# Use the project logger directly
import logging
logger = logging.getLogger("sparks")  # always write to sparks.log

class SoftLink():

    def __init__(self,
                 file_path: str,
                 project : str,
                 ):
        """
        @file_path: str
        Path to the folder with the files to be used
        @project: str
        Name of the bw project
        @multiple_codes: bool
        Boolean: when True, it allows the possibility of using two bw codes for one single activity
        This is necessary in cases where you want to consider operation and construction separately
        """
        logger.info('Initiating Sparks_functions')

        self.project = project
        self.file_path = file_path
        self.SoftLink = None
        self._arrange_paths()
        self._bw_project_and_DB()
        self._cleaner = None

        
    

    @staticmethod
    def timer(func):
        def wrapper(*args, **kwargs):
            start = time.time()
            func(*args,**kwargs)
            end = time.time()
            print(f'function {func.__name__} executed in {end - start} seconds')
        return wrapper


    def _bw_project_and_DB(self):
        """
        Check the BW project and database.
        It also allows for the creation of a new one
        """
        projects=list(bd.projects)
        if self.project not in str(projects):
            raise AssertionError(f'Project {self.project} not in BW projects. Please, create it before continuing')

        bd.projects.set_current(self.project)
        self._save_const(self.project)

        logger.info(f"Project {self.project} exists")


    @staticmethod
    def _create_BW_project(project, database, spolds):
        """Create a new project and database"""
        logger.info(f"Creating project {project} and database {database}, coming from {spolds}")
        bd.projects.set_current(project)
        bi.bw2setup()
        ei=bi.SingleOutputEcospold2Importer(spolds,database,use_mp=False)
        ei.apply_strategies()
        ei.write_database()



    def _arrange_paths(self)-> None:
        """
         - Checks for the presence of a mandatory file 'basefile.xlsx' and assigns it to `self.basefile_path`.
        - Stores paths to all other files in the directory in a dictionary (`self.paths_dict`).
        """
        self.file_path = Path(self.file_path).resolve()  # Asegura que sea absoluta
        basefile_path = self.file_path / "basefile.xlsx"

        if basefile_path.exists():
            self.mother = basefile_path
        else:
            raise FileNotFoundError(
                f"The mandatory file 'basefile.xlsx' is missing in the directory: {self.file_path}")

        # Store other paths in a dictionary
        self.paths_dict = {
            file_name: os.path.join(self.file_path, file_name)
            for file_name in os.listdir(self.file_path)
            if os.path.isfile(os.path.join(self.file_path, file_name))
        }
        
        logger.debug(f"Basefile directory in {self.mother}")


    @timer
    def preprocess(self,
                   national: bool = False,
                   specify_database: bool = False,
                   additional_columns: Optional[List[str]] = None
                   ) -> pd.DataFrame:

            """
            Preprocess the dataset according to the specified flags.

            Parameters
            ----------
            national : bool, optional
                Whether to apply national-level preprocessing (default is False).
                It aggregates energy data per country. Must specify country level in the monther file
               .

            specify_database : bool, optional
                Whether to include logic for specifying or filtering by database (default is False).
                If true, it will look for the database column in the mother file

            additional_columns : list of str, optional
                List of additional column names to include or process (default is None).

        returns:
            -pd.DataFrame: modified Calliope file with the following changes:
                -Unit adaptation
                -Scaling according to the conversion factor
                -Filtered activities contained in the mother file

        """
        # Create an instance of the Cleaner class
            #if national:
            #    raise NotImplementedError('This function has not been implemented yet')
            logger.debug(f"Information passed to the Cleaner object: {national=}, {specify_database=}, {additional_columns=}")


            self._cleaner = Cleaner(
                    motherfile=self.paths_dict['basefile.xlsx'],
                    file_handler=self.paths_dict,
                    national=national,
                    specify_database = specify_database,
                    additional_columns= additional_columns
                )

            
            self._cleaner.preprocess_data()
            self.preprocessed_units= self._cleaner.adapt_units()
            self.exluded_techs_and_regions = self._cleaner.techs_region_not_included


    def data_for_ENBIOS(self, path_save: str = None, smaller_vers: bool = False):
        """
        Transform the data into enbios like dictionary
        """
        # Create an instance of the SoftLInkCalEnb
        self.SoftLink = SoftLinkCalEnb(
            calliope=self.preprocessed_units,
            motherfile=self.mother,
            mother_data=self._cleaner.base_activities,
            sublocations=self._cleaner._techs_sublocations,
            smaller_vers=smaller_vers
        )

        self.SoftLink.run(path_save)
        self.enbios2_data = self.SoftLink.enbios2_data #TODO: FIX exluded_techs_and_regions
        self._save_json_data(self.enbios2_data, path_save)


    def _save_json_data(self,data, path: str):
        if path is not None:
            try:
                with open(path, 'w') as file:
                    json.dump(data,file, indent=4)
                self.path_saved=path
            except FileNotFoundError:
                raise FileNotFoundError(f'Path {path} does not exist. Please check it')
        else:
            current=os.path.dirname(os.path.abspath(__file__))
            folder_path=os.path.join(os.path.dirname(current),'Default')

            os.makedirs(folder_path,exist_ok=True)
            file_path=os.path.join(folder_path,'data_enbios.json')
            print(file_path)
            with open(file_path, 'w') as file:
                json.dump(data, file,indent=4)
            print(f'Data for enbios saved in {file_path}')
            self.path_saved=file_path


    @staticmethod
    def _save_const(project: str):
        """Get the project name and store it in a const.py file."""
        try:

            current_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))

            file_path = os.path.join(current_dir, "Sparks_functions", "const", "const.py")

            os.makedirs(os.path.dirname(file_path), exist_ok=True)

            logger.debug(f"Updating constant values in: {file_path}")

            with open(file_path, 'w') as f:
                f.write(f"bw_project = '{project}'\n")


        except Exception as e:
            raise FileNotFoundError(f"Failed to write file: {e}")



    def sup_basefile(self,
                    file_path: str =  r'testing\basefile_dev',
                    gen_file : str = r'tech_mapping.xlsx' ) -> pd.DataFrame:
        """
        transform tech_mapping files into basefiles.
        See tech_mapping.xlsx for an example 

        path: str --> path to the folder containing the data files
        gen_file: str --> path to the excel file with the basic information to be transformed
        """
        
        paths_dict = {
            file_name: os.path.join(file_path, file_name)
            for file_name in os.listdir(file_path)
            if os.path.isfile(os.path.join(file_path, file_name))
        }
        
        sup = Support(file= gen_file,
         project = self.project,
        calliope = paths_dict)
        sup.run()

        return sup.df

        










