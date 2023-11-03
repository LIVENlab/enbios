"""
@author: Alexander de Tomás (ICTA-UAB)
        -LexPascal
"""

from projects.seed.MixUpdater.errors.errors import *
from typing import Optional
import pandas as pd
import bw2data.errors
import pandas as pd
pd.options.mode.chained_assignment = None
import bw2data as bd
from pathlib import Path
from tqdm import tqdm
from projects.seed.MixUpdater.const.const import bw_project,bw_db
from typing import Dict,Union
bd.projects.set_current(bw_project)            # Select your project
database = bd.Database(bw_db)        # Select your db



class Cleaner():
    """
    The main objectives of this class are:
        *Clean the input data
        *Modify the units of the input data
    """
    def __init__(self, caliope, motherfile, subregions : [Optional,bool]= False):
        """
        @param caliope: Calliope data, str path
        @param motherfile: Basefile, str path
        @param subregions: BOOL:
            -If true, the cleaning and preprocess of the data, will consider the different subregions
            *ESP_1,ESP_2..
            Default value set as false:
            - It will group the different subregions per country
        """
        self.subregions=subregions
        self.data=caliope # flow_out_sum
        self.mother_file=motherfile # basefile
        self.clean=None # Final output of the Cleaning functions

        # Unit changer
        self.activity_conversion_dictionary : Dict[str,Dict[str,Union[str,int]]]=dict() #Dictionary containing the activity-conversion factor information
        self.clean_modified=None # Final output of the Unit functions
        self.techs_region_not_included=[] #List of the Processors and regions (together) not included in the basefile
    @staticmethod
    def create_df() -> pd.DataFrame:
        """
        Basic template for clean data
        """
        columns = [
            "spores",
            "techs",
            "locs",
            "carriers",
            "unit",
            "flow_out_sum"
        ]
        df = pd.DataFrame(columns=columns)
        return df


    @staticmethod
    def input_checker(data):
        """
        Check whether the input from Calliope follows the expected structure
        data: pd.Dataframe
        """
        expected_cols = set(['spores', 'techs', 'locs', 'carriers', 'unit', 'flow_out_sum'])
        cols = set(data.columns)

        # Search for possible differences. Older versions of calliope produce "spore"
        if "spore" in cols:
            data.rename(columns={'spore': 'spores'}, inplace=True)
            cols = set(data.columns)

        if expected_cols == cols:
            print('Input checked. Columns look ok')
        else:
            raise KeyError(f"Columns {cols} do not match the expected columns: {expected_cols}")

    def changer(self):
        """
        *Assume that the csv is comma separated*
        Group subregions in regions and sum the value for each technology /carrier
        :param df:
        :return:

        """

        print('Adapting input data...')
        try:
            df = pd.read_csv(self.data, delimiter=',')
            self.data=df
        except FileNotFoundError:
            raise FileNotFoundError(f'File {self.data} does not exist. Please check it')

        else:

            self.input_checker(df)
            df = df.dropna()
            # Create an empty df with the expected data
            gen_df = self.create_df()

            scenarios = list(df.spores.unique())
            for scenario in scenarios:
                df_sub = df.loc[df['spores'] == scenario]   # Create a new df with the data from 1 scenario

                df_sub['locs'] = df['locs'].apply(self.manage_regions)
                df_sub = df_sub.groupby(['techs', 'locs']).agg({
                    "spores": "first",
                    "carriers": "first",
                    "unit": "first",
                    "flow_out_sum": "sum"
                }).reset_index()
                gen_df = pd.concat([gen_df, df_sub])




        return gen_df


    def filter_techs(self, df):
        """
        This function filters the Calliope data with the technologies defined in the basefile
        If the name is not defined in the "Processor column, it will be removed
        *Update: the function also filters if the technology with region included is not defined
        """
        df_names=df.copy()
        df_names['tecregion']=df_names['techs'] + df_names['locs']
        df_techs = pd.read_excel(self.mother_file, sheet_name='Processors')
        df_techs['tecregion']= df_techs['Processor']+df_techs['Region']

        techs = df_techs['Processor'].tolist()

        techs_regions=(df_techs['tecregion'].unique().tolist())

        mark_tec = df_names['techs'].isin(techs)
        mark=df_names['tecregion'].isin(techs_regions)

        df_filtered = df_names[mark]
        # Catch some information
        techs_not_in_list = df_names['techs'][~mark_tec].unique().tolist()

        self.techs_region_not_included=df_names['tecregion'][~mark].unique().tolist()
        print(f'The following technologies, are present in the energy data but not in the Basefile:')
        print('Check the following items in order to avoid missing information')
        print(techs_not_in_list)

        return df_filtered


    def manage_regions(self,arg):


        # Special issue for the Portugal Analysis
        if arg == 'ESP-sink':
            region = 'ESP'

        if self.subregions is True:
            # If the used is not interested in having subregions, the location will be the same
            region=str(arg)
        else:
            # If the user is interested in having subregions, get the first part
            # CZE_1 = CZE
            region = arg.split('_')[0]

        return region


    def preprocess_data(self):
        """
        Run different functions of the class under one call
        @return: final_df: calliope data cleaned. Check definitions of the class for more information
        """
        dat = self.changer()
        final_df = self.filter_techs(dat)
        self.clean=final_df
        return final_df


    ##############################################################################
    # This second part focuses on the modification of the units

    def data_merge(self):
        """
        This function reads the Excel "mother file" and generates a dictionary following the structure:

        {technology name :
            { bw_code : [str],
            conversion factor: [int]}}
        """
        df = pd.read_excel(self.mother_file)
        general_dict = {}
        for index, row in df.iterrows():
            name = row['Processor'] + '_' + row['@SimulationCarrier']
            code = row['BW_DB_FILENAME']
            factor = row['@SimulationToEcoinventFactor']
            general_dict[name] = {
                'factor': factor,
                'code': code
            }
        self.activity_conversion_dictionary=general_dict
        return general_dict



    def modify_data(self):
        """
        This function reads the dictionary generated by data_merge, and the flow_out_sum file.
        Applies some transformations:
            *Multiply the flow_ou_sum by the characterization factor
            *Change the unit according to the conversion
        :return: Two files.
            *calliope_function.csv : Intermediate file to check units and techs
            * flow_out_sum_modified.csv : Final csv ready for enbios

            Returns a list of techs to apply the following function "check elements"
        """
        df=self.clean
        # Create a modified column name to match  the names
        print('Preparing to change and adapt the units...')

        df['names2'] = self.join_techs_and_carriers(df)
        df=self.ecoinvent_units_factors(df)
        # Prepare an enbios-like file    cols = ['spores', 'locs', 'techs', 'carriers', 'units', 'new_vals']
        cols = ['spores', 'locs', 'techs', 'carriers', 'units', 'new_vals']
        df = df[cols]
        df.rename(columns={'spores': 'scenarios', 'new_vals': 'flow_out_sum'}, inplace=True)
        df.dropna(axis=0, inplace=True)
        print('Units adapted and ready to go')
        self.clean_modified=df
        return df


    def adapt_units(self):
        self.data_merge()
        modified_units_df=self.modify_data()
        return modified_units_df


    @staticmethod
    def join_techs_and_carriers(df) -> list:
        """
        This function checks joins the tech names and carriers and returns a list
        """
        names_joined = []
        for index, row in df.iterrows():
            try:
                new_name = str(row['techs']) + '_' + str(row['carriers'])
                names_joined.append(new_name)
            except TypeError as e:
                print(f'An error occured in row {row}. Check the type. More info {e}')
                continue
        return names_joined

    def ecoinvent_units_factors(self,df):
        """
        Read the calliope data and extend the information based on self.actvity_conversion dictionary
        *add new columns and apply the conversion factor to the value
        """
        # Create new columns
        #df=df.copy() # avoid modifications during the loop
        df['new_vals'] = None
        df['Units_new'] = None
        df['codes'] = None
        df['names_db'] = None
        # df['flow_out_sum']=[x.replace(',','.') for x in df['flow_out_sum']]
        for key in self.activity_conversion_dictionary.keys():
            code = self.activity_conversion_dictionary[key]['code']
            try:
                activity = database.get_node(code)
                unit = activity['unit']
                act_name = activity['name']
            except bw2data.errors.UnknownObject:
                print(f"{code} from activity, {key} not found. Please check your database")
                continue  # If activity doesn't exists, do nothing
            for index, row in df.iterrows():
                if str(key) == str(row['names2']):
                    factor = (self.activity_conversion_dictionary[key]['factor'])
                    value = float(row['flow_out_sum'])
                    new_val = value * factor
                    df.at[index, 'codes'] = code
                    df.at[index, 'units'] = unit
                    df.at[index, 'new_vals'] = new_val
                    df.at[index, 'names_db'] = act_name
                else:
                    pass
        return df





























