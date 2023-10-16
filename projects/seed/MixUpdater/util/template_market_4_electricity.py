import pprint
import json
import random
from logging import getLogger
import bw2data as bd
import openpyxl
import pandas as pd
from bw2data.errors import UnknownObject
from projects.seed.MixUpdater.const.const import bw_project,bw_db
getLogger("peewee").setLevel("ERROR")
bd.projects.set_current(bw_project)            # Select your project
ei = bd.Database(bw_db)

class Market_for_electricity():

    def __init__(self,enbios_data):
        self.enbios_data= enbios_data


    @staticmethod
    def create_template_df():
        """
        :return: a template df of the excel like inventory
        """
        column_names = ["Amount",
                        "Location",
                        "Activity name",
                        "Activity_code",
                        "Reference_product",
                        "Unit",
                        "Act_to",
                        "Technosphere",
                        "Database",
                        "Unit_check"]
        # Update --> add a unit check to verify the units of the inventory
        df = pd.DataFrame(columns=column_names)
        return df


    def get_list(self,final_key, present_dict=None):
        """
        :param
            -args: We're looking for the activities under that define a "future market for electricity". Hence, we specify
                    the keys that point something like "Electricity generation" in the hierarchy
        :return:  list of the activities failing into the electricity_market
        """
        present_dict=self.enbios_data


        keys_to_check = [present_dict]  # Lista de diccionarios que se están explorando

        while keys_to_check:
            current_dict = keys_to_check.pop()  # Tomar el siguiente diccionario
            for key, value in current_dict.items():
                if key == final_key and isinstance(value, list):
                    self.electricity_list=value
                    return value
                if isinstance(value, dict):
                    keys_to_check.append(value)
        return None



    def template_market_4_electricity(self,*args,**kwargs):
        """
        This function returns a template of the "default_market_for electricity" in order to create an inventory.

        Check the template_market.csv to do some changes

        :param market_el_list:
        :param Location:
        :param Activity_name:
        :param Activity_code:
        :param Reference_product:
        :param Unit:
        :param Database:
        :return:
        """
        Location=args[0]
        Activity_name=args[1]
        Activity_code=args[2]
        Reference_product=args[3]
        Units=args[4]




        # Call create_template to create the df of the inventory
        df = self.create_template_df()


        map_activities=self.enbios_data['activities']


        print('Creating the new market for electricity...')

        # Add first row to the df
        first_row = {"Amount": 1,
                     "Location": Location,
                     "Activity name": Activity_name,
                     "Activity_code": Activity_code,
                     "Reference_product": Reference_product,
                     "Unit": Units,
                     "Act_to": "marker",
                     "Technosphere": "Yes",
                     "Database": bw_db,
                     "Unit_check": None
                     }
        df.loc[len(df.index)] = first_row

        for element in self.electricity_list:
            for key in map_activities.keys():
                if element == key:
                    code = map_activities[key]['id']['code']  # This might change


                    act = ei.get_node(code)
                    location = act['location']
                    ref_prod = act['reference product']
                    unit_check = act['unit']

                    print(code)
                    row = {"Amount": 1,
                           "Location": str(location),
                           "Activity name": str(key),
                           "Activity_code": code,
                           "Reference_product": ref_prod,  # TODO: change to "activity_to" or similar
                           "Unit": Units,  # All should be converted previously
                           "Act_to": Activity_name,
                           "Technosphere": "Yes",  # All should be
                           "Database": act['database'],
                           "Unit_check": unit_check
                           }


                    df.loc[len(df.index)] = row

        # df_gruoped = df.groupby('Activity_code')['Amount'].transform('sum')
        # df['Amount'] = df_gruoped
        # df = df.drop_duplicates(subset='Activity_code')
        if act in ei:
            print(f"Activity act {act['name']} stored in database {act['database']}")
            self.Template_Market=df
        else:
            raise AssertionError (f"activity {act['name']} failed to store in {act['database']}")
        return df


if __name__ =='__main__':
    pass
