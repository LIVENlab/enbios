"""
Created on Tue Sep  5 10:10:04 2023

@author: Alexander de Tomás (ICTA-UAB)
        -LexPascal
"""

import pprint
import json
import random
from logging import getLogger
import bw2data as bd
import openpyxl
import pandas as pd
from bw2data.errors import UnknownObject
from projects.seed.Data.const import data_path
from projects.seed.MixUpdater.const.const import bw_project,bw_db


bd.projects.set_current(bw_project)            # Select your project
ei = bd.Database(bw_db)        # Select your db


dict_path = data_path / 'enbios_input_subregions.json'       # Dictionary
getLogger("peewee").setLevel("ERROR")

print(bd.projects.dir)

with open(dict_path,'r') as file:
    map_names = json.load(file)




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


def get_list(*args):
    """
    :param args: We're looking for the activities under that define a "future market for electricity". Hence, we specify here
                the keys that point something like "Electricity generation" in the hierarchy
    :return:  List of the activities
    """
    present_dict=map_names
    for arg in args:
        if arg in present_dict:
            present_dict=present_dict[arg]
        else:
            return None
    if isinstance(present_dict,list):
        return present_dict

def template_market_4_electricity(market_el_list,Location=None, Activity_name=None, Activity_code=None, Reference_product=None, Unit=None,Database=None):
    """
    This function returns a template of the "default_market_for electricity" in order to create an inventory

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


    # Call create_template to create the df of the inventory
    df= create_template_df()

    map_activities=map_names['activities']

    # Add first row
    first_row={"Amount":1,
                 "Location": Location,
                 "Activity name": Activity_name,
                 "Activity_code": Activity_code,
                 "Reference_product":Reference_product,
                 "Unit": Unit,
                 "Act_to": "marker",
                 "Technosphere": "Yes",
                 "Database": Database,
                 "Unit_check": None

               }
    df.loc[len(df.index)] = first_row

    for element in market_el_list:
        for key in map_activities.keys():
            if element==key:
                code=map_activities[key]['id']['code'] # This might change
                act=ei.get_node(code)
                location=act['location']
                ref_prod=act['reference product']
                unit_check=act['unit']

                print(code)
                row={"Amount":1,
                     "Location":str(location),
                     "Activity name": str(key),
                     "Activity_code": code,
                     "Reference_product":ref_prod,              # TODO: change to "activity_to" or similar
                     "Unit": Unit,                          # All should be converted previously
                     "Act_to": Activity_name,
                     "Technosphere": "Yes",                 # All should be
                     "Database": act['database'],
                     "Unit_check":unit_check
                     }

                #df=df.append(row,ignore_index=True)
                df.loc[len(df.index)] = row

    #df_gruoped = df.groupby('Activity_code')['Amount'].transform('sum')
    #df['Amount'] = df_gruoped
    #df = df.drop_duplicates(subset='Activity_code')

    df.to_csv(r'C:\Users\altz7\PycharmProjects\enbios__git\projectsMixUpdater\Intermediate_data/template_market_subregions.csv',index=False,sep=',')


    return df

#TODO: improve for the case when the db is empty of hydrogen

if __name__=='__main__':
    aa=get_list("hierarchy","Energysystem","Generation","Electricity_generation")
    cosa=template_market_4_electricity(aa,Location='PT',Activity_name="Future market for electricity",Activity_code="f_test",Reference_product="electricity production, 2050 in Portugal",Unit='kWh',Database=bw_db)




