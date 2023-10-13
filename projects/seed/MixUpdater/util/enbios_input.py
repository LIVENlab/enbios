import pprint
import json
import random
from logging import getLogger
import bw2data as bd
import openpyxl
import pandas as pd
from bw2data.errors import UnknownObject

from projects.seed.MixUpdater.const.const import bw_project,bw_db


bd.projects.set_current(bw_project)            # Select your project
database = bd.Database(bw_db)        # Select your db

class SoftLinkCalEnb():

    def __init__(self,calliope,motherfile):
        #TODO: Add input checker


        self.calliope=calliope
        self.motherfile=motherfile
        self.dict_gen=None
        self.scens=None
        self.aliases=[]
        self.final_acts={}
        self.hierarchy_tree=None



    def generate_scenarios(self, smaller_vers=None):
        """
        Iterate through the data from calliope (data.csv, output results...)
            -Create new columns, such as alias
        The function includes an intermediate step to create the hierarchy


        :param calliope_data:
        :param smaller_vers: BOOL, if true, a small version of the data for testing gets produced
        :return:scen_dict, acts
                *scen dict --> dictionary of the different scenarios
                *acts --> list including all the unique activities
        """
        # TODO: Here does not read a csv only, read df when coming from the other class

        if isinstance(self.calliope, pd.DataFrame):
            cal_dat=self.calliope

        elif isinstance(self.calliope,str):

            cal_dat = pd.read_csv(self.calliope, delimiter=',')

        else:
            raise Exception
        cal_dat['aliases'] = cal_dat['techs'] + '__' + cal_dat['carriers'] + '___' + cal_dat['locs']  # Use ___ to split the loc for the recognision of the activities
        scenarios = cal_dat['scenarios'].unique().tolist()
        if smaller_vers is not None:  # get a small version of the data ( only 3 scenarios )
            try:
                scenarios = scenarios[:smaller_vers]
            except:
                raise ValueError('Scenarios out of bonds')

        scen_dict = {}
        for scenario in scenarios:
            df = cal_dat[cal_dat['scenarios'] == scenario]
            stuff = SoftLinkCalEnb.get_scenario(df)
            scen_dict[scenario] = {}
            scen_dict[scenario]['activities'] = stuff

        # GENERATE KEYS FOR THE SCENARIOS
        scens = random.choice(list(scen_dict.keys()))  # select a random scenario from the list
        print(f'techs from scenario {scens} chosen')
        acts = list(scen_dict[scens]['activities'].keys())

        # Intermediate step
        # Generate a code-region alias name dictionary to create the hierarchy

        general = {}
        for act in set(acts):
            act_key = act.split('___')[0]
            if act_key not in general.keys():
                elements_to_append = []
                for act2 in set(acts):
                    act_key2 = act2.split('___')[0]
                    if act_key2 == act_key:
                        elements_to_append.append(act2)
                general[act_key] = elements_to_append

        # Here is where one interemediate file is created
        self.dict_gen=general
        self.acts=acts
        self.scens=scen_dict



        """
        Old 
        
        with open(dict_gen, 'w') as file:
            json.dump(general, file, indent=4)
        """




    @staticmethod
    def get_scenario(df) -> dict:
        """
        Iters through 1 scenario of the data.csv (scenarios data), storing basic data in a dictionary
        Get {
        activities : {
            alias : [
            unit,
            amount]}}
        :param df:
        :return:
        """
        scenario = {}
        for index, row in df.iterrows():
            other_stuff = []
            alias = row['aliases']
            flow_out_sum = (row['flow_out_sum'])
            unit = row['units']
            other_stuff.append(unit)
            other_stuff.append(flow_out_sum)

            scenario[alias] = other_stuff
        return scenario







    def generate_activities(self,*args) -> dict:
        """
        This function reads the Excel "mother file" and creates a dictionary.
        It reads the BWs' codes and extracts information from each activity and stores them in a dictionary

        :param args:
        :return:
        """

        processors = pd.read_excel(self.motherfile, sheet_name='BareProcessors simulation')

        activities_cool = {}
        for index, row in processors.iterrows():
            code = str(row['BW_DB_FILENAME'])

            print('im parsing', str(row['Processor']), code)
            try:
                act = database.get_node(code)

            except UnknownObject:
                print(row['Processor'], 'has an unknown object')


            name = act['name']
            unit = act['unit']
            alias = str(row['Processor']) + '__' + str(row['@SimulationCarrier'])
            print(alias)

            activities_cool[alias] = {
                'name': name,
                'code': code,
            }


        activities = {}
        for element in args:
            new_element = element.split('___')[0]  # This should match the name
            for key in activities_cool.keys():
                if new_element == key:
                    new_code = activities_cool[key]['code']
                    activities[element] = {
                        "id": {
                            'code': new_code
                        }

                    }

        #pprint.pprint(activities)
        self.final_acts = activities
        print('Activities stored as a dict')




    def hierarchy(self, *args) -> dict:
        """
        This function creates the hierarchy tree.
        It uses two complementary functions (generate_dict and tree_last_level).

        It reads the information contained in the mother file starting by the bottom (n-lowest) level
        :param data:
        :param args:
        :return:
        """
        print('Creating tree following the structure defined in the basefile')
        df = pd.read_excel(self.motherfile, sheet_name='parents')
        df2 = pd.read_excel(self.motherfile, sheet_name='BareProcessors simulation')

        # Do some changes to match the regions and aliases

        df2['Processor'] = df2['Processor'] + '__' + df2['@SimulationCarrier']  # Mark, '__' for carrier split
        # Start by the last level of parents
        levels = df['Level'].unique().tolist()
        last_level_parent = int(levels[-1].split('-')[-1])
        last_level_processors = 'n-' + str(last_level_parent + 1)
        df2['Level'] = last_level_processors
        df = pd.concat([df, df2[['Processor', 'ParentProcessor', 'Level']]], ignore_index=True, axis=0)

        levels = df['Level'].unique().tolist()

        list_total = []
        for level in reversed(levels):
            df_level = df[df['Level'] == level]
            if level == levels[0]:
                break

            elif level == levels[-1]:
                last = self.tree_last_level(df_level, *args)
                global last_list
                last_list = last

            else:
                df_level = df[df['Level'] == level]
                list_2 = self.generate_dict(df_level, last_list)
                last_list = list_2
                list_total.append(list_2)

        dict_tree = list_total[-1]
        self.hierarchy_tree=dict_tree[-1]

    @staticmethod
    def tree_last_level(df, *args):
        """
        This function supports the creation of the tree.
        It's specific for the lowest level of the dendrogram
        Return the act
        :param df:
        :param names: comes from generate scenarios. List of unique aliases
        :return:
        """
        new_rows = []
        for index, row in df.iterrows():
            processor = row['Processor']
            for element in args:
                cop = element.split('___')[0]
                if cop == processor:
                    new_row = row.copy()
                    new_row['Processor'] = element
                    new_rows.append(new_row)

        df = pd.concat([df] + new_rows, ignore_index=True)
        last_level_list = []
        # Return a list of dictionaries
        parents = list(df['ParentProcessor'].unique())
        for parent in parents:
            last_level = {}
            df3 = df[df['ParentProcessor'] == parent]
            childs = df3['Processor'].unique().tolist()
            last_level[parent] = childs
            last_level_list.append(last_level)

        return last_level_list

    @staticmethod
    def generate_dict(df, list_pre):
        """
        Pass a list of the lower level and the dataframe of the present
        Returns a list of the dictionary corresponding that branch

        :param df:
        :param list:
        :return:
        """
        parents = df['ParentProcessor'].unique().tolist()
        list_branches = []
        for parent in parents:
            branch = {}
            df_parent = df[df['ParentProcessor'] == parent]
            branch[parent] = {}

            for _, row in df_parent.iterrows():
                child_df = row['Processor']
                for element in list_pre:
                    if child_df in element:
                        branch[parent][child_df] = element[child_df]
            list_branches.append(branch)

        return list_branches




    def hierarchy_refinement(self,hierarchy_dict):
        """
        Include different regions in the tree

        Read the hierarchy dictionary and do some modifications:
            * Replace the names of the activities by the ones defined in the mapping dictionary
            Ex: hydro_run_of_river__electricity : [hydro_run_of_river__electricity___PRT_1, hydro_run_of_river__electricity___PRT_2]

        :param hierarchy_dict:
        :return: same dictionary modified
        """
        # 1 look for the lists. List contain the last levels of the dendrogram, where the names need to be modified


        map_names=self.dict_gen

        for value in hierarchy_dict.values():

            if isinstance(value, list):

                # Copy the list
                values_copy = value[:]
                value.clear()
                for element in values_copy:
                    for key, val in map_names.items():
                        if element == key:
                            list_names = map_names[key]
                            # 3. Include the new names
                            for name in list_names:

                                value.append(name)
            elif isinstance(value, dict):

                self.hierarchy_refinement(value)

        self.hierarchy_tree = hierarchy_dict


    def run(self, path= None):
        self.generate_scenarios(smaller_vers=10)
        self.generate_activities(*self.acts)
        self.hierarchy(*self.final_acts)
        self.hierarchy_refinement(hierarchy_dict=self.hierarchy_tree)

        # TODO: Implement function to generate this data
        enbios2_methods = {

            'agricultural land occupation (LOP)': ('ReCiPe 2016 v1.03, midpoint (H)',
                                                   'land use',
                                                   'agricultural land occupation (LOP)'),
            'surplus ore potential (SOP)': ('ReCiPe 2016 v1.03, midpoint (H)',
                                            'material resources: metals/minerals',
                                            'surplus ore potential (SOP)'),
            'global warming potential (GWP1000)': ('ReCiPe 2016 v1.03, midpoint (H)',
                                                   'climate change',
                                                   'global warming potential (GWP1000)'),
            'water consumption potential (WCP)': ('ReCiPe 2016 v1.03, midpoint (H)',
                                                  'water use',
                                                  'water consumption potential (WCP)'),
            'freshwater eutrophication potential (FEP)': ('ReCiPe 2016 v1.03, midpoint (H)',
                                                          'eutrophication: freshwater',
                                                          'freshwater eutrophication potential (FEP)')

        }

        self.enbios2_data = {
            "bw_project": bw_project,
            "activities": self.acts,
            "hierarchy": self.hierarchy_tree,
            "methods": enbios2_methods,
            "scenarios": self.scens
        }
        if path:
            with open(path, 'w') as gen_diction:
                json.dump(self.enbios2_data, gen_diction, indent=4)
            gen_diction.close()
        print('Tree created. Check out cls.enbios2 data or path where saved')






if __name__=='__main__':
    # TODO: Calliope path will be the atribute of a preprocess class.
    # Keep the path link for testing
    a=SoftLinkCalEnb(r'C:\Users\Alex\PycharmProjects\pythonProject\enbios_2\projects\seed\Data\flow_out_sum_modified_unit_checked_full_subregions.csv',r'C:\Users\Alex\PycharmProjects\pythonProject\enbios_2\projects\seed\Data\base_file_simplified.xlsx')
    a.run(path=r'C:\Users\Alex\PycharmProjects\pythonProject\enbios_2\projects\seed\lex_seeds\results\res.json')