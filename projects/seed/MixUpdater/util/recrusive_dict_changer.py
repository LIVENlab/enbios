import pandas as pd
import json
from pathlib import Path

from decimal import Decimal, getcontext

data= Path(r"C:\Users\Administrator\PycharmProjects\enbios2\projects\seed\MixUpdater\Intermediate_data")
temp_csv=data / 'template_market_subregions.csv'
path= r'C:\Users\Administrator\PycharmProjects\enbios2\projects\seed\Data\enbios_input_subregions.json'


def inventoryModify(scenario : str, save_file : bool = False,
                    save_path : str = None)->pd.DataFrame:

    """
    This function updates the values of the template inventory.
    ** Update
    :param scenario: str --> scenario to modify
    :param save_file: BOOL --> if True, every single file created will be changed
    :param save_path: str --> path where the files are saved
    :return: pandas Dataframe --> df with the market for electricity modified
    """

    with open(path,'r') as file:
        dict=json.load(file)
    # load the template
    df=pd.read_csv(temp_csv,delimiter=',')

    subdict=dict['scenarios'][scenario]['activities']
    for key in subdict.keys():
        name=key
        amount=subdict[key][1]
        for index,row in df.iterrows():
            if row['Activity name'] == name:
                df.loc[df['Activity name'] == name, 'Amount'] = amount

    df_gruoped=df.groupby('Activity_code')['Amount'].transform('sum')
    df['Amount']=df_gruoped
    df=df.drop_duplicates(subset='Amount')

    getcontext().prec = 50

    sum_of_column = sum(map(Decimal, df['Amount'][1:]))
    print(sum_of_column)

    df['Amount'] = [1] + [Decimal(x) / sum_of_column for x in df['Amount'][1:]]

    print('Check total', sum(map(Decimal, df['Amount'][1:])))
    #TODO: reference of one

    if save_file:
        filename=str(scenario) + '_inventoryFile.csv'
        path_=save_path + '/' + filename
        df.to_csv(path_,sep=',',index=False)

    return df



if __name__=='__main__':
    # Check
    a=inventoryModify('2')

