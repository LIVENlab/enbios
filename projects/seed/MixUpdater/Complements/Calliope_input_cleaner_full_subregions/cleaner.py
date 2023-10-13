"""
This cleaner doesn't aggregate technologies by region
"""


import pandas as pd



def create_df():
    columns=[
        "spores",
        "techs",
        "locs",
        "carriers",
        "unit",
        "flow_out_sum"
    ]
    df=pd.DataFrame(columns=columns)
    return df




def filter_techs(mother_path,df):
    """
    Filter the technologies defined in the mother file
    :param mother_path:
    :param df:
    :return:
    """
    df_techs=pd.read_excel(mother_path,sheet_name='BareProcessors simulation')
    techs=df_techs['Processor'].tolist()
    mark=df['techs'].isin(techs)
    df_filtered=df[mark]
    return df_filtered


def manage_regions(arg):

    if arg =='ESP-sink':
        region='ESP'
    else:
        region=arg.replace('-','_')

    return region

def changer(data):
    """
    Group subregions in regions and sum the value for each technology /carrier
    :param df:
    :return:

    """
    df=pd.read_csv(data,delimiter=',')
    df=df.dropna()

    gen_df=create_df()
    scenarios=list(df.spores.unique())

    for scenario in scenarios:
        df_sub=df.loc[df['spores']==scenario]

        df_sub['locs']=df['locs'].apply(manage_regions)


        gen_df=pd.concat([gen_df,df_sub])

    return gen_df



def preprocess_calliope(data, motherfile):
    """
    data: csv from calliope
    motherfile: xlsx basefile
    """

    df=changer(data)
    final_df=(motherfile,df)

    return final_df



if __name__=='__main__':

    data = pd.read_csv('flow_out_sum.csv', delimiter=',')
    mother_file = r'C:\Users\Administrator\PycharmProjects\enbios2\projects\seed\Data\base_file_simplified.xlsx'

    df=changer(data)

    final_df=filter_techs(mother_file,df)
    final_df.to_csv(r'C:\Users\Administrator\PycharmProjects\enbios2\projects\seed\Data\flow_out_sum_modified_full_subregions.csv')
else:
    pass
