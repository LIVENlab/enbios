import pandas as pd
data=pd.read_csv('flow_out_sum.csv',delimiter=',')
mother_file=r'C:\Users\Administrator\PycharmProjects\enbios2\projects\seed\Data\base_file_simplified.xlsx'
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

def manage_regions(arg):

    if arg =='ESP-sink':
        region='ESP'
    else:
        region=arg.split('_')[-1]
        region="PRT_" + region
    return region




def changer(df):
    """
    Group subregions in regions and sum the value for each technology /carrier
    :param df:
    :return:
    """
    df=df.dropna()

    gen_df=create_df()
    scenarios=list(df.spores.unique())

    for scenario in scenarios:
        df_sub=df.loc[df['spores']==scenario]

        df_sub['locs']=df['locs'].apply(manage_regions)

        df_sub=df_sub.groupby(['techs','locs']).agg({
            "spores": "first",
            "carriers": "first",
            "unit" : "first",
            "flow_out_sum": "sum"
        }).reset_index()
        gen_df=pd.concat([gen_df,df_sub])

    return gen_df

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




if __name__=='__main__':
    df=changer(data)
    final_df=filter_techs(mother_file,df)
    final_df.to_csv(r'C:\Users\Administrator\PycharmProjects\enbios2\projects\seed\Data\flow_out_sum_modified.csv')