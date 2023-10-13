"""

"""

from projects.seed.MixUpdater.errors.errors import *
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

def input_checker(data):
    """
    Check whether the input from Calliope follows the expected structure
    data: pd.Dataframe
    """
    expected_cols=set(['spores','techs','locs','carriers','unit','flow_out_sum'])
    cols=set(data.columns.tolist())

    if expected_cols == cols:
        print('Input checked. Columns look ok')
    else:
        raise ExpectedColumns(f"Columns {cols} do not match the expected columns: {expected_cols}")




def filter_techs(mother_path,df):
    """
    Filter the technologies defined in the mother file
    :param mother_path:
    :param df:
    :return:
    """
    # TODO: Check input
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
    *Assume that the csv is comma separated*
    Group subregions in regions and sum the value for each technology /carrier
    :param df:
    :return:

    """
    print('Adapting input data...')
    try:
        df=pd.read_csv(data,delimiter=',')

    except FileNotFoundError:
        raise FileNotFoundError(f'File {data} does not exist. Please check it')

    else:
        input_checker(df)  # Check columns
        df = df.dropna()

        gen_df = create_df()
        scenarios = list(df.spores.unique())

        for scenario in scenarios:
            df_sub = df.loc[df['spores'] == scenario]

            df_sub['locs'] = df['locs'].apply(manage_regions)

            gen_df = pd.concat([gen_df, df_sub])

    return gen_df






def preprocess_calliope(data, motherfile):

    """
    data: csv from calliope
    motherfile: xlsx basefile
    """

    dat=changer(data)
    final_df=filter_techs(motherfile,dat)

    return final_df




