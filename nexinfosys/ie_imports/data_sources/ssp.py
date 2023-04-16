# -*- coding: utf-8 -*-
import pandas as pd
from typing import List

from nexinfosys.models.statistical_datasets import DataSource, Database, Dataset
from nexinfosys.ie_imports.data_source_manager import IDataSourceManager


def get_ssp_dimension_names_dataset(dataset_name):
    if dataset_name.lower()=="regions":
        return ["source_id", "model", "scenario", "spatial", "temporal", "variable"]
    elif dataset_name.lower()=="countries":
        return []


def get_ssp_dataset(dataset_name: str, convert_dimensions_to_lower=False):
    """
    Read into a Dataframe the requested SSP dataset

    :param dataset_name: either "regions" or "countries"
    :param convert_dimensions_to_lower: True is to convert dimensions to lower case
    :return: A DataFrame with the dataset
    """
    # Read some configuration knowing where the ssp is stored
    if "SSP_FILES_DIR" in app.config:
        base_path = app.config["SSP_FILES_DIR"]
    else:
        base_path = "/home/rnebot/GoogleDrive/AA_MAGIC/Data/SSP/"
    fname = base_path + "/" + dataset_name.lower() + ".csv"
    # Read the file into a Dataframe
    df = pd.read_csv(fname)
    dims = get_ssp_dimension_names_dataset(dataset_name)
    # Convert to lower case if needed
    if convert_dimensions_to_lower:
        for d in dims:
            df[d] = df[d].astype(str).str.lower()
    # Index the dataframe on the dimensions
    df.set_index(dims, inplace=True)

    # Return the Dataframe
    return df


class SSPLocal(IDataSourceManager):
    def __init__(self):
        pass

    def get_name(self) -> str:
        """ Source name """
        pass

    def get_datasource(self) -> DataSource:
        """ Data source """
        return "SSP"

    def get_databases(self) -> List[Database]:
        """ List of databases in the data source """
        return ["SSP"]

    def get_datasets(self, database=None) -> list:
        """ List of datasets in a database, or in all the datasource (if database==None)
            Return a list of tuples (database, dataset)
        """
        return {"regions": "SSP by regions (6) and global",
                "countries": "SSP by country"
                }

    def get_dataset_structure(self, database, dataset) -> Dataset:
        """ Obtain the structure of a dataset: concepts, dimensions, attributes and measures """
        s = get_ssp_dimension_names_dataset(dataset)
        pass

    def etl_full_database(self, database=None, update=False):
        """ If bulk download is supported, refresh full database """
        pass

    def etl_dataset(self, dataset, update=False):
        """ If bulk download is supported, refresh full dataset """
        pass

    def get_dataset_filtered(self, dataset, dataset_params: list) -> Dataset:
        """ Obtains the dataset with its structure plus the filtered values
            The values can be in a pd.DataFrame or in JSONStat compact format
            After this, new dimensions can be joined, aggregations need to be performed
        """
        pass

    def get_refresh_policy(self):  # Refresh frequency for list of databases, list of datasets, and dataset
        pass
