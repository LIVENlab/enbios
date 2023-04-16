from abc import abstractmethod

from typing import List

from nexinfosys.ie_imports.data_source_manager import IDataSourceManager
from nexinfosys.models.statistical_datasets import DataSource, Database, Dataset


class WIOD(IDataSourceManager):
    @abstractmethod
    def get_name(self) -> str:
        """ Source name """
        pass

    @abstractmethod
    def get_datasource(self) -> DataSource:
        """ Data source """
        pass

    @abstractmethod
    def get_databases(self) -> List[Database]:
        """ List of databases in the data source """
        pass

    @abstractmethod
    def get_datasets(self, database=None) -> list:
        """ List of datasets in a database, or in all the datasource (if database==None)
            Return a list of tuples (database, dataset)
        """
        pass

    @abstractmethod
    def get_dataset_structure(self, database, dataset) -> Dataset:
        """ Obtain the structure of a dataset: concepts, dimensions, attributes and measures """
        pass

    @abstractmethod
    def etl_full_database(self, database=None, update=False):
        """ If bulk download is supported, refresh full database """
        pass

    @abstractmethod
    def etl_dataset(self, dataset, update=False):
        """ If bulk download is supported, refresh full dataset """
        pass

    @abstractmethod
    def get_dataset_filtered(self, dataset, dataset_params: list) -> Dataset:
        """ Obtains the dataset with its structure plus the filtered values
            The values can be in a pd.DataFrame or in JSONStat compact format
            After this, new dimensions can be joined, aggregations need to be performed
        """
        pass

    @abstractmethod
    def get_refresh_policy(self):  # Refresh frequency for list of databases, list of datasets, and dataset
        pass
