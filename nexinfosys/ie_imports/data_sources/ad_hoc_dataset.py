from typing import List, Tuple, Dict

from nexinfosys.common.helper import create_dictionary, load_dataset
from nexinfosys.ie_imports.data_source_manager import IDataSourceManager, filter_dataset_into_dataframe
from nexinfosys.models.statistical_datasets import Dataset, DataSource, Database


class AdHocDatasets(IDataSourceManager):
    """
    Datasets in a file (external, or the file currently being analyzed) with format defined by Magic project
    """
    def __init__(self, datasets_list: List[Dataset]):
        self._registry = None
        self.initialize_datasets_registry(datasets_list)

    def initialize_datasets_registry(self, datasets_list: Dict[str, Dataset]):
        """
        Receive a list of the datasets and make a copy

        :param datasets_list:
        :return: None
        """
        self._registry = create_dictionary()
        for ds_name, ds in datasets_list.items():
            self.register_dataset(ds.code, ds)

    def register_dataset(self, name, ds):
        self._registry[name] = ds

    def get_name(self) -> str:
        """ Source name """
        return self.get_datasource().name

    def get_datasource(self) -> DataSource:
        """ Data source """
        src = DataSource()
        src.name = "AdHoc"
        src.description = "A special, ad-hoc, data source, providing datasets elaborated inside an execution. Datasets are local to the execution."
        return src

    def get_databases(self) -> List[Database]:
        """ List of databases in the data source """
        db = Database()
        db.code = ""
        db.description = "AdHoc is a database itself"
        return [db]

    def get_datasets(self, database=None) -> list:
        """ List of datasets in a database, or in all the datasource (if database==None)
            Return a list of tuples (database, dataset)
        """

        lst = []
        for d in self._registry:
            lst.append((d, self._registry[d].description))  # [(name, description)]

        return lst

    def get_dataset_structure(self, database, dataset) -> Dataset:
        """ Obtain the structure of a dataset: concepts, dimensions, attributes and measures """
        return self._registry[dataset]
        # return self.etl_dataset(dataset, update=False)

    def etl_full_database(self, database=None, update=False):
        pass

    def etl_dataset(self, dataset, update=False) -> str:
        """
        Read dataset data and metadata into NIS databases

        :param url:
        :param local_filename:
        :param update:
        :return: String with full file name
        """
        pass

    def get_dataset_filtered(self, dataset: str, dataset_params: List[Tuple]) -> Dataset:
        """ This method has to consider the last dataset download, to re"""
        # Read dataset structure
        # TODO - CLONE "ds"
        ds = self.get_dataset_structure(None, dataset)

        if ds.data is None:
            df = load_dataset(ds.attributes["_location"])
        else:
            df = ds.data

        # Create index with
        idx_cols = []
        for c in ds.dimensions:
            if not c.is_measure:
                idx_cols.append(c.code)

        df = df.set_index(idx_cols)

        # Filter it using generic Pandas filtering capabilities
        if dataset_params.get("StartPeriod") and dataset_params.get("EndPeriod"):
            years = [str(y) for y in range(int(dataset_params["StartPeriod"][0]), int(dataset_params["EndPeriod"][0])+1)]
            dataset_params["year"] = years
            del dataset_params["StartPeriod"]
            del dataset_params["EndPeriod"]
        ds.data = filter_dataset_into_dataframe(df, dataset_params, dataset)

        return ds

    def get_refresh_policy(self):  # Refresh frequency for list of databases, list of datasets, and dataset
        pass


