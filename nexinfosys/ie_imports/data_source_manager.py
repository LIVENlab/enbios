from abc import ABCMeta, abstractmethod
from typing import List, Union
import pandas as pd
import numpy as np

import nexinfosys
from nexinfosys import case_sensitive
from nexinfosys.common.helper import create_dictionary, Memoize2, \
    get_dataframe_copy_with_lowercase_multiindex, strcmp
from nexinfosys.models.statistical_datasets import DataSource, Database, Dataset
from nexinfosys.models.musiasem_methodology_support import force_load


class IDataSourceManager(metaclass=ABCMeta):
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
    def etl_dataset(self, dataset, update=False) -> Dataset:
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


class DataSourceManager:

    def __init__(self, session_factory):
        self.registry = create_dictionary()
        self._session_factory = session_factory

    # ---------------------------------------------------------------------------------
    def register_local_datasets(self, local_datasets):
        from nexinfosys.ie_imports.data_sources.ad_hoc_dataset import AdHocDatasets
        # Register AdHocDatasets
        if local_datasets:
            if "adhoc" not in self.registry:
                adhoc = AdHocDatasets(local_datasets)
                self.register_datasource_manager(adhoc)

    def unregister_local_datasets(self, local_datasets):
        from nexinfosys.ie_imports.data_sources.ad_hoc_dataset import AdHocDatasets
        # Unregister AdHocDatasets
        if local_datasets:
            for i in self.registry.values():
                if isinstance(i, AdHocDatasets):
                    self.unregister_datasource_manager(i)
                    break

    def register_datasource_manager(self, instance: IDataSourceManager):
        self.registry[instance.get_name()] = instance

    def unregister_datasource_manager(self, instance: IDataSourceManager):
        """
        Remove a data source
        This is necessary for AdHocDatasets which has to be created and deleted depending on needs
        (it is an Adapter of a map of the Datasets in the current state)

        :param instance:
        :return:
        """
        n = instance.get_name()
        if n in self.registry:
            del self.registry[instance.get_name()]

    def update_data_source(self, source: IDataSourceManager):
        # TODO Clear database
        # TODO Read structure of ALL datasets from this source
        pass

    def _get_source_manager(self, source):
        if source:
            if isinstance(source, str):
                if source in self.registry:
                    source = self.registry[source]
        return source

    # ---------------------------------------------------------------------------------

    def get_supported_sources(self):
        # e.g.: return ["Eurostat", "FAO", "OECD", "FADN", "COMEXT"]
        return [s for s in self.registry]

    def update_sources(self, local_datasets=None):
        """ Update bulk downloads for sources allowing full database. Not full dataset (on demand) """
        # Register AdHoc datasets
        self.register_local_datasets(local_datasets)

        for s in self.registry:
            # Check last update
            poli = self.registry[s].get_refresh_policy()
            update = False
            if update:
                pass

        # Unregister AdHoc datasets
        self.unregister_local_datasets(local_datasets)

    def get_databases(self, source: Union[IDataSourceManager, str], local_datasets=None):
        # Register AdHoc datasets
        self.register_local_datasets(local_datasets)

        # List of databases in source (a database contains one or more datasets)
        if source:
            source = self._get_source_manager(source)

        lst = []
        if not source:
            for s in self.registry:
                lst.extend([(s,
                             [db for db in self.registry[s].get_databases()]
                             )
                            ]
                           )
        else:
            lst = [(source.get_name(),
                     [db for db in source.get_databases()]
                    )
                   ]

        # Unregister AdHoc datasets
        self.unregister_local_datasets(local_datasets)

        return lst

    # @cachier(stale_after=datetime.timedelta(days=1))
    def get_datasets(self, source: Union[IDataSourceManager, str]=None, database=None, local_datasets=None):
        # Register AdHoc datasets
        self.register_local_datasets(local_datasets)

        if source:
            source = self._get_source_manager(source)

        lst = []
        if source:
            if strcmp(source.get_name(), "AdHoc"):
                lst = [(source.get_name(), source.get_datasets(database))]
            else:
                lst = self.get_external_datasets(source, database)
        else:  # ALL DATASETS
            lst_total = []
            lst = self.get_external_datasets(source, database)  # Because "get_external_datasets" uses "Memoize", DO NOT modify "lst" outside
            lst_total.extend(lst)
            for s in self.registry:
                if strcmp(s, "AdHoc") and local_datasets:
                    lst_total.append((s, [ds for ds in self.registry[s].get_datasets()]))

        # Unregister AdHoc datasets
        self.unregister_local_datasets(local_datasets)

        return lst

    @Memoize2
    def get_external_datasets(self, source: Union[IDataSourceManager, str]=None, database=None):
        """
        Obtain a list of tuples (Source, Dataset name)

        :param source: If specified, the name of the source
        :param database: If specified, the name of a database in the source
        :return: List of tuples (Source name, Dataset name)
        """

        if source:
            source = self._get_source_manager(source)

        if source:
            if database:  # SOURCE+DATABASE DATASETS
                return [(source.get_name(), source.get_datasets(database))]
            else:  # ALL SOURCE DATASETS
                lst = []
                for db in source.get_databases():
                    lst.extend(source.get_datasets(db))
                return [(source.get_name(), lst)]  # List of tuples (dataset code, description, urn)
        else:  # ALL DATASETS
            lst = []
            for s in self.registry:
                if not strcmp(s, "AdHoc"):
                    lst.append((s, [ds for ds in self.registry[s].get_datasets()]))
            return lst  # List of tuples (source, dataset code, description, urn)

    def get_dataset_structure(self, source: Union[IDataSourceManager, str], dataset: str, local_datasets=None) -> Dataset:
        """ Obtain the structure of a dataset, a list of dimensions and measures, without data """
        # Register AdHoc datasets
        self.register_local_datasets(local_datasets)

        if not source:
            source = DataSourceManager.obtain_dataset_source(dataset, local_datasets)
            if not source:
                raise Exception("Could not find a Source containing the Dataset '"+dataset+"'")

        source = self._get_source_manager(source)
        struc = source.get_dataset_structure(None, dataset)

        # Unregister AdHoc datasets
        self.unregister_local_datasets(local_datasets)

        return struc

    def get_dataset_filtered(self, source: Union[IDataSourceManager, str], dataset: str, dataset_params: dict, local_datasets=None) -> Dataset:
        """ Obtain the structure of a dataset, and DATA according to the specified FILTER, dataset_params """
        # Register AdHoc datasets
        self.register_local_datasets(local_datasets)
        source = self._get_source_manager(source)

        fds = source.get_dataset_filtered(dataset, dataset_params)

        # Unregister AdHoc datasets
        self.unregister_local_datasets(local_datasets)

        return fds

    @staticmethod
    def obtain_dataset_source(dset_name, local_datasets=None):
        from nexinfosys.ie_imports.data_sources.ad_hoc_dataset import AdHocDatasets
        # Register AdHocDatasets
        if local_datasets:
            if "AdHoc" not in nexinfosys.data_source_manager.registry:
                adhoc = AdHocDatasets(local_datasets)
                nexinfosys.data_source_manager.register_datasource_manager(adhoc)

        # Obtain the list of ALL datasets, and find the desired one, then find the source of the dataset
        lst = nexinfosys.data_source_manager.get_datasets(None, None, local_datasets)  # ALL Datasets, (source, dataset)
        ds = create_dictionary(data={d[0]: t[0] for t in lst for d in t[1]})  # Dataset to Source (to obtain the source given the dataset name)

        if dset_name in ds:
            source = ds[dset_name]
        else:
            source = None

        # Unregister AdHocDatasets
        if local_datasets:
            nexinfosys.data_source_manager.unregister_datasource_manager(adhoc)

        return source

# --------------------------------------------------------------------------------------------------------------------


def get_dataset_structure(session_factory, source: IDataSourceManager, dataset: str) -> Dataset:
    """ Helper function called by IDataSourceManager implementations """

    src_name = source.get_name()

    # ACCESS TO METADATA DATABASE
    session = session_factory()
    # Check if the source exists. Create it if not
    src = session.query(DataSource).filter(DataSource.name == src_name).first()
    if not src:
        src = source.get_datasource()
        session.add(src)
    # Check if the dataset exists. "ETL" it if not
    ds = session.query(Dataset).\
        filter(Dataset.code == dataset).\
        join(Dataset.database).join(Database.data_source).\
        filter(DataSource.name == src_name).first()
    if not ds:
        # >>>> It may imply a full ETL operation <<<<
        ds = source.etl_dataset(dataset, update=False)
        # Use existing database
        db = session.query(Database).filter(Database.code == ds.database.code).first()
        if db:
            ds.database = db
        else:
            ds.database.data_source = src  # Assign the DataSource to the Database
        session.add(ds)

    session.commit()

    force_load(ds)

    session.close()
    session_factory.remove()
    # END OF ACCESS TO DATABASE

    return ds


def filter_dataset_into_dataframe(in_df, filter_dict, dataset_name, eurostat_postprocessing=False):
    """
    Function allowing filtering a dataframe passed as input,
    using the information from "filter_dict", containing the dimension names and the list
    of codes that should pass the filter. If several dimensions are specified an AND combination
    is done

    "in_df" must have as index a MultiIndex with all the dimensions appearing in the "filter_dict"


    :param in_df: Input dataset, pd.DataFrame
    :param filter_dict: A dictionary with the items to keep, per dimension
    :param dataset_name: Original dataset name (to obtain dimensions and their domains)
    :param eurostat_postprocessing: Eurostat dataframe needs special postprocessing. If True, do it
    :return: Filtered dataframe
    """

    # TODO If a join is requested, do it now. Add a new element to the INDEX
    # TODO The filter params can contain a filter related to the new joins

    start = None
    if "StartPeriod" in filter_dict:
        start = filter_dict["StartPeriod"]
        if isinstance(start, list): start = start[0]
    if "EndPeriod" in filter_dict:
        endd = filter_dict["EndPeriod"]
        if isinstance(endd, list): endd = endd[0]
    else:
        if start:
            endd = start
    if not start:
        columns = in_df.columns  # All columns
    else:
        # Assume year, convert to integer, generate range, then back to string
        start = int(start)
        endd = int(endd)
        columns = [str(a) for a in range(start, endd + 1)]

    # Combinatorial dataset
    dset = nexinfosys.data_source_manager.get_dataset_structure(None, dataset_name)
    pre_combined = dict()
    combined = dict()
    for d in dset.dimensions:
        if not d.is_measure:
            if not d.is_time:
                cl = []
                if d.get_hierarchy():
                    if isinstance(d.get_hierarchy, list):
                        for v in d.get_hierarchy().codes:
                            cl.append(v.name)
                    else:  # Fix: codes can be in a dictionary
                        for v in d.get_hierarchy().codes.values():
                            cl.append(v.name)
                pre_combined[d.code] = cl
            else:
                pre_combined[d.code] = columns
    for i, k in enumerate(in_df.index.names):
        if k in filter_dict:
            combined[k] = filter_dict[k]
        else:
            combined[k] = pre_combined[k]

    # To bring all possible cells, prepare a combinatorial then merge with explicit values from dataset
    midx= pd.MultiIndex.from_product(combined.values(), names=combined.keys())
    in_df = in_df.merge(pd.DataFrame(index=midx), left_index=True, right_index=True, how='outer').fillna(np.NaN)

    if not case_sensitive:
        in_df_lower = get_dataframe_copy_with_lowercase_multiindex(in_df)

    # Rows (dimensions)
    cond_accum = np.full(in_df.index.size, fill_value=True)
    for i, k in enumerate(in_df.index.names):
        if k in filter_dict:
            lst = filter_dict[k]
            if not isinstance(lst, list):
                lst = [lst]
            if len(lst) > 0:
                if not case_sensitive:
                    cond_accum &= in_df_lower.index.isin([str(l).lower() for l in lst], i)
                else:
                    cond_accum &= in_df.index.isin([str(l) for l in lst], i)
            else:
                cond_accum &= in_df[in_df.columns[0]] == in_df[in_df.columns[0]]

    # Remove non existent index values
    for v in columns.copy():
        if v not in in_df.columns:
            columns.remove(v)

    tmp = in_df[columns][cond_accum]

    # Convert columns to a single column "TIME_PERIOD"
    if eurostat_postprocessing:
        if len(tmp.columns) > 0:
            lst = []
            for i, cn in enumerate(tmp.columns):
                df2 = tmp[[cn]].copy(deep=True)
                # TODO: use column name from metadata instead of hardcoded "value"
                df2.columns = ["value"]
                df2["TIME_PERIOD"] = cn
                lst.append(df2)
            in_df = pd.concat(lst)
            in_df.reset_index(inplace=True)
            # Value column should be last column
            lst = [l for l in in_df.columns]
            for i, l in enumerate(lst):
                if l == "value":
                    lst[-1], lst[i] = lst[i], lst[-1]
                    break
            in_df = in_df.reindex(lst, axis=1)
            return in_df
        else:
            return None
    else:
        tmp.reset_index(inplace=True)
        if len(tmp.columns) > 0:
            return tmp
        else:
            return None
