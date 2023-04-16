import os
import tempfile
from typing import List
import pandas as pd
import sdmx
import pandas_datareader.data as web
import requests
import requests_cache

from nexinfosys.common.helper import Memoize2
from nexinfosys.ie_imports.data_source_manager import IDataSourceManager, filter_dataset_into_dataframe
from nexinfosys.models.statistical_datasets import DataSource, Database, Dataset, Dimension, CodeList, CodeImmutable


class OECD(IDataSourceManager):
    def __init__(self):
        d = {"backend": "sqlite", "include_get_headers": True, "cache_name": tempfile.gettempdir() + "/oecd_bulk_datasets"}
        requests_cache.install_cache(**d)

    def get_name(self) -> str:
        """ Source name """
        return self.get_datasource().name

    def get_datasource(self) -> DataSource:
        """ Data source """
        src = DataSource()
        src.name = "OECD"
        src.description = "OECD is group of 34 democracies with market economy plus 70 economies where growth, prosperity and sustainable development is promoted"
        return src

    def get_databases(self) -> List[Database]:
        """ List of databases in the data source """
        db = Database()
        db.code = ""
        db.description = "OECD provides all Datasets in a single database"
        return [db]

    @Memoize2
    def get_datasets(self, database=None) -> list:
        """ List of datasets in a database, or in all the datasource (if database==None)
            Return a list of tuples (database, dataset)
        """
        from lxml import etree
        # List of datasets, containing three columns: ID, description, ID
        lst = []
        # TODO The third value should be a URN, but it is not available
        xml = requests.get("http://stats.oecd.org/RestSDMX/sdmx.ashx/GetKeyFamily/all")
        root = etree.fromstring(xml.content.decode("utf-8"))
        ids = root.xpath("//*[@agencyID='OECD']/@id")
        descs = root.xpath("//*[@agencyID='OECD']/*[@xml:lang='en']/text()")
        for desc, id_ in zip(descs, ids):
            lst.append((id_, desc, id_))

        return lst

    def get_dataset_structure(self, database, dataset) -> Dataset:
        """ Obtain the structure of a dataset: concepts, dimensions, attributes and measures """
        # Retrieve and save to file
        xml = requests.get("http://stats.oecd.org/restsdmx/sdmx.ashx/GetDataStructure/"+dataset)
        xml = xml.content.decode("utf-8")
        _, fname = tempfile.mkstemp(".oecd_dsd.xml", text=True)
        file = open(fname, "w")
        file.writelines(xml)
        file.close()

        dsd = sdmx.dsd_reader(fname)
        kfam = dsd.key_families()[0]

        # SDMXConcept = collections.namedtuple('Concept', 'type name istime description code_list')
        # DataSource <- Database <- DATASET <- Dimension(s) (including Measures) <- CodeList
        #                                      |
        #                                      v
        #                                      Concept <- CodeList  (NOT CONSIDERED NOW)
        ds = Dataset()
        ds.code = dataset
        ds.description = kfam.name("en")
        ds.attributes = {}  # Dataset level attributes? (encode them using a dictionary)
        ds.metadata = None  # Metadata for the dataset SDMX (flow, date of production, etc.)
        ds.database = database  # Reference to containing database

        # Code lists (assume flat)
        c_dict = {}
        for cl in dsd.code_lists():
            codes = [CodeImmutable(c.value, c.description("en"), "", []) for c in cl.codes()]
            c_dict[cl.id] = CodeList.construct(cl.id, cl.name("en"), [""], codes=codes)

        # Time dimension name
        if kfam.time_dimension():
            t_dim = kfam.time_dimension().concept_ref()
            t_dim_cl = kfam.time_dimension().code_list_id()
        else:
            t_dim = None

        # Dimensions
        for d in kfam.dimensions():
            dd = Dimension()
            dd.code = d.concept_ref()
            dd.attributes = None
            istime = dd.code == t_dim
            if istime:
                dd.is_time = istime
                dd.code_list = c_dict[t_dim_cl]
            else:
                dd.code_list = c_dict[d.code_list_id()]
            dd.description = dd.code_list.description
            dd.is_measure = False  # DIMENSION
            dd.dataset = ds

        # Measure
        dd = Dimension()
        dd.code = kfam.primary_measure().concept_ref()
        dd.description = dsd.concept(dd.code).name("en")
        dd.attributes = None
        dd.is_time = False
        dd.is_measure = True
        dd.dataset = ds

        return ds

    def etl_full_database(self, database=None, update=False):
        """ If bulk download is supported, refresh full database """
        pass

    def etl_dataset(self, dataset, update=False) -> str:
        """
        Download a OECD bulk file

        :param url:
        :param local_filename:
        :param update:
        :return: String with full file name
        """
        fname = tempfile.gettempdir() + "/oecd_" + dataset + '.pickled_dataframe'

        if os.path.isfile(fname):
            if not update:
                return fname
            else:
                os.remove(fname)

        df = web.DataReader(dataset, "oecd")
        df.to_pickle(fname)

        return fname

    def get_dataset_filtered(self, dataset: str, dataset_params: dict) -> Dataset:
        """ This method has to consider the last dataset download, to re"""

        # Read OECD dataset structure
        ds = self.get_dataset_structure(None, dataset)

        # Read full OECD dataset into a Dataframe
        dataframe_fn = tempfile.gettempdir() + "/oecd_" + dataset + ".bin2"
        df = None
        if os.path.isfile(dataframe_fn):
            df = pd.read_parquet(dataframe_fn)
            # df = pd.read_msgpack(dataframe_fn)

        if df is None:
            # Read the unprocessed dataset
            fname = self.etl_dataset(dataset, update=False)
            df = pd.read_pickle(fname)

            # Process DataFrame to make it compatible with "filter_dataset_into_dataframe"
            df = df.transpose()
            # Date to Year dictionary
            df.columns = pd.DatetimeIndex(df.columns.values, name=df.columns.name).year
            # t_dict = {d: y for d, y in zip(df.columns.values, pd.DatetimeIndex(df.columns.values).year)}
            # df.replace({"Year": t_dict}, inplace=True)
            # Time dimension is incorporated with
            ser = df.stack().sort_index()
            ser.name = "value"
            # By removing the index, the series is again a pd.DataFrame
            df = ser.reset_index()

            # Convert descriptions to codes
            recodes = {}
            for d in ds.dimensions:
                if d.code_list:
                    codes = d.code_list.levels[0].codes
                    if len(codes) > 0:
                        recodes[d.description] = {c.description: c.code.lower() for c in codes}
                else:
                    pass

            df.replace(recodes, inplace=True)

            df.set_index([c for c in df.columns[:-1]], inplace=True)
            # Save it
            df.to_parquet(dataframe_fn)
            # df.to_msgpack(dataframe_fn)

        # Filter it using generic Pandas filtering capabilities
        ds.data = filter_dataset_into_dataframe(df, dataset_params, dataset)

        return ds

    def get_refresh_policy(self):  # Refresh frequency for list of databases, list of datasets, and dataset
        pass


# o = OECD()
# filter_ = create_dictionary(data=dict(country=["AUT", "AUS"]))
# df = o.get_dataset_filtered("TUD", filter_)
# print(df)
