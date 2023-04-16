import gzip
import logging

import os
import re
import tempfile
from io import StringIO
from typing import List, Dict
import getpass
import csv

import numpy as np
import pandas as pd
import pandasdmx
import requests
import requests_cache

from nexinfosys import get_global_configuration_variable
from nexinfosys.common.helper import create_dictionary, import_names, Memoize2, translate_case
from nexinfosys.ie_imports.data_source_manager import IDataSourceManager, filter_dataset_into_dataframe
from nexinfosys.models.statistical_datasets import DataSource, Database, Dataset, Dimension, CodeList, CodeImmutable


def create_estat_request():
    # EuroStat datasets
    if get_global_configuration_variable('CACHE_FILE_LOCATION'):
        cache_name = get_global_configuration_variable('CACHE_FILE_LOCATION')
        logging.debug(f"USER: {getpass.getuser()}, creating cache for Eurostat requests")
        if not os.path.isdir(cache_name):
            os.makedirs(cache_name)
    else:
        cache_name = tempfile.gettempdir() + "/sdmx_datasets_cache"
    r = pandasdmx.Request("ESTAT", cache={"backend": "sqlite", "include_get_headers": True, "cache_name": cache_name})
    r.timeout = 180
    return r


estat = None


class Eurostat(IDataSourceManager):
    def __init__(self):
        global estat
        if not estat:
            estat = create_estat_request()
        d = {"backend": "sqlite", "include_get_headers": True, "cache_name": tempfile.gettempdir() + "/eurostat_bulk_datasets"}
        requests_cache.install_cache(**d)

    def get_name(self) -> str:
        """ Source name """
        return self.get_datasource().name

    def get_datasource(self) -> DataSource:
        """ Data source """
        src = DataSource()
        src.name = "Eurostat"
        src.description = "Eurostat is the statistical office of the European Union"
        return src

    def get_databases(self) -> List[Database]:
        """ List of databases in the data source """
        db = Database()
        db.code = ""
        db.description = "Eurostat provides all Datasets in a single database"
        return [db]

    @Memoize2
    def get_datasets(self, database=None) -> list:
        """ List of datasets in a database, or in all the datasource (if database==None)
            Return a list of tuples (database, dataset)
        """
        import xmltodict
        lst = []
        # Make a table of datasets, containing three columns: ID, description, URN
        # List of datasets
        xml = requests.get("http://ec.europa.eu/eurostat/SDMX/diss-web/rest/dataflow/ESTAT/all/latest")
        t = xml.content.decode("utf-8")
        j = xmltodict.parse(t)
        for k in j["mes:Structure"]["mes:Structures"]["str:Dataflows"]["str:Dataflow"]:
            for n in k["com:Name"]:
                if n["@xml:lang"] == "en":
                    desc = n["#text"]
                    break
            if k["@id"][:3] != "DS-":  # or k["@id"] in ("DS-066341", "DS-066342", "DS-043408", "DS-043409"):
                # dsd_id = k["str:Structure"]["Ref"]["@id"]
                lst.append((k["@id"], desc, k["@urn"]))

            # print(dsd_id + "; " + desc + "; " + k["@id"] + "; " + k["@urn"])
        return lst

    def get_dataset_structure(self, database, dataset) -> Dataset:
        """ Obtain the structure of a dataset: concepts, dimensions, attributes and measures """
        refs = dict(references='all')
        dsd_response = estat.datastructure("DSD_" + dataset, params=refs)
        dsd = dsd_response.datastructure["DSD_" + dataset]
        metadata = dsd_response.write()
        # SDMXConcept = collections.namedtuple('Concept', 'type name istime description code_list')
        # DataSource <- Database <- DATASET <- Dimension(s) (including Measures) <- CodeList
        #                                      |
        #                                      v
        #                                      Concept <- CodeList  (NOT CONSIDERED NOW)
        ds = Dataset()
        ds.code = dataset
        ds.description = None  # How to get description?
        ds.attributes = {}  # Dataset level attributes? (encode them using a dictionary)
        ds.metadata = None  # Metadata for the dataset SDMX (flow, date of production, etc.)
        ds.database = database  # Reference to containing database

        dims = {}

        for d in dsd.dimensions:
            istime = str(dsd.dimensions.get(d)).split("|")[0].strip() == "TimeDimension"
            dd = Dimension()
            dd.code = d
            dd.description = None
            dd.attributes = None
            dd.is_time = istime
            dd.is_measure = False
            dd.dataset = ds
            dims[d] = dd
        for m in dsd.measures:
            dd = Dimension()
            dd.code = m
            dd.description = None
            dd.attributes = None
            dd.is_time = False
            dd.is_measure = True
            dd.dataset = ds
            dims[m] = dd
        for a in dsd.attributes:
            ds.attributes[a] = None  # TODO Get the value
        for l in metadata.codelist.index.levels[0]:
            first = True
            # Read code lists
            cl = create_dictionary()
            for m, v in list(zip(metadata.codelist.loc[l].index, metadata.codelist.loc[l]["name"])):
                if not first:
                    cl[m] = v.replace("\n", ";")
                else:
                    first = False
            # Attach it to the Dimension or Measure
            if metadata.codelist.loc[l]["dim_or_attr"][0] == "D":
                # Build Code List from dictionary
                dims[l].code_list = CodeList.construct(l, None, [""], [CodeImmutable(k, cl[k], "", []) for k in cl])

        return ds

    def etl_full_database(self, database=None, update=False):
        """ If bulk download is supported, refresh full database """
        pass

    def etl_dataset(self, dataset, update=False) -> str:
        """
        Download a file (general purpose, not only for Eurostat datasets)

        :param url:
        :param local_filename:
        :param update:
        :return: String with full file name
        """
        url = "http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?downfile=data%2F" + dataset + ".tsv.gz"
        zip_name = tempfile.gettempdir() + "/" + dataset + '.tsv.gz'
        if os.path.isfile(zip_name):
            if not update:
                return zip_name
            else:
                os.remove(zip_name)

        r = requests.get(url, stream=True)
        # http://stackoverflow.com/questions/15352668/download-and-decompress-gzipped-file-in-memory
        with open(zip_name, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)

        return zip_name

    def get_dataset_filtered(self, dataset: str, dataset_params: dict) -> Dataset:
        """ This method has to consider the last dataset download, to re"""

        def multi_replace(text, rep):
            rep = dict((re.escape(k), v) for k, v in rep.items())
            pattern = re.compile("|".join(rep.keys()))
            return pattern.sub(lambda m: rep[re.escape(m.group(0))], text)

        # Read Eurostat dataset structure
        ds = self.get_dataset_structure(None, dataset)

        # Read full Eurostat dataset into a Dataframe
        dataframe_fn = tempfile.gettempdir() + "/" + dataset + ".bin2"
        df = None
        if os.path.isfile(dataframe_fn):
            df = pd.read_parquet(dataframe_fn)
            # df = pd.read_msgpack(dataframe_fn)

        if df is None:
            zip_name = self.etl_dataset(dataset, update=False)
            new_method = False
            if new_method:
                # TODO Obtain a Dataframe with pairs of columns for observations
                with gzip.open(zip_name, "rb") as gz:
                    st = None
                    fc = StringIO(st)
                    # Read header
                    csv_reader = csv.reader(fc)
                    header = next(csv_reader)
                    fc.seek(0)
                    # Obtain real header: each period column is added a
                    # Parse it

            else:
                with gzip.open(zip_name, "rb") as gz:
                    # Read file and
                    # Remove status flags (documented at http://ec.europa.eu/eurostat/data/database/information)
                    #
                    # TODO ISOLATE STATUS FLAGS INTO ANOTHER COLUMN
                    # TODO ":" -> "NaN\t",
                    # TODO ([-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?)(.*) -> (1)\t(2)
                    pattern = re.compile("(\\:)|( [b-fnpruzscde]+)")
                    st = pattern.sub(lambda m: "NaN" if m.group(0) == ":" else "", gz.read().decode("utf-8"))

                    # st = multi_replace(gz.read().decode("utf-8"),
                    #                    {":": "NaN", " p": "", " e": "", " f": "", " n": "", " c": "", " u": "",
                    #                     " z": "", " r": "", " b": "", " d": ""})
                    fc = StringIO(st)
                    # fc = StringIO(gz.read().decode("utf-8").replace(" p\t", "\t").replace(":", "NaN"))
                os.remove(zip_name)
                # Remove ":" -> NaN
                # Remove " p" -> ""
                df = pd.read_csv(fc, sep="\t")

                def split_codes(all_codes):  # Split and strip
                    return [s.strip() for s in all_codes.split(",")]

                original_column = df.columns[0]
                new_cols = [s.strip() for s in original_column.split(",")]
                new_cols[-1] = new_cols[-1][:new_cols[-1].find("\\")]
                temp = list(zip(*df[original_column].map(split_codes)))
                del df[original_column]
                df.columns = [c.strip() for c in df.columns]
                # Convert to numeric
                for cn in df.columns:
                    try:
                        df[cn] = df[cn].astype(np.float)
                    except ValueError:
                        print("BORRAME - Error conversion columna "+cn+", probablemente queda algun flag sin eliminar")
                    # df[cn] = df[cn].apply(lambda x: pd.to_numeric(x, errors='coerce'))
                # Add index columns
                for i, c in enumerate(new_cols):
                    df[c] = temp[i]
                # set index on the dimension columns
                df.set_index(new_cols, inplace=True)
                # Save df
                df.to_parquet(dataframe_fn)
                # df.to_msgpack(dataframe_fn)

        # Change dataframe index names to match the case of the names in the metadata
        # metadata_names_dict = {dim.code.lower(): dim.code for dim in ds.dimensions}
        # dataframe_new_names = [metadata_names_dict.get(name.lower(), name) for name in df.index.names]
        dataframe_new_names = translate_case(df.index.names, [dim.code for dim in ds.dimensions])
        df.index.names = dataframe_new_names

        # Filter it using generic Pandas filtering capabilities
        if dataset_params:
            ds.data = filter_dataset_into_dataframe(df, dataset_params, dataset, eurostat_postprocessing=True)
        else:
            ds.data = df

        return ds

    def get_refresh_policy(self):  # Refresh frequency for list of databases, list of datasets, and dataset
        pass


def multi_replace(text, rep):
    import re
    rep = dict((re.escape(k), v) for k, v in rep.items())
    pattern = re.compile("|".join(rep.keys()))
    return pattern.sub(lambda m: rep[re.escape(m.group(0))], text)


if __name__ == '__main__':
    e = Eurostat()
    e.get_dataset_filtered("sbs_na_ind_r2", None)
