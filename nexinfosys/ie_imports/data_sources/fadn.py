import getpass
import logging
import os
import tempfile
import zipfile
from typing import List
import pandas as pd

from nexinfosys import get_global_configuration_variable

pd.core.common.is_list_like = pd.api.types.is_list_like
import pandas_datareader.data as web
import numpy as np
import requests
import datetime
from io import StringIO

from nexinfosys.common.helper import Memoize2, import_names
from nexinfosys.ie_imports.data_source_manager import IDataSourceManager, filter_dataset_into_dataframe, \
    get_dataset_structure
from nexinfosys.models.statistical_datasets import DataSource, Database, Dataset, Dimension, CodeList, CodeImmutable

base_url = "http://ec.europa.eu/agriculture/rica/database/reports/"
measures_file = "http://ec.europa.eu/agriculture/rica/database/help/infometa.csv"

dimensions = {"Year": ("YEAR", "Y", "Year"), # Dimension name: (Dimension name in files, Dimension name in dataset code)
              "Country": ("COUNTRY", "C", "Country"),
              "Region": ("REGION", "R", "Region"),
              "TypeOfFarming8": ("TF8", "F8", "Type of farming 8 FADN"),
              "TypeOfFarming14": ("TF14", "F14", "Type of farming 14 FADN"),
              "TypeOfFarmingGen1": ("TF_GEN1", "FG", "Type of farming from EU Typology Gen 1"),
              "TypeOfFarmingPrin2": ("TF_PRIN2", "FP", "Type of farming from EU Typology Prin 2"),
              "TypeOfFarmingSubP4": ("TF_SUBP4", "FS", "Type of farming from EU Typology Sub P 4"),
              "EconomicSize6": ("SIZ6", "S6", "Economic Size 6 FADN"),
              "EconomicSizeEUTypology": ("SIZC", "SEU", "Economic Size EU Typology"),
              "OrganicProduction": ("ORGANIC", "ORG", "Organic Production categorization"),
              "ANC3": ("ANC3", "ANC", "ANC3"),
              "LFA": ("LFA", "LFA", "LFA")
              }

measures = {}  # Download and parse from  file at http://ec.europa.eu/agriculture/rica/database/help/infometa.csv

# Label and Code are computed from the dimension, plus
# the list is replicated for the combination SO/SGO and MEAN/MEDIAN, changing the label and the code used
datasets = [dict(dimensions=["Year", "Country"]),
            dict(dimensions=["Year", "Country", "TypeOfFarming8"]),
            dict(dimensions=["Year", "Country", "TypeOfFarming14"]),
            dict(dimensions=["Year", "Country", "TypeOfFarmingGen1", "TypeOfFarmingPrin2", "TypeOfFarmingSubP4"]),
            dict(dimensions=["Year", "Country", "EconomicSize6"]),
            dict(dimensions=["Year", "Country", "EconomicSizeEUTypology"]),
            dict(dimensions=["Year", "Country", "ANC3"]),
            dict(dimensions=["Year", "Country", "LFA"]),
            dict(dimensions=["Year", "Country", "OrganicProduction"]),
            dict(dimensions=["Year", "Country", "EconomicSize6", "TypeOfFarming8"]),
            dict(dimensions=["Year", "Country", "EconomicSize6", "TypeOfFarming14"]),
            dict(dimensions=["Year", "Country", "Region"]),
            dict(dimensions=["Year", "Country", "Region", "TypeOfFarming8"]),
            dict(dimensions=["Year", "Country", "Region", "TypeOfFarming14"]),
            dict(dimensions=["Year", "Country", "Region", "EconomicSize6"]),
            dict(dimensions=["Year", "Country", "Region", "EconomicSizeEUTypology"]),
            dict(dimensions=["Year", "Country", "Region", "EconomicSize6", "TypeOfFarming8"])
            ]

versions = sorted(["20120827", "20121212", "20130313", "20130521", "20130605", "20131108", "20131211",
                   "20140312", "20140325", "20140720", "20140917", "20141107",
                   "20150107", "20150504", "20160107", "20160217", "20170126", "20170129", "20170518",
                   "20170608", "20170717", "20170912", "20171113", "20171114", "20180120", "20180518", "20180610"
                   ])
# Number of datasets
# 15 * 2 (SO, SGM) * 2 (Media, Mediana) * number of versions


def generate_datasets(seed_datasets):
    ds_out = []
    for ds in seed_datasets:
        label = " - ".join(ds["dimensions"])
        fname = ".".join([dimensions[t][0] for t in ds["dimensions"]])
        code = ".".join([dimensions[t][1] for t in ds["dimensions"]])

        ds_out.append(dict(dimensions=ds["dimensions"], fname=fname, code=code+".AVG.SO", label=label + " - SO"))
        ds_out.append(dict(dimensions=ds["dimensions"], fname=fname+".MED", code=code+".MED.SO", label=label + " - SO (Mediane)"))
        ds_out.append(dict(dimensions=ds["dimensions"], fname=fname, code=code+".AVG.SGM", label=label + " - SGM"))
        ds_out.append(dict(dimensions=ds["dimensions"], fname=fname+".MED", code=code+".MED.SGM", label=label + " - SGM (Mediane)"))

    return ds_out


def get_fadn_directory():
    # EuroStat datasets
    if get_global_configuration_variable('FADN_FILES_LOCATION'):
        dir_name = get_global_configuration_variable('FADN_FILES_LOCATION')
        logging.debug(f"USER: {getpass.getuser()}, creating dir for FADN files")
    else:
        dir_name = tempfile.gettempdir() + "/fadn_datasets"

    if not os.path.isdir(dir_name):
        os.makedirs(dir_name)

    return dir_name


def download(url, file):
    logging.debug("download URL: "+url)
    logging.debug("file: "+file)
    if not os.path.isfile(file):
        r = requests.get(url, stream=True)
        if r.status_code == 200:
            # http://stackoverflow.com/questions/15352668/download-and-decompress-gzipped-file-in-memory
            with open(file, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)


def load_measures_dictionary(directory):
    fname = directory+"/measures.csv"
    if not os.path.isfile(fname):
        download(measures_file, fname)
    if not os.path.isfile(fname):
        raise Exception("Could not obtain measures dictionary file")
    # Read the file into a DataFrame
    with open(fname, "rb") as f:
        s = f.read().decode("ISO-8859-1")
    df = pd.read_csv(StringIO(s.replace("\"", "")), sep="^")
    d = {}
    en_pos = 1
    formula_pos = df.shape[1]-1
    for r in range(df.shape[0]):
        unit1 = None
        unit2 = None
        formula = df.iloc[r, formula_pos]
        if "pre-2014" in formula:
            s = formula[formula.find("pre-2014")+9:]
            unit1 = s[s.find("(")+1:s.find(")")]

        if "post-2013" in formula:
            s = formula[formula.find("post-2013")+10:]
            unit2 = s[s.find("(")+1:s.find(")")]

        add = True
        if unit1 and unit2:
            if unit1 != unit2:
                logging.debug("Different: "+df.iloc[r, 0]+"; "+formula)
                add = False

        if add:
            d[df.iloc[r, 0]] = (df.iloc[r, en_pos], formula)

    return d


def load_dataset(code, date, ds_lst, directory, base_url=base_url):
    """
    Loads a dataset into a DataFrame
    If the dataset is present, it decompresses it in memory to obtain one of the four datasets per file
    If the dataset is not downloaded, downloads it and decompresses into the corresponding version directory
    :param code:
    :param date:
    :param ds_lst: list of FADN datasets
    :param directory:
    :param base_url:
    :return:
    """

    def get_version_date():
        # Version date from date parameter
        vdate = None
        if not date:
            vdate = versions[-1]
        else:
            if isinstance(date, datetime.datetime):
                sdate = date.strftime("%Y%m%d")
            elif isinstance(date, str):
                sdate = date
            # Find previous nearest
            for v in reversed(versions):
                if v < sdate:
                    version_date = v
                    break
        return vdate

    version_date = get_version_date()

    # Dataset dictionary
    d = None
    for ds in ds_lst:
        if ds["code"] == code:
            d = ds
            break

    # Dir name
    dname = directory + os.sep + version_date
    # File name
    fname = dname + os.sep + d["fname"] + ".zip"

    # Check if the file exists
    if not os.path.isfile(fname):
        # If it is the latest version
        if version_date == versions[-1] and "MED" not in code:
            os.makedirs(dname)
            # Download directly the file
            download(base_url+d["fname"]+".zip", fname)
        else:
            # Download the full package
            zip_name = directory+f"{os.sep}fadn"+version_date+".zip"
            download(base_url+f"{os.sep}archives{os.sep}fadn"+version_date+".zip", zip_name)
            # Create directory
            target_name = directory + os.sep + version_date
            os.makedirs(target_name)
            # Uncompress it into the version directory
            z = zipfile.ZipFile(zip_name)
            z.extractall(target_name)
            z.close()

    # At this point the file should exist. If not, raise an Exception
    if not os.path.isfile(fname):
        raise Exception("Could not obtain the dataset "+code+" for processing")

    # Obtain the file from the ZIP file
    z = zipfile.ZipFile(fname)
    if code.endswith(".SO"):
        path = "SO"
    else:
        path = "SGM"
    b = z.open(path + os.sep + d["fname"] + ".csv").read().decode("utf-8")
    df = pd.read_csv(StringIO(b), sep=";")

    # Split dimension columns in two (except Year)
    # Also, remove rows where a dimension is "NaN"
    for dim in d["dimensions"]:
        if dim == "Year":
            continue
        col = dimensions[dim][0]
        # Remove rows where
        df = df[~df[col].isnull()]
        # Do the split
        tmp = df[col].str.split(")", 1).str
        df[col] = tmp[0].str[1:]
        df[col+"_DESC"] = tmp[1].str[1:]

    return df


class FADN(IDataSourceManager):
    def __init__(self, metadata_session_factory, data_engine):
        self._datasets = generate_datasets(datasets)
        self._base_directory = get_fadn_directory()
        self._measures = load_measures_dictionary(self._base_directory)
        self._metadata_session_factory = metadata_session_factory
        self._data_engine = data_engine

    def get_name(self) -> str:
        """ Source name """
        return self.get_datasource().name

    def get_datasource(self) -> DataSource:
        """ Data source """
        src = DataSource()
        src.name = "FADN"
        src.description = "The Farm Accountancy Data Network (FADN) is an instrument for evaluating the income of agricultural holdings and the impacts of the Common Agricultural Policy"
        return src

    def get_databases(self) -> List[Database]:
        """ List of databases in the data source """
        db = Database()
        db.code = ""
        db.description = "FADN provides all Datasets in a single database"
        return [db]

    @Memoize2
    def get_datasets(self, database=None) -> list:
        """ List of datasets in a database, or in all the datasource (if database==None)
            Return a list of tuples (database, dataset)
        """

        lst = []
        for d in self._datasets:
            lst.append((d["code"], d["label"]))

        return lst

    def get_dataset_structure(self, database, dataset) -> Dataset:
        """ Obtain the structure of a dataset: concepts, dimensions, attributes and measures """

        return get_dataset_structure(self._metadata_session_factory, self, dataset)

    def etl_full_database(self, database=None, update=False):
        """ If bulk download is supported, refresh full database """
        pass

    def etl_dataset(self, dataset, update=False) -> Dataset:
        """
        Download a FADN bulk file

        :param url:
        :param local_filename:
        :param update:
        :return: String with full file name
        """

        # Obtain the dictionary
        d = None
        for ds in self._datasets:
            if ds["code"] == dataset:
                d = ds
                break

        # Read the full file, but only to extract the code lists
        df = load_dataset(dataset, None, self._datasets, self._base_directory)

        # ---------------------------------------------------------
        #     Elaborate the dataset, with dimensions and measures
        # ---------------------------------------------------------
        ds = Dataset()
        ds.code = dataset
        ds.description = d["label"]
        ds.attributes = {}  # Dataset level attributes? (encode them using a dictionary)
        ds.metadata = None  # Metadata for the dataset SDMX (flow, date of production, etc.)
        ds.database = self.get_databases()[0]  # Assign in the calling function

        # Add Dimensions
        for dim in d["dimensions"]:
            dd = Dimension()
            dd.code = dimensions[dim][0]
            dd.description = dimensions[dim][2]
            dd.attributes = None
            dd.is_time = dim == "Year"
            dd.is_measure = False
            if dim == "Year":
                cl = sorted(list([str(c) for c in df[dd.code].unique() if c != np.nan]))
                dd.code_list = CodeList.construct(dim, dim, [""], [CodeImmutable(c, c, "", []) for c in cl])
            else:
                df_tmp = df[[dd.code, dd.code + "_DESC"]].drop_duplicates()
                cl = sorted(list([str(c) for c in df_tmp[dd.code].unique() if c != np.nan]))
                df_tmp.set_index(dd.code, inplace=True)
                dd.code_list = CodeList.construct(dim, dim, [""], [CodeImmutable(c, df_tmp.loc[c].values[0], "", []) for c in cl])

            dd.dataset = ds

        # Add Measures
        for m in df.columns:
            if m.endswith("_Q50"):
                t = m[:-len("_Q50")]
            else:
                t = m
            if t in self._measures:
                dd = Dimension()
                dd.code = m
                dd.description = self._measures[t][0]
                dd.attributes = None
                dd.is_time = False
                dd.is_measure = True
                dd.code_list = None
                dd.dataset = ds

        return ds

    def get_dataset_filtered(self, dataset: str, dataset_params: dict) -> Dataset:
        """ This method has to consider the last dataset download, to re"""

        # Read dataset structure
        ds = self.get_dataset_structure(None, dataset)

        df = load_dataset(dataset, None, self._datasets, self._base_directory)

        # Obtain dataset dictionary
        d = None
        for dd in self._datasets:
            if dd["code"] == dataset:
                d = dd
                break

        # Remove descriptions
        # Lower case contents of dimension columns
        lst_idx_cols = []
        for dim in d["dimensions"]:
            col = dimensions[dim][0]
            lst_idx_cols.append(col.lower())
            if col.lower() != "year":
                del df[col+"_DESC"]
                df[col] = df[col].str.lower()

        # Lower case column names
        df.columns = [c.lower() for c in df.columns]
        df.set_index(lst_idx_cols, inplace=True)

        # Filter it using generic Pandas filtering capabilities
        if dataset_params["StartPeriod"] and dataset_params["EndPeriod"]:
            years = [str(y) for y in range(int(dataset_params["StartPeriod"][0]), int(dataset_params["EndPeriod"][0])+1)]
            dataset_params["year"] = years
            del dataset_params["StartPeriod"]
            del dataset_params["EndPeriod"]
        ds.data = filter_dataset_into_dataframe(df, dataset_params, dataset)

        return ds

    def get_refresh_policy(self):  # Refresh frequency for list of databases, list of datasets, and dataset
        pass


if __name__ == '__main__':
    datasets = generate_datasets(datasets)
    d = load_measures_dictionary("/home/rnebot/Downloads/fadn")
    df = load_dataset("Y.C.MED.SO", None, datasets, "/home/rnebot/Downloads/fadn")
    print(df)
