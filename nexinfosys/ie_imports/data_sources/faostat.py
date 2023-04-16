import logging
import os
import subprocess
from io import StringIO
from typing import List
from zipfile import ZipFile
import numpy as np
import pandas as pd
import requests
import sqlalchemy

from nexinfosys.ie_imports.data_source_manager import IDataSourceManager, get_dataset_structure
from nexinfosys.models import log_level
from nexinfosys.models.statistical_datasets import DataSource, Database, Dataset, Dimension, CodeList, CodeImmutable

# eng = sqlalchemy.create_engine("monetdb:///demo:", echo=True)
# df = pd.DataFrame(np.random.randn(6, 4), index=pd.date_range('20130101', periods=6), columns=list('ABCD'))
# df.to_sql('test', eng, if_exists='append')

logger = logging.getLogger(__name__)
logger.setLevel(log_level)


"""
To obtain the structure, it is assumed the download process parses the file to obtain dimensions
FAOSTAT needs access to the metadata database

Full database is an iteration?
Also, create full code lists for the different concepts

"""

dataset_csv = """
Code,File,Label
AE,ASTI_Expenditures,ASTI-Expenditures
AF,ASTI_Researchers,ASTI-Researchers
BC,CommodityBalances_Crops,Commodity Balances – Crops Primary Equivalent
BL,CommodityBalances_LivestockFish,Commodity Balances – Livestock and Fish Primary Equivalent
CC,FoodSupply_Crops,Food Supply – Crops Primary Equivalent
CISP,Investment_CountryInvestmentStatisticsProfile_,Country Investment Statistics Profile
CL,FoodSupply_LivestockFish,Food Supply – Livestock and Fish Primary Equivalent
CP,ConsumerPriceIndices,Consumer Price Indices
CS,Investment_CapitalStock,Capital Stock
EA,Development_Assistance_to_Agriculture,Development Flows to Agriculture
EC,Environment_AirClimateChange,Air and Climate change
EE,Environment_Energy,Energy
EF,Environment_Fertilizers,Fertilizers
EI,Environment_Emissions_intensities,Emissions intensities
EK,Environment_LivestockPatterns,Livestock patterns
EL,Environment_LandUse,Land Use
EM,Environment_Emissions_by_Sector,Emissions by Sector
EP,Environment_Pesticides,Pesticides
ES,Environment_Soil,Soil
ET,Environment_Temperature_change,Temperature change
EW,Environment_Water,Water
FA,Food_Aid_Shipments_WFP,Food Aid Shipments (WFP)
FBS,FoodBalanceSheets,Food Balance Sheets
FDI,Investment_ForeignDirectInvestment,Foreign Direct Investment (FDI)
FO,Forestry,Forestry Production and Trade
FS,Food_Security_Data,Suite of Food Security Indicators
FT,Forestry_Trade_Flows,Forestry Trade Flows
GA,Emissions_Agriculture_Crop_Residues,Crop Residues
GB,Emissions_Agriculture_Burning_crop_residues,Burning – Crop Residues
GC,Emissions_Land_Use_Cropland,Cropland
GE,Emissions_Agriculture_Enteric_Fermentation,Enteric Fermentation
GF,Emissions_Land_Use_Forest_Land,Forest Land
GG,Emissions_Land_Use_Grassland,Grassland
GH,Emissions_Agriculture_Burning_Savanna,Burning – Savanna
GI,Emissions_Land_Use_Burning_Biomass,Burning – Biomass
GL,Emissions_Land_Use_Land_Use_Total,Land Use Total
GM,Emissions_Agriculture_Manure_Management,Manure management
GN,Emissions_Agriculture_Energy,Energy Use
GP,Emissions_Agriculture_Manure_left_on_pasture,Manure left on Pasture
GR,Emissions_Agriculture_Rice_Cultivation,Rice Cultivation
GT,Emissions_Agriculture_Agriculture_total,Agriculture Total
GU,Emissions_Agriculture_Manure_applied_to_soils,Manure applied to Soils
GV,Emissions_Agriculture_Cultivated_Organic_Soils,Cultivation of Organic Soils
GY,Emissions_Agriculture_Synthetic_Fertilizers,Synthetic Fertilizers
HS,Indicators_from_Household_Surveys,"Indicators from Household Surveys (gender, area, socieoconomics)"
IC,Investment_CreditAgriculture,Credit to Agriculture
IG,Investment_GovernmentExpenditure,Government Expenditure
LC,Environment_LandCover,Land Cover
MK,Macro-Statistics_Key_Indicators,Macro Indicators
OA,Population,Annual population
OE,Employment_Indicators,Employment Indicators
PA,PricesArchive,Producer Prices – Archive
PD,Deflators,Deflators
PE,Exchange_rate,Exchange rates – Annual
PI,Price_Indices,Producer Price Indices – Annual
PM,Prices_Monthly,Producer Prices – Monthly
PP,Prices,Producer Prices – Annual
QA,Production_Livestock,Live animals
QC,Production_Crops,Crops
QD,Production_CropsProcessed,Crops processed
QI,Production_Indices,Production Indices
QL,Production_LivestockPrimary,Livestock Primary
QP,Production_LivestockProcessed,Livestock Processed
QV,Value_of_Production,Value of Agricultural Production
RA,Inputs_FertilizersArchive,Fertilizers Archive
RF,Inputs_Fertilizers,Fertilizers
RL,Inputs_Land,Land Use
RM,Investment_Machinery,Machinery
RP,Inputs_Pesticides_Use,Pesticides Use
RT,Inputs_Pesticides_Trade,Pesticides Trade
RV,Inputs_FertilizersTradeValues,Fertilizers – Trade Value
RY,Investment_MachineryArchive,Machinery Archive
TA,Trade_LiveAnimals,Live animals
TI,Trade_Indices,Trade indices
TM,Trade_DetailedTradeMatrix,Detailed trade matrix
TP,Trade_Crops_Livestock,Crops and livestock products
"""


class FAOSTAT(IDataSourceManager):  # FAOStat (not AquaStat)
    def __init__(self, datasets_directory, metadata_session_factory, data_engine):
        self._bulk_download_url = "http://fenixservices.fao.org/faostat/static/bulkdownloads/"
        self._datasets = pd.read_csv(StringIO(dataset_csv))
        self._datasets["idx"] = self._datasets["Code"]
        self._datasets.set_index("idx", inplace=True)
        self._datasets_directory = datasets_directory
        if not os.path.isdir(self._datasets_directory):
            os.makedirs(self._datasets_directory, mode=0o770)
        self._metadata_session_factory = metadata_session_factory
        self._data_engine = data_engine

    def get_name(self) -> str:
        """ Source name """
        return self.get_datasource().name

    def get_datasource(self) -> DataSource:
        """ Data source """
        src = DataSource()
        src.name = "FAO"
        src.description = "Food and Agriculture Organization"
        return src

    def get_databases(self) -> List[Database]:
        """ List of databases in the data source """
        db = Database()
        db.code = "FAOSTAT"
        db.description = "FAOSTAT is the main database provided by FAO. Other databases are not available currently"
        return [db]

    def get_datasets(self, database=None) -> List:
        """ List of datasets in a database, or in all the datasource (if database==None)
            Return a list of tuples (database, dataset)
        """
        return [(r["Code"], r["Label"], r["Code"]) for i, r in self._datasets.iterrows()]

    def get_dataset_structure(self, database, dataset) -> Dataset:
        """ Obtain the structure of a dataset: concepts, dimensions, attributes and measures """
        # The manager tries to access the metadata database
        # When this function is called, it is because no metadata is available
        # So, perform the ETL, which includes:
        # * download (if not available),
        # * elaboration of metadata and
        # * import of data
        #
        # TODO Signal a "being updated" status for the dataset
        # TODO Delete all metadata about the dataset.
        ds = get_dataset_structure(self._metadata_session_factory, self, dataset)
        measure_found = False
        for d in ds.dimensions:
            if d.code.lower() == "value":
                measure_found = True
                break
        if not measure_found:
            dd = Dimension()
            dd.code = "Value"
            dd.description = None
            dd.attributes = None
            dd.is_time = False
            dd.is_measure = True
            dd.dataset = ds

        return ds

    def get_dataset_filtered(self, dataset, dataset_params: List[tuple]) -> Dataset:
        """ This method has to consider the last dataset download, to re"""
        # Check for the presence of the dataset in the metadata database
        # Check for the presence of data in the data database (count records)
        ds = get_dataset_structure(self._metadata_session_factory, self, dataset)

        # Read dimensions and their code lists
        dims = {}
        for dim in ds.dimensions:
            dims[dim.code] = dim.code_list.to_dict()
            if dim.is_time:
                interval_start = None
                interval_end = None
                for p in dataset_params.copy():
                    if p.lower() in ["starttime", "startperiod"]:
                        interval_start = dataset_params[p][0]
                        del dataset_params[p]
                    elif p.lower() in ["endtime", "endperiod"]:
                        interval_end = dataset_params[p][0]
                        del dataset_params[p]
                if interval_start and interval_end:
                    dataset_params[dim.code] = [str(i) for i in range(int(interval_start), int(interval_end)+1)]

        # Time

        # Prepare the query (WITH count and columns versions)
        # TODO IMPROVEMENTS: Add GROUP BY if a pivot table is available. Pass the list of dimensions, plus the aggregation as TWO new parameters
        sql = "SELECT * FROM \""+dataset+"\" WHERE "
        first = True
        for col, values in dataset_params.items():
            field_name = col+"_id"
            if first:
                first = False
            else:
                sql += " AND "
            if values:
                if not isinstance(values, list):
                    values = [values]
                sql += "(\""+field_name+"\" IN ("+", ".join(["'"+str(v)+"'" for v in values])+"))"
        # Execute the query store it in pd.DataFrame "df"
        # inspec = sqlalchemy.inspect(self._data_engine)
        # for table_name in inspec.get_table_names():
        #     print(table_name)
        conn = self._data_engine.connect()
        res = conn.execute(sql)
        result_list = res.fetchall()
        conn.close()
        if result_list:
            cols = []
            for f in result_list[0].keys():
                if f.endswith("_id"):
                    # Look for dimension named the same
                    for dim in dims:
                        if dim.lower() == f[:-3].lower():
                            cols.append(dim)
                            break
                else:
                    cols.append(f)
            df = pd.DataFrame(result_list, columns=cols)
            # # Add the descriptions for CODE columns
            # for d, cl in dims.items():
            #     df[d+" (desc.)"] = df[d.lower()+"_id"].map(cl)
        ds.data = df
        return ds

    def get_refresh_policy(self):  # Refresh frequency for list of databases, list of datasets, and dataset
        pass

    def etl_full_database(self, database=None, update=False):
        """ If bulk download is supported, refresh full database """
        pass

    def etl_dataset(self, dataset, update=False) -> str:
        """
        Download a file for an FAO Dataset
        It provokes a total refresh of its Data and Metadata

        :param url:
        :param local_filename:
        :param update:
        :return: String with full file name
        """
        # Find (the exact name is not known before hand)
        file_base = self._datasets.loc[dataset, "File"]
        file_suffix = ["_E_All_Data_(Normalized).zip", "_E_All_Data_(Norm).zip"]
        for i in file_suffix:
            file = self._datasets_directory + file_base + i
            if os.path.isfile(file):
                if update:
                    os.remove(file)
                break
        # TODO Check, if possible, if the file on the Web is newer than the file at the file system
        # Download the file if it is not in the file system
        for i in file_suffix:
            url = self._bulk_download_url + file_base + i
            file = self._datasets_directory + file_base + i
            if not os.path.isfile(file):
                r = requests.get(url, stream=True)
                if r.status_code == 200:
                    # http://stackoverflow.com/questions/15352668/download-and-decompress-gzipped-file-in-memory
                    with open(file, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=1024):
                            if chunk:  # filter out keep-alive new chunks
                                f.write(chunk)
                    break
            else:
                break

        # Read the dataset, with two simultaneous operations: collect metadata (do not write it),
        # and transfer data to the data database
        ds = self._process_single_file(self._datasets_directory, self._datasets.loc[dataset],
                                       file, self._metadata_session_factory, self._data_engine)

        ds.database = self.get_databases()[0]

        return ds

    @staticmethod
    def _process_single_file(datasets_directory, dataset_row, file, engine_metadata, engine_data) -> Dataset:
        """
        Obtain the structure of the dataset
        * Dimensions: each with their code lists
          - Unit as dimension
          - Year is its own code
        * Measures: there is a special column for the measures. Each of the detected codes
        * Measure attributes: add "Flag" and "Note" to each measure
        * Concepts: create also "Concept"s. If they exist, link; if not create them
        * ¿Time Dimension? Not special:
        :param dataset_row: Row of the DataFrame self._datasets with "Code", "File" and "Label" columns 
        :param file: Full file name to process
        :param engine_metadata: Database where Metadata will be added
        :param engine_data: Database where Data will be added
        :return: A Dataset, not persisted
        """
        def get_file_sample(fi):
            # Open the file, get the header
            try:
                zip_file = ZipFile(fi, "r")
                fname = zip_file.namelist()[0]
                tmp = pd.read_csv(zip_file.open(fname), nrows=5, encoding="cp1252")
            except NotImplementedError:
                new_file = datasets_directory + "/" + fname
                if not os.path.exists(new_file):
                    cmd = "/usr/bin/unzip -p \"" + fi + "\" \"" + fname + "\" > \"" + new_file + "\""
                    logging.debug(cmd)
                    subprocess.call(cmd, shell=True)
                fi = new_file
                tmp = pd.read_csv(fi, nrows=5, encoding="cp1252")
            return tmp, fi

        logging.debug("Processing "+dataset_row["Label"]+" dataset ...")

        # pd.DataFrame (first rows of the dataset) + name of file to be read
        tmp, file = get_file_sample(file)
        measures_attributes = ["Flag", "Unit"]

        # Special cases
        if "Year" in tmp.columns and "Year Code" not in tmp.columns:
            add_year_code_column = True
            tmp["Year Code"] = tmp["Year"]
        else:
            add_year_code_column = False

        if "Note" in tmp.columns:
            measures_attributes.append("Note")

        if "Measure" in tmp.columns:
            several_measures = True
        else:
            several_measures = False

        # TODO "ElementGroup" ????

        # Look for pairs of dimensions "<Dim>" and "<Dim> Code", create a dictionary if found, if it does not yet exist
        code_lists = {}  # Dictionary of code lists, each code list's value is a flat dictionary of codes:descriptions
        cols_to_keep = []  # Column names in the DataFrame to KEEP. Used to filter the DataFrame to ONLY these columns
        cols_to_skip = set()  # Columns to be skipped (if there is "Col Code" and "Col", "Col" is skipped)
        col_field_names = []  # Column FIELD names (in the target database)
        dims = set()  # Set of dimensions
        meas = set()  # Set of measures
        attr = set()  # Set of attributes
        # Pairs of columns
        for c in tmp.columns:
            if c.strip().lower().endswith("code"):
                dname = c[:-len("Code")].strip()  # Dimension name without "Code" suffix
                if dname in tmp.columns:
                    cols_to_skip.add(dname)
                    dims.add(dname)
                    cols_to_keep.append(c)
                    col_field_names.append(dname+"_id")
                    if dname.lower() not in code_lists:
                        code_lists[dname.lower()] = {}
        # Columns not in pairs
        for c in tmp.columns:
            if c not in cols_to_skip and c not in cols_to_keep:
                cols_to_keep.append(c)
                col_field_names.append(c)

        # A pass through ALL the file, gather a DICT for each DIMENSION -> CodeList
        count2 = 0
        zip_file = ZipFile(file, "r")
        fname = zip_file.namelist()[0]
        for data in pd.read_csv(zip_file.open(fname), chunksize=100000, encoding="cp1252"):
            logging.debug(f"Chunk: {count2+1}")
            if add_year_code_column:
                data["Year Code"] = data["Year"]
            # For each Dimension, collect DISTINCT values
            for c in data.columns:
                # Look for a pair of dimensions, add codes and descriptions to it
                # For pairs of dimensions, COLLECT DISTINCT pairs (CODE, DESCRIPTION)
                # (this will go to the concept or to the dimension)
                if c.strip().lower().endswith("code"):
                    dname = c[:-len("Code")].strip()
                    if dname in tmp.columns:
                        suffix = c[len(dname):]  # Some times "Code", most of the time " Code"
                        data[dname + suffix] = data[dname+suffix].astype(str)
                        data[dname] = data[dname].astype(str)
                        cl = code_lists[dname.lower()]
                        cl.update(dict(zip(data[dname+suffix].values, data[dname].values)))
            dtypes = {}
            if engine_data:
                logger.debug(f"Writing")
                # Remove unneeded columns
                data = data[cols_to_keep]
                data.reset_index(inplace=True)
                del data["index"]
                # Rename columns
                data.columns = col_field_names
                if not dtypes:
                    for i, t in enumerate(data.dtypes):
                        if t == np.dtype('O'):
                            dtypes[data.columns[i]] = sqlalchemy.types.String(length=16)  # Unicode 16 caracteres
                        elif t == np.dtype('float64'):
                            dtypes[data.columns[i]] = sqlalchemy.types.Float(30)  # Float

                # TODO Possibly, add convenient columns
                # Insert into "Datamart", regenerate table if it exists, only in the first chunk
                data.to_sql(dataset_row["Code"], engine_data, if_exists='append' if count2 > 0 else 'replace', index=False, dtype=dtypes)
                logger.debug("Written")
            count2 += 1

        # ------------------------------
        #     Elaborate the dataset
        # ------------------------------
        ds = Dataset()
        ds.code = dataset_row["Code"]
        ds.description = dataset_row["Label"]
        ds.attributes = {}  # Dataset level attributes? (encode them using a dictionary)
        ds.metadata = None  # Metadata for the dataset SDMX (flow, date of production, etc.)
        ds.database = None  # Assign in the calling function

        # Add Dimensions
        for d in dims:
            if d not in measures_attributes and d != "Value":
                dd = Dimension()
                dd.code = d
                dd.description = d
                dd.attributes = None
                dd.is_time = d in ["Year", "Month"]
                dd.is_measure = False
                dd.code_list = \
                    CodeList.construct(d, d, [""],
                                       [CodeImmutable(k, v, "", []) for k, v in code_lists[d.lower()].items()])
                dd.dataset = ds

        # TODO Add measures
        # All datasets have a single measure, which could be named after the dataset name
        # Measures have attributes: flag, unit and note
        # In one of the datasets there can be several measures
        # The dataset shows dimensions with their codes
        # The Measure column shows
        return ds


# ----------------------------------------------------------------------------------------------------------------------
#   Exploration of dimensions per dataset and codes per dimension
# ----------------------------------------------------------------------------------------------------------------------

def elaborate_dimensions_and_categories_dataframes(dictionary_file, files_directory, engine_metadata, engine_data, output_file_name):
    """
    Elaborate a dataframe containing all datasets in rows, all dimensions in rows, and a check for a cross when a dataset has a dimension
    Elaborate a dataframe per dimension, all dimension values in rows, datasets in columns, showing which codes is using each of the datasets
    Elaborate a PANEL with the result

    :param dictionary_file: A CSV file containing the correspondence between FAOSTAT dataset files and dataset codes and labels
    :param files_directory: A directory containing all FAOSTAT files
    :return: An Excel file with the result
    """
    import glob

    def get_file_name(f0):
        # Find files matching, take the newest one
        fi = None
        file_date = None
        pattern = files_directory + "/" + f0.strip() + "_E_All*.zip"
        for f2 in glob.glob(pattern):
            if not file_date or file_date < os.path.getmtime(f2):
                file_date = os.path.getmtime(f2)
                fi = f2
        if not fi:
            raise Exception(f0 + " NOT FOUND")
        return fi

    def get_file_sample(f0):
        fi = get_file_name(f0)
        # Open the file, get the header
        try:
            fname = ZipFile(fi, "r").namelist()[0]
            tmp = pd.read_csv(fi, nrows=5, encoding="cp1252")
        except NotImplementedError:
            new_file = files_directory + "/" + fname
            if not os.path.exists(new_file):
                cmd = "/usr/bin/unzip -p \""+fi+"\" \""+fname+"\" > \""+new_file+"\""
                logging.debug(cmd)
                subprocess.call(cmd, shell=True)
            fi = new_file
            tmp = pd.read_csv(fi, nrows=5, encoding="cp1252")
        return tmp, fi

    ds = pd.read_excel(dictionary_file)
    m1 = {r["Code"]: r["File"] for i, r in ds.iterrows()}
    m1_rev = {r["File"]: r["Code"] for i, r in ds.iterrows()}
    df1 = pd.DataFrame(index=m1.values())
    d_df = {}  # Dictionary of dataframes, one per dimension
    d_concept = {}  # Dictionary of concepts, each concept's value is a flat dictionary of codes:descriptions
    # TODO A complication would be to prepare for a full code list model, ready to be persisted

    # Just check that all files are in the directory
    f_prefixes = tuple([f for f in ds["File"]])
    _ = [get_file_name(f) for f in f_prefixes]
    # Then check which files are NOT in the list
    pattern = files_directory + "/*_E_All*.zip"
    nf = []

    for f in glob.glob(pattern):
        if not os.path.basename(f).startswith(f_prefixes):
            logging.debug(f + " not registered")
            nf.append(f)

    for f in f_prefixes:
        logging.debug("Processing "+f+" dataset ...")

        # pd.DataFrame (first rows of the dataset) + name of file to be read
        tmp, file = get_file_sample(f)

        # Mark dimensions present in the current Dataset
        # Create an empty dataframe for new dimensions
        #
        lst_cols = []
        lst_col_names = []
        skip_col = set()
        for c in tmp.columns:
            df1.loc[f, c] = "X"
            # Codes for the column, if it is not a special column
            if c not in d_df and c.upper() not in ["VALUE"]:
                d_df[c] = pd.DataFrame(index=m1.values())
            # Look for a pair, create a dictionary in that case, if it does not yet exist
            if c.strip().lower().endswith("code"):
                dname = c[:-len("Code")].strip()
                if dname in tmp.columns:
                    skip_col.add(dname)
                    lst_cols.append(c)
                    lst_col_names.append(dname.lower()+"_id")
                    if dname.lower() not in d_concept:
                        d_concept[dname.lower()] = {}
            elif c.strip() not in skip_col:
                lst_cols.append(c)
                lst_col_names.append(c.lower())

        # A pass through ALL the file, gather a SET for each DIMENSION
        if engine_data:
            pass
            # TODO Remove existing table

        sets_dict = {}
        count2 = 0
        for data in pd.read_csv(file, chunksize=10000, encoding="cp1252"):
            logging.debug("Chunk "+str(count2+1))
            for c in data.columns:
                if c.upper() not in ["VALUE"]:
                    if c not in sets_dict:
                        sets_dict[c] = set()
                    sets_dict[c].update(data[c].astype(str))
                # Look for a pair of dimensions, add codes and descriptions to it
                if c.strip().lower().endswith("code"):
                    dname = c[:-len("Code")].strip()
                    if dname in tmp.columns:
                        suffix = c[len(dname):] # Some times "Code", most of the time " Code"
                        dd = d_concept[dname.lower()]
                        dd.update(dict(zip(data[dname+suffix], data[dname])))
            if engine_data:
                # Remove unneeded columns
                data = data[lst_cols]
                data.reset_index(inplace=True)
                del data["index"]
                # Rename columns
                data.columns = lst_col_names
                # TODO Possibly, add convenient columns
                # Insert into "Datamart", regenerate table if it exists, only in the first chunk
                data.to_sql(m1_rev[f], engine_data, if_exists='append' if count2 > 0 else 'replace', index=False)
            count2 += 1
        # Once file is scanned, write sets for each DIMENSION DataFrame, for the current file
        for c in tmp.columns:
            if c in d_df:
                df = d_df[c]
                for cd in sets_dict[c]:
                    df.loc[f, cd] = "X"

    # Remove empty DataFrames
    d_df = {k: d_df[k] for k in d_df if d_df[k].shape[1] > 0}
    # Write to Excel
    writer = pd.ExcelWriter(output_file_name)
    # Write each worksheet, shorten name to <=31 characters (Excel format limitation)
    df1.to_excel(writer, sheet_name="Dimensions per File")
    _ = [d_df[k].to_excel(writer, sheet_name=k[:31]) for k in d_df]
    if engine_metadata:
        pass
        # TODO Source "FAO", database "FAOSTAT"
        # TODO datasets ds["File"], ds["Code"], ds["Description"]
        # TODO Concepts: create or update all dimensions plus their code lists


"""
 
* Importar conceptos (dimensiones)
  - Con sus listas de códigos
* Importar metadataos de datasets
  - Con sus dimensiones
  - Con sus medidas
  - Apuntando a conceptos
* Importar datos, cada dataset en su tabla. Medidas como dimensión, un campo adicional. Luego, valor y flag. Menos eficiente en espacio, ¿más eficiente en lectura?
* Lectura. Filtrar, obtener dataframe

------------------------------------------------------------------------

* Create MonetDB container "magic-monetdb". Use a Data Volume
* Change monetdb user password
* Python, package pymonetdb and sqlalchemy_monetdb
* Test import
* Install MonetDB JDBC for PyCharm access

"Datamart"

* Import FAO
  - Get structure, write into 
  - Write into relational using SQLAlchemy
  - Dimensions and Measure code as columns
  - Value and Attributes as separate columns
  - If there is a single Measure, ok
  - Map dataset to connection string plus table
* Query FAO
  - Get structure. Dimensions and Measures. Each has 
  - Get dataset
    - Filter
    - Into a Dataframe
    - 
* Make Magic Box use the new approach
  - Refactor
  
External Repositories
* Own Excel
* File system
* Repository
* OECD
* FAOSTAT


Common DS
* Data cube, JSONStat and SDMX style

Storage
* "Datamart"
* 





* All files
  - Delete all data source dimensions
  - 
* A single dataset
  - Delete dataset dimensions
  - Update concept (add codes, update description of codes)
  - Create dimensions
  - Delete dataset table
  - Create dataset table

"""

#a = ZipFile("/home/rnebot/DATOS/FAOSTAT/FoodBalanceSheets_E_All_Data_(Normalized).zip", "r")
#b = a.open(a.namelist()[0])

# EXECUTE THIS

if __name__ == '__main__':
    db_connection_string = "postgresql://postgres:postgres@localhost:5432/FAOSTAT"
    engine_data = sqlalchemy.create_engine(db_connection_string, echo=False)

    elaborate_dimensions_and_categories_dataframes("/home/rnebot/GoogleDrive/AA_MAGIC/FAOSTAT_datasets.xlsx",
                                                   "/home/rnebot/DATOS/FAOSTAT",
                                                   None,
                                                   engine_data,
                                                   "/home/rnebot/GoogleDrive/AA_MAGIC/FAOSTAT_analysis.xlsx")
