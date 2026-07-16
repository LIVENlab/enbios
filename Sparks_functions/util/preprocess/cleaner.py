"""
@author: Alexander de Tomás (ICTA-UAB)
        -LexPascal
"""
import pandas as pd
from Sparks_functions.generic.generic_dataclass import *
from tqdm import tqdm
from dataclasses import asdict
from pandas.api.types import is_numeric_dtype
import re
import logging
import pandera.pandas as pa

from Sparks_functions.generic.basefile_schema import schema

pd.options.mode.chained_assignment = None  #
tqdm.pandas()
bd.projects.set_current(bw_project)
logger = logging.getLogger("sparks")


class Cleaner:
    """Clean the input data and modify the units"""

    def __init__(self,
                 motherfile: str,
                 file_handler: dict,
                 national: Optional[bool] = False,
                 specify_database: Optional[bool] = False,
                 additional_columns: Optional[List[str]] = None
                 ):
        logger.debug("Initiating the Claner class")

        self.national = national
        self.specify_database = specify_database
        self.mother_file = motherfile
        self.final_df = None
        self.techs_region_not_included = []
        self.file_handler = file_handler
        self.additional_columns = additional_columns or []
        self._edited = False

    @staticmethod
    def create_template_df() -> pd.DataFrame:
        """
        Basic template for clean data
        """
        # todo: remove from, here
        columns = ['spores',
                   "techs",
                   "carriers",
                   "energy_value"]
        logger.debug(f"Creating empty dataframe template with {columns}")
        df = pd.DataFrame(columns=columns)

        assert list(df.columns) == columns, "Template df didn't work as expected"
        return df

    def _verify_csv(self, source: str) -> None:
        """
        Verify that the value passed has a csv extension and exists in self.file_handler.
        """
        if source not in self.file_handler:
            message = f"File key {source} not found in file handler dictionary. Check the basefile and the files in the directory"
            logger.error(message)
            raise KeyError(message)

        if not source.endswith('.csv'):
            message = f"File {source} is not a csv file"
            logger.error(message)
            raise KeyError(message)

    def _validate_basefile(self):
        """ Get some basic debug information about the basefile"""
        logger.info(f"Validating basefile schema...")
        try:
            schema.validate(self.basefile)
        except pa.errors.SchemaError as exc:
            logger.error(f"Schema validation failed for basefile {self.mother_file}")
            failure_cases = getattr(exc, "failure_cases", None)
            if failure_cases is not None:
                logger.error("Validation failed at these locations:\n%s", failure_cases)
            raise ValueError(f"Schema validation failed for basefile {self.mother_file})") from exc

        logger.debug(f"Files passed in basefile: {self.basefile['File_source'].unique()}")

        for item in self.basefile['File_source'].unique().tolist():
            if item not in self.file_handler.keys():
                logger.error(f"File defined in basefile {item} not present in the base folder")
                raise ValueError(f"File defined in basefile {item} not present in the base folder")


    def _verify_national(self) -> None:
        """
        Check locations in the motherfile and rise a warning if it looks like national
        """
        if not self.national:
            regions_series = self.basefile.get('Region')
            if regions_series is None:
                return
            regions = regions_series.dropna().astype(str).unique().tolist()

            pattern = re.compile(r"[-_].+")
            subnational_regions = [r for r in regions if pattern.search(r)]

            if len(subnational_regions) < 1:
                message = f"""Region names look national (no '-' or '_') in the Basefile. \n
                              Since national was defined as False, but data looks like national, this could lead to critical errors in the results \n
                              If you expected subnational detail, review the 'Region' values in the basefile."""
                warnings.warn(message)
                logger.warning(message)
                logger.debug(f"Regions passed {regions}")


    def _load_data(self, source: str) -> pd.DataFrame:
        """ Assuming comma separated input, load the data from a specific file"""
        self._verify_csv(source)

        try:
            logger.info(f"Loading data from {source}")
            data = pd.read_csv(self.file_handler[source], sep=None, engine='python').dropna()
            return data

        except FileNotFoundError as e:
            logger.error(f"Failed to load {source}: {e}")
            raise FileNotFoundError(f"File {source} does not exist") from e


    def _input_checker(self, data: pd.DataFrame, filename: str) -> pd.DataFrame:
        # TODO: transform this into a schema validation - version 1.2.0
        """
        Validate and standardize the input DataFrame structure.

        This function ensures that required columns are present, renames specific columns for consistency,
        and injects default values where necessary. It assumes that the column corresponding to the `filename`
        parameter contains the main energy value to be analyzed.

        Scenarios / spores are now treated as synonyms
        Locs / nodes are now consiered synonyms

        """
        filename_base = filename.split('.')[0]

        logger.debug(f"Checking missing columns for {filename}. Initial columns {data.columns}")
        # Standardize scenario/spores column
        if 'spores' not in data.columns:
            if 'scenario' in data.columns:
                logger.debug(f"renaming --scenario-- for spores in {filename}")
                data = data.rename(columns={'scenario': 'spores'})
            else:
                data['spores'] = 0  # Default spores value

        if 'locs' not in data.columns:
            if 'nodes' in data.columns:
                logger.debug(f"renaming --nodes-- for --locs-- in {filename}")
                data = data.rename(columns={'nodes': 'locs'})

        if 'Unnamed: 0' in data.columns:
            data = data.drop(columns='Unnamed: 0')

        if 'carriers' not in data.columns:
            logger.warning(
                f"carriers column not found in {filename}. Adding a default_carrier column. Add this carrier to the basefile")
            data['carriers'] = 'default_carrier'

        expected_columns = {'spores', 'techs', 'locs', 'carriers', filename_base}

        missing_columns = expected_columns - set(data.columns)
        if missing_columns:
            logger.error(f"Missing required columns in {filename}: {missing_columns}")
            raise KeyError(
                f"Missing required column(s): {missing_columns}. "
                f"Expected columns include: {expected_columns}. "
                f"Available columns: {list(data.columns)}"
            )

        data = data.rename(columns={filename_base: 'energy_value'})
        data['filename'] = filename_base

        if data.empty:
            raise ValueError(
                f"Error processing '{filename}'. No rows found after processing. "
                f"Verify that the input file contains valid data."
            )
        logger.debug(f"Columns after checker {data.columns}")
        return data

    def _validate_databases(self) -> None:
        """
        if specify database is activated, check that the basefile has non NaN values
        """
        logger.debug(f"specify_database has been set as {self.specify_database}")
        logger.info(f"specify_database has been set as True. Checking the database column...")

        if 'database' not in self.basefile.columns:
            logger.error("-database- column not found in the basefile")
            raise KeyError(f"Please, specify the database column in the basefile")

        if self.basefile['database'].isnull().any():
            logger.error("The column database from the basefile containse missing values")
            raise ValueError(f"The column database from the basefile containse missing values")



    def _preprocess_basefile(self):

        self._validate_basefile()

        self.basefile = pd.read_excel(self.mother_file, sheet_name='Processors').dropna(
            subset=['Ecoinvent_key_code'])

        self._verify_national()

        self.basefile['alias_carrier'] = (self.basefile['Processor']
                                          + '_' + self.basefile['@SimulationCarrier'])

        # tech_carrier_{file_name}
        self.basefile['alias_filename_base'] = (self.basefile['alias_carrier']
                                                + '__' +
                                                self.basefile['File_source'].astype(str).str.split('.').str[0])

        self.basefile['alias_filename_loc'] = self.basefile['alias_filename_base'] + '___' + self.basefile[
            'Region'].astype(str)  # alias_filename_base + COUNTRY (no subregions)

        self.basefile['full_alias'] = self.basefile['alias_filename_loc'] + '-' + self.basefile['geo_loc'].astype(
            str) # alias-filename-country+geoloc


    def _preprocess_calliope(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Previous to filtering the data, preprocess the calliope data:
            -Add new columns for the filters
        """
        df_names = df.copy()
        # Filter Processors from calliope data
        df_names['alias_carrier'] = df_names['techs'] + '_' + df_names['carriers'] # tech + carrier
        df_names['alias_filename_base'] = df_names['alias_carrier'] + '__' + df_names['filename'] # tech_carrier + file

        # create the country column
        df_names = self._manage_regions(df_names) # split countries (e.g [ESP]) from locs (ESP_1)
        df_names['alias_filename_loc'] = df_names['alias_filename_base'] + '___' + df_names['countries']

        if self.national:
            df_names['full_name'] = df_names['alias_filename_loc']
        else:  # subnational
            df_names['full_name'] = df_names['alias_filename_base'] + '___' + df_names['locs']

        return df_names


    def _filter_techs(self, df: pd.DataFrame,
                      filter: str) -> pd.DataFrame:
        """
        Filter the input data based on technologies defined in the basefile
        * filter: File_source to filter the data
        """
        logger.info("Starting filter techs")
        df_names = self._preprocess_calliope(df) # create new columns aggregating others.

        if self._edited is False:  # working with basefile data
            if self.specify_database:  # check that the database column is there and doesn't contain empty values
                self._validate_databases()

            self._preprocess_basefile() # Create new columns and aliases
            self._edited = True

        basefile = self.basefile.loc[self.basefile['File_source'] == filter] # FILTERS BY FILE SOURCE FIRST.
        calliope = df_names[df_names['filename'] == str(filter).split('.')[0]]

        excluded_techs = set(calliope['alias_carrier']) - set(basefile['alias_carrier'])

        self.techs_region_not_included.extend(excluded_techs)
        calliope = calliope[~calliope['alias_carrier'].isin(excluded_techs)]  # exclude the technologies

        logger.debug(f"Filtering technologies for {filter}")
        logger.debug(f"Excluded techs: {excluded_techs}")
        logger.debug(f"Data shape before filtering: {df_names.shape}")
        logger.debug(f"Data shape after filtering: {calliope.shape}")

        return calliope



    def _group_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Group the energy data based on technologies defined in the basefile
        If national= True, it aggregates by country
        """
        logger.info(f"Grouping Energy System data according to the Basefile...")

        # combine spores + additional columns into a single string key
        if self.additional_columns:
            df['spores'] = df['spores'].astype(str)
            for col in self.additional_columns:
                if col in df.columns:
                    df['spores'] += '_' + df[col].astype(str)
        else:
            df['spores'] = df['spores'].astype(str)

            # Ensure alias_filename_base and alias_filename_loc exist
            if 'alias_filename_base' not in df.columns:
                df['alias_carrier'] = df['techs'].astype(str) + '_' + df['carriers'].astype(str)
                df['alias_filename_base'] = df['alias_carrier'] + '__' + df['filename'].astype(str).str.split('.').str[
                    0]
                df = self._manage_regions(df)
                df['alias_filename_loc'] = df['alias_filename_base'] + '___' + df['countries']

            if self.national: # TODO: seems redundant, as it could be done with country
                # clean locs -> country part
                df['locs'] = df['locs'].astype(str).str.split('_').str[0].str.split('-').str[0]
                grouped_df = df.groupby(['alias_filename_base', 'locs'], as_index=False).agg({
                    'energy_value': 'sum',
                    'spores': 'first',
                    'techs': 'first',
                    'carriers': 'first',
                })
                # canonical full_name is base alias (no region suffix) in national mode
                grouped_df['full_name'] = grouped_df['alias_filename_base']
            else:
                # subnational: group by detailed alias including country
                group_cols = [
                    'spores',
                    'techs',
                    'carriers',
                    'locs',
                    'alias_carrier',
                    'alias_filename_loc',
                    'full_name' if 'full_name' in df.columns else 'alias_filename_loc'
                ]
                # Ensure the column used exists
                group_cols = [c for c in group_cols if c in df.columns]
                grouped_df = df.groupby(group_cols, as_index=False).agg({'energy_value': 'sum'})

            logger.info("Data grouped completed")
            logger.debug(f"Grouped df columns: {grouped_df.columns}")
            logger.debug(f"Grouped df shape: {grouped_df.shape}")
            logger.debug(f"Grouped df head:\n{grouped_df.head(3)}")
        return grouped_df

    @staticmethod
    def _manage_regions(df: pd.DataFrame) -> pd.DataFrame:
        """
        Edit the regions format strings in order to get general country names
        """
        df['locs'] = df['locs'].astype(str)
        df['countries'] = df['locs'].str.split('_').str[0].str.split('-').str[0].str.strip()
        logger.debug(f"Countries generated {df['countries'].unique()[:10]}")
        return df

    def preprocess_data(self) -> pd.DataFrame:
        """Run data preprocessing steps"""
        logger.info("Starting preprocessing data")

        self.basefile = pd.read_excel(self.mother_file, sheet_name='Processors')
        all_data = self.create_template_df()
        grouped = self.basefile.groupby('File_source')

        for data_source, group in grouped:
            if pd.isna(data_source):
                warnings.warn(f"DataSource is missing for some entries. Skipping these entries.", Warning)
                continue
            try:

                raw_data = self._load_data(data_source)  # Calliope data

                checked_data = self._input_checker(data=raw_data, filename=data_source)  # calliope data

                filtered_data = self._filter_techs(checked_data,
                                                   data_source)  # calliope data

                logger.debug("Adding filtered data to the template...")
                all_data = pd.concat([all_data, filtered_data], ignore_index=True)

            except ValueError as e:
                raise

            except Exception as e:
                logger.exception(f"Error processing {data_source}: {e}")
                warnings.warn(f"Error processing {data_source}: {e}", Warning)

        if len(self.techs_region_not_included) > 1:
            # Find the FileHandler path
            log_file_path = next(
                (h.baseFilename for h in logger.handlers if isinstance(h, logging.FileHandler)),
                "log file not found"
            )

            message = (
                "\nThere are technologies present in the energy data but not in the Basefile.\n"
                f"Please check the log file for details: {log_file_path} and see -Excluded techs-\n"
            )
            warnings.warn(message, Warning)
            logger.warning(message)

        self.final_df = self._group_data(all_data)  # calliope data
        #self.final_df = self._manage_regions(self.final_df) # seems redundant

        logger.info("Data preprocessing finished")
        return self.final_df

    ######################
    # Unit adapter
    ######################

    def _extract_data(self) -> List['BaseFileActivity']:
        """
        extract activities from the basefile and create a list BasFileActivity instances
        """
        logger.info("Extracting LCA activities...")

        def _create_activity(row):
            # move the activities from the basefile into a DataBase dataclass
            try:
                kwargs = {
                    'name': row['Processor'],
                    'alias_carrier': row['alias_carrier'],
                    'carrier': row['@SimulationCarrier'],
                    'parent': row['ParentProcessor'],
                    'region': row['Region'],
                    'code': row['Ecoinvent_key_code'],
                    'factor': row['@SimulationToEcoinventFactor'],
                    'full_alias': row['full_alias'],
                    'alias_filename_loc': row['alias_filename_loc'], # without subregion!,
                    'geo_loc': row['geo_loc'],
                    'national': self.national
                }

                if self.specify_database:
                    kwargs['database'] = row['database']

                return BaseFileActivity(**kwargs)

            except KeyError as e:
                logger.warning(f"Warning during data extraction {e}")
                return None

        base_activities = self.basefile.progress_apply(_create_activity, axis=1).dropna().tolist()
        logger.info(f"{len(base_activities)} activities extracted")
        logger.debug(f"First 3 extracted activities: {base_activities[:3]}")
        return [activity for activity in base_activities if getattr(activity, 'unit', None) is not None]

    def _adapt_units(self):
        """adapt the units (flow_out_sum * conversion factor)"""
        logger.info("Adapting units...")

        self.base_activities = self._extract_data()
        logger.debug(f"Converting {len(self.base_activities)} activities into a DataFrame")

        rows = []
        for activity in self.base_activities:
            d = asdict(activity)  # serialize dataclass fields
            d["full_name"] = activity.full_name
            rows.append(d)

        df = pd.DataFrame(rows)  # basefile
        logger.debug(f"Base activities DataFrame shape: {df.shape}")

        if df.empty:
            logger.error("No base activity found.")
            raise RuntimeError("No base activity found for unit adaptaiton.")

        self.final_df['full_name'] = self.final_df['full_name'].astype(str).str.strip()
        df['full_name'] = df['full_name'].astype(str).str.strip()

        merged = pd.merge(self.final_df,
                          df,
                          on='alias_filename_loc',
                          how='right',
                          indicator=True,
                          suffixes=('_energy', '_act'))

        logger.debug(f"After merge: {merged.shape}")
        logger.info("Merge counts: matched=%d, energy-only=%d, basefile-only=%d",
                    (merged['_merge'] == 'both').sum(),
                    (merged['_merge'] == 'left_only').sum(),
                    (merged['_merge'] == 'right_only').sum())

        merged['energy_value'] = pd.to_numeric(merged.get('energy_value', 0), errors='coerce').fillna(0.0)

        # factor required
        if 'factor' not in merged.columns:
            logger.error("Missing 'factor' column after merge; cannot compute new_vals.")
            raise KeyError("Missing 'factor' column in merged data")
        merged['factor'] = pd.to_numeric(merged['factor'], errors='coerce')

        # Drop rows missing required calculation fields (factor or unit)
        before = len(merged)
        merged = merged.dropna(subset=['factor', 'unit'])
        dropped_required = before - len(merged)
        if dropped_required:
            logger.warning("Dropped %d rows missing required fields (factor/unit) after merge", dropped_required)

        # Compute new values
        merged['new_vals'] = merged['factor'] * merged['energy_value']
        merged['new_units'] = merged['unit']

        # Cleanup and forward
        merged = merged.drop(columns=['_merge'], errors='ignore')
        logger.debug("Computed new_vals for %d rows", len(merged))

        if not self.national:
            #merged = self._fix_fullname(merged)  # extend full_name with subregion if subnational
            #logger.debug("Extended full_name with subregion for subnational data")
            pass # TODO: remove from clean versions
        return self._final_dataframe(merged)

    @staticmethod
    def _fix_fullname(df: pd.DataFrame) -> pd.DataFrame:
        """
        If national is True, extend the full_name with the subregion
        """
        locs = df['locs'].str.split("_").str[1]
        df['full_name'] = df['full_name'] + '_' + locs
        return df

    @staticmethod
    def _check_unique_full_names(df: pd.DataFrame) -> None:
        """
        Check if the full_name column is unique. If not, raise a warning
        """


        duplicates = df.loc[df['full_name'].duplicated(keep=False), 'full_name']
        if not duplicates.empty:
            unique_dupes = sorted(set(duplicates.tolist()))
            logger.warning(
                f"The 'full_name' column is not unique. Found {len(unique_dupes)} duplicates: {unique_dupes}"
            )

    @staticmethod
    def _check_str_values(df: pd.DataFrame,
                          column: str,
                          cast_to_int: bool = False):
        """
        if ';' passed, issues may rise. This function checks a particular column that could potentially be a string
        instead of float
        """
        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found in DataFrame.")

        if not is_numeric_dtype(df[column]):
            logger.debug(f"column {column} is not numeric. Fixing it...")

            df[column] = (
                df[column]
                .astype(str)
                .str.replace(",", ".", regex=False)
                .str.replace(" ", "", regex=False)
                .str.strip()
            )
            df[column] = pd.to_numeric(df[column], errors="coerce")

        if cast_to_int:
            df[column] = df[column].astype("Int64")  # Nullable integer type for missing values
        return df


    def _final_dataframe(self, df):
        """
            Prepare and validate the final cleaned DataFrame.
            Assumes `full_name` already exists and is the canonical join key.
            """
        cols = [
            'spores',
            'locs',
            'techs',
            'full_name_energy',
            'code',
            "full_alias",
            "parent",
            "geo_loc",
            'carriers',
            'new_vals',
            'new_units'
        ]

        # Drop rows that are entirely empty, but avoid blanket dropna that removes useful rows
        df = df.dropna(axis=0, how='all')

        # hardfix #21 --> use the merged data to combine full_energy_name and full_alias


        # Verify required columns exist
        missing_cols = [c for c in cols if c not in df.columns]
        if missing_cols:
            logger.error("Missing columns in final dataframe: %s", missing_cols)
            raise KeyError(f"Missing columns in final dataframe: {missing_cols}")

        df = df[cols]

        # rename internal column names to final consumer names
        df = df.rename(columns={'new_vals': 'energy_value', 'new_units': 'unit', 'spores': 'scenarios'})

        # build aliases column WHY??? #TODO: REMOVE
        df['aliases'] = df['techs'].astype(str) + '__' + df['carriers'].astype(str) + '___' + df['locs'].astype(str)

        df["full_name"] = df["full_name_energy"] + '-' + df['geo_loc']
        self._techs_sublocations = df['full_name'].unique().tolist() # TODO: REMOVE
        self._check_unique_full_names(df)

        logger.info("Final preprocess DataFrame ready with shape %s", df.shape)
        logger.debug("Final DataFrame columns: %s", list(df.columns))

        return df

    def adapt_units(self):
        """Public method to adapt the units"""
        return self._adapt_units()











































