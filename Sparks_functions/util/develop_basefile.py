"""
@LexPascal
"""
import pandas as pd
import bw2data as bd
from dataclasses import dataclass, field, InitVar
from typing import Union, Optional, List, Tuple, NamedTuple, Dict, Any
import warnings
from bw2data.errors import UnknownObject
from bw2data.backends import Activity, ActivityDataset

"""
@LexPascal
Refactored BaseAct and Support classes with explicit schemas and no tuple-indexing.
"""
import pandas as pd
import bw2data as bd
from dataclasses import dataclass, field, InitVar
from typing import List, NamedTuple, Dict, Any
import warnings
from bw2data.backends import Activity, ActivityDataset

# ISO3 → ISO2 for ecoinvent regions
LOCATION_EQUIVALENCE = {
    'ALB': 'AL', 'FRA': 'FR', 'SWE': 'SE', 'DNK': 'DK', 'POL': 'PL', 'IRL': 'IE',
    'EST': 'EE', 'HRV': 'HR', 'PRT': 'PT', 'BIH': 'BA', 'LVA': 'LV', 'SVN': 'SI',
    'AUT': 'AT', 'GBR': 'GB', 'DEU': 'DE', 'MNE': 'ME', 'NOR': 'NO', 'BGR': 'BG',
    'NLD': 'NL', 'HUN': 'HU', 'BEL': 'BE', 'CHE': 'CH', 'CZE': 'CZ', 'ROU': 'RO',
    'CYP': 'CY', 'ESP': 'ES', 'GRC': 'GR', 'MKD': 'MK', 'ISL': 'IS', 'ITA': 'IT',
    'LTU': 'LT', 'FIN': 'FI', 'SVK': 'SK', 'SRB': 'RS', 'LUX': 'LU'
}


class Combination(NamedTuple):
    """One row from the Calliope CSV: tech name + calliope loc."""
    tech_name: str
    calliope_loc: str


class EActivity(NamedTuple):
    """One loaded Ecoinvent activity: name + loc_equiv + code."""
    name: str
    loc_equiv: str
    code: str


@dataclass
class BaseAct:
    """Base Activity Definition for one calliope technology."""
    name: str
    ecoinvent_name: str
    file_name: str
    factor: float
    ecoinvent_location: str
    ecoinvent_unit: str
    database: str
    calliope_units: str
    locations: List[Combination]
    sheet_name: str
    regional: str
    carrier: str

    activities: List[EActivity] = field(default_factory=list)
    basefile_activities: List[Dict[str, Any]] = field(default_factory=list)
    init_post: InitVar[bool] = True
    generic_location: bool = False

    def __post_init__(self, init_post):
        # Load one or multiple Ecoinvent activities
        if self.ecoinvent_location != 'country':
            self.generic_location = True
            triplet = self._load_activity(
                name=self.ecoinvent_name,
                database=self.database,
                location=self.ecoinvent_location
            )
            self.activities.append(EActivity(*triplet))
        else:
            for triplet in self._load_multi_activity(
                name=self.ecoinvent_name,
                database=self.database,
                locations=self.locations
            ):
                self.activities.append(EActivity(*triplet))

        # Build the basefile_activities list
        self._combine_activities_and_locs()

    def _load_activity(self, name: str, database: str, location: str) -> tuple:
        """Load a single activity; return (name, loc_equiv, code)."""
        query = list(ActivityDataset.select().where(
            (ActivityDataset.name == name) &
            (ActivityDataset.database == database)
        ))
        if query:
            all_acts = [Activity(r) for r in query]
            pass
            matches = [a for a in all_acts if a['location'] == location]
            if not matches:
                fallback = [a for a in all_acts if a['location'] in ('CH', 'FR', 'DE')]
                if fallback:
                    a = fallback[0]
                    return name, a['location'], a['code']
                code = 'DB Undefined' if self.generic_location else 'unknown location'
                return name, location, code
            a = matches[0]
            return name, a['location'], a['code']
        else:
            warnings.warn(f"No activity named '{name}' in DB", UserWarning)
            return name, location, 'DB Undefined'

    def _load_multi_activity(
        self, name: str, database: str, locations: List[Combination]
    ) -> List[tuple]:
        """Load activities for each calliope location; returns list of (n,loc,code)."""
        out = []
        for combo in locations:
            iso2 = LOCATION_EQUIVALENCE.get(combo.calliope_loc, combo.calliope_loc)
            out.append(self._load_activity(name, database, iso2))
        return out

    def _combine_activities_and_locs(self):
        """
        Populate self.basefile_activities with dicts:
          {
            'tech_name':        str,
            'ecoinvent_region': str,
            'ecoinvent_code':   str,
            'geo_scope':        str,
            'carrier':          str,
            'database':         str,
          }
        """
        def check_code(code, name):
            if not isinstance(code, str) or ' ' in code:
                warnings.warn(f"Suspect ecoinvent code '{code}' for '{name}'", UserWarning)

        if self.generic_location:
            # One Ecoinvent activity for all tech locs
            ea = self.activities[0]
            check_code(ea.code, ea.name)
            for combo in self.locations:
                self.basefile_activities.append({
                    'tech_name'       : combo.tech_name,
                    'location'        : combo[1],
                    'ecoinvent_region': ea.loc_equiv,
                    'ecoinvent_code'  : ea.code,
                    'geo_scope'       : self.regional,
                    'carrier'         : self.carrier,
                    'database'        : self.database,
                })
        else:
            # Match each EActivity to calliope locs
            grouped: Dict[str, List[str]] = {}
            for combo in self.locations:
                grouped.setdefault(combo.calliope_loc, []).append(combo.tech_name)

            for ea in self.activities:
                check_code(ea.code, ea.name)
                if ea.loc_equiv in grouped:
                    for tech in grouped[ea.loc_equiv]:
                        self.basefile_activities.append({
                            'tech_name'       : tech,
                            'ecoinvent_region': ea.loc_equiv,
                            'ecoinvent_code'  : ea.code,
                            'geo_scope'       : self.regional,
                            'carrier'         : self.carrier,
                            'database'        : self.database,
                        })


class Support:
    """Orchestrates reading calliope CSVs, building BaseAct, and saving to Excel."""
    def __init__(self, file: str, project: str, calliope: Dict[str, str]):
        self.file = file
        self.project = project
        self.calliope = calliope
        self.sheet_name = None

        bd.projects.set_current(self.project)
        self.df = self._create_empty_dataframe()

    def run(self):
        self.om = pd.read_excel(self.file, sheet_name='o&m')
        self.infrastructure = pd.read_excel(self.file, sheet_name='infrastructure')
        self._iter_sheet(self.infrastructure, 'infrastructure')
        self._iter_sheet(self.om, 'o&m')

    def _iter_sheet(self, df: pd.DataFrame, sheet_name: str):
        self.sheet_name = sheet_name
        activities: List[BaseAct] = []
        for _, row in df.iterrows():
            combos = self._extract_combinations(
                row['technology_name_calliope'],
                row['calliope_file'],
            )
            activities.append(BaseAct(
                name=row['technology_name_calliope'],
                ecoinvent_name=row['life_cycle_inventory_name'],
                file_name=row['calliope_file'],
                factor=row['prod_scaling_factor'],
                ecoinvent_location=row['prod_location'],
                ecoinvent_unit=row['prod_unit'],
                database=row['prod_database'],
                calliope_units=row['calliope_prod_unit'],
                locations=combos,
                sheet_name=sheet_name,
                regional=row['geographical_scope'],
                carrier=row['carrier'],
            ))
        self._store_excel(activities)

    def _store_excel(self, activities: List[BaseAct]):
        new_rows = []
        for act in activities:
            for entry in act.basefile_activities:
                new_rows.append({
                    'Processor'                   : entry['tech_name'],
                    'Region'                      : entry['location'],
                    '@SimulationCarrier'          : entry['carrier'],
                    'ParentProcessor'             : 'Unknown',
                    '@SimulationToEcoinventFactor': act.factor,
                    'Ecoinvent_key_code'          : entry['ecoinvent_code'],
                    'File_source'                 : act.file_name + '.csv',
                    'activity_name_passed'        : act.name,
                    'location_passed'             : entry['ecoinvent_region'],
                    'geo_loc'                     : entry['geo_scope'],
                    'database'                    : entry['database'],
                })
        new_df = pd.DataFrame(new_rows)
        self.df = pd.concat([self.df, new_df], ignore_index=True)

    def _extract_combinations(self, name: str, file: str) -> List[Combination]:
        """
        Return unique (tech_name, loc) combos from the Calliope CSV.
        Tries both `file` and `file + '.csv'` as keys in self.calliope,
        and only appends '.csv' if the path doesn’t already end with it.
        """
        if self.sheet_name == 'o&m':
            name = name.split(',')[0]

        # 1) Find the correct key in self.calliope
        if file in self.calliope:
            file_key = file
        elif f"{file}.csv" in self.calliope:
            file_key = f"{file}.csv"
        else:
            raise KeyError(
                f"Calliope key for '{file}' not found; available: {list(self.calliope.keys())}"
            )

        # 2) Get the mapped path
        raw_path = self.calliope[file_key]
        # If it already endswith .csv, use as‐is; otherwise append
        csv_path = raw_path if raw_path.lower().endswith('.csv') else f"{raw_path}.csv"

        # 3) Cache the DataFrame on self under a clean attribute name
        attr = file_key.replace('.csv', '')
        if not hasattr(self, attr):
            df = pd.read_csv(csv_path, sep=None, engine='python') # Hardfix #8
            self._check_columns(df, csv_path)  # Validate required columns
            setattr(self, attr, df)
            #setattr(self, attr, pd.read_csv(csv_path))

        df = getattr(self, attr)

        rows = df.loc[df['techs'] == name, ['techs', 'locs']]

        # 4) Build and dedupe Combination objects
        combos = [Combination(tech, loc)
                  for tech, loc in rows.itertuples(index=False, name=None)]
        seen = set()
        unique = []
        for combo in combos:
            if combo not in seen:
                seen.add(combo)
                unique.append(combo)

        return unique

    @staticmethod
    def _create_empty_dataframe() -> pd.DataFrame:
        cols = [
            'Processor', 'Region', '@SimulationCarrier', 'ParentProcessor',
            '@SimulationToEcoinventFactor', 'Ecoinvent_key_code', 'File_source',
            'activity_name_passed', 'location_passed', 'database'
        ]
        return pd.DataFrame(columns=cols)

    @staticmethod
    def _check_columns(data: pd.DataFrame, passing_file: str):
        """
        Validates that the required columns are present in the input DataFrame.

        Parameters:
        -----------
        data : pd.DataFrame
            The input DataFrame to validate.
        passing_file : str
            The name or path of the file being validated, used for error messages.
        """
        required_column = 'techs'
        if required_column not in data.columns:
            raise TypeError(f"Required column '{required_column}' is missing in file: {passing_file}")

        if 'locs' not in data.columns:
            if 'nodes' in data.columns:
                data.rename(columns={'nodes': 'locs'}, inplace=True)
            else:
                raise TypeError(f"Required column 'locs' is missing in file: {passing_file} "
                                f"and could not be inferred from a 'nodes' column.")

        """
        
        # Add 'spores' column if it does not exist
        if 'spores' not in data.columns:
            data['spores'] = 0
        if 'Unnamed: 0' in data.columns:
            data = data.drop('Unnamed: 0', axis=1)

        if 'carriers' not in data.columns:
            data['carriers'] = 'default_carrier'
        """





