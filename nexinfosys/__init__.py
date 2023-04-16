import configparser
import importlib
import logging
import traceback
from enum import Enum
import os
import regex as re
from typing import Optional, Any, List, Tuple, Callable, Dict, Union, Type

import pint
from collections import namedtuple
from attr import attrs, attrib

# GLOBAL VARIABLES
cfg_file_env_var = "MAGIC_NIS_SERVICE_CONFIG_FILE"
engine = None

# Database containing OLAP data (cache of Data Cubes)
data_engine = None

# Data source manager
data_source_manager = None  # type: DataSourceManager

# REDIS
redis = None

# Use AdHoc datasets source (datasets in memory as a datasource, for DatasetQry)
enable_adhoc_datasets_source = True

# Case sensitive
case_sensitive = False

# Create units registry
ureg = pint.UnitRegistry()
ureg.define("cubic_meter = m^3 = m3")
ureg.define("square_meter = m^2 = m2")
ureg.define("euro = [] = EUR = Eur = eur = Euro = Euros = €")
ureg.define("dollar = [] = USD = Usd = usd = Dollar = Dollars = $")
ureg.define("capita = []")
ureg.define("dimensionless = [] = adimensional")
ureg.define('fraction = [] = frac')
ureg.define('percent = 1e-2 frac = pct')

# Named tuples
Issue = namedtuple("Issue",
                   "sheet_number sheet_name c_type type message")

SDMXConcept = namedtuple('Concept', 'type name istime description code_list')

# Global Types

IssuesOutputPairType = Tuple[List[Issue], Optional[Any]]
ExecutableCommandIssuesPairType = Tuple[Optional["IExecutableCommand"], List[Issue]]
IssuesLabelContentTripleType = Tuple[List[Issue], Optional[Any], Optional[Dict[str, Any]]]
# Tuple (top, bottom, left, right) representing the rectangular area of the input worksheet where the command is present
AreaTupleType = Tuple[int, int, int, int]

# ##################################
# METADATA special variables

# Simple DC fields not covered:
#  type (controlled),
#  format (controlled),
#  rights (controlled),
#  publisher,
#  contributor,
#  relation
#
# XML Dublin Core: http://www.dublincore.org/documents/dc-xml-guidelines/
# Exhaustive list: http://dublincore.org/documents/dcmi-type-vocabulary/

# Fields: ("<field label in Spreadsheet file>", "<field name in Dublin Core>", Mandatory?, Controlled?, NameInJSON)
metadata_fields = [("Case study name", "title", False, False, "case_study_name"),
                   ("Case study code", "title", True, False, "case_study_code"),
                   ("Title", "title", True, False, "title"),
                   ("Subject, topic and/or keywords", "subject", False, True, "subject_topic_keywords"),
                   ("Description", "description", False, False, "description"),
                   ("Geographical level", "description", True, True, "geographical_level"),
                   ("Dimensions", "subject", True, True, "dimensions"),
                   ("Reference documentation", "source", False, False, "reference_documentation"),
                   ("Authors", "creator", True, False, "authors"),
                   ("Date of elaboration", "date", True, False, "date_of_elaboration"),
                   ("Temporal situation", "coverage", True, False, "temporal_situation"),
                   ("Geographical location", "coverage", True, False, "geographical_situation"),
                   ("DOI", "identifier", False, False, "doi"),
                   ("Language", "language", True, True, "language"),
                   ("Restriction level", None, True, True, "restriction_level"),
                   ("Version", None, True, False, "version")
                   ]

# Regular expression definitions
regex_var_name = "([a-zA-Z][a-zA-Z0-9_-]*)"
regex_hvar_name = "(" + regex_var_name + r"(\." + regex_var_name + ")*)"
regex_cplex_var = "((" + regex_var_name + "::)?" + regex_hvar_name + ")"
regex_optional_alphanumeric = "([ a-zA-Z0-9_-]*)?"  # Whitespace also allowed


# Regular expression for "worksheet name" in version 2
def simple_regex(names: List[str]):
    return r"(" + "|".join(names) + ")" + regex_optional_alphanumeric


# #####################################################################################################################
# >>>> CONFIGURATION FILE AND DIRECTORIES <<<<
# #####################################################################################################################

def prepare_default_configuration(create_directories):
    def default_directories(path, tmp_path):
        return {
            "CASE_STUDIES_DIR": f"{path}{os.sep}data{os.sep}cs{os.sep}",
            "FAO_DATASETS_DIR": f"{path}{os.sep}data{os.sep}faostat{os.sep}",
            "FADN_FILES_LOCATION": f"{path}{os.sep}data{os.sep}fadn",
            "CACHE_FILE_LOCATION": f"{tmp_path}{os.sep}sdmx_datasets_cache",
            "REDIS_HOST_FILESYSTEM_DIR": f"{tmp_path}{os.sep}sessions",
            "SSP_FILES_DIR": "",
        }

    from appdirs import AppDirs
    app_dirs = AppDirs("nis-backend")

    # Default directories, multi-platform
    data_path = app_dirs.user_data_dir
    cache_path = app_dirs.user_cache_dir
    if create_directories:
        logging.debug(f"Creating directories {data_path} and {cache_path}")
        os.makedirs(data_path, exist_ok=True)
        os.makedirs(cache_path, exist_ok=True)

    # Obtain and create directories
    dirs = default_directories(data_path, cache_path)
    for v in dirs.values():
        if v:
            if create_directories:
                logging.debug(f"Creating directory {v}")
                os.makedirs(v, exist_ok=True)

    # Default configuration
    return f"""{os.linesep.join([f'{k}="{v}"' for k, v in dirs.items()])}
DB_CONNECTION_STRING="sqlite:///{data_path}{os.sep}nis_metadata.db"
DATA_CONNECTION_STRING="sqlite:///{data_path}{os.sep}nis_cached_datasets.db"
SHOW_SQL=True
# Flask Session (server side session)
REDIS_HOST="filesystem:local_session"
TESTING="True"
ENABLE_CYTHON_OPTIMIZATIONS="True"
SELF_SCHEMA=""
FS_TYPE="WebDAV"
FS_SERVER=""
FS_USER=""
FS_PASSWORD=""
# Google Drive API
GAPI_CREDENTIALS_FILE="{data_path}/credentials.json"
GAPI_TOKEN_FILE="{data_path}/token.pickle"    
""", data_path + os.sep + "nis_local.conf"

# Global configuration variables
global_configuration = None


def initialize_configuration():
    try:
        _, file_name = prepare_default_configuration(False)
        logging.debug(f"Default configuration file name: {file_name}")
        if not os.environ.get(cfg_file_env_var):
            logging.debug(f"{cfg_file_env_var} not defined, trying default configuration file location ({file_name}")
            found = False
            for f in [file_name]:  # f"{nexinfosys.__path__[0]}/restful_service/nis_local_dist.conf"
                if os.path.isfile(f):
                    logging.debug(
                        f"Default configuration file {file_name} found, setting {cfg_file_env_var} to that file")
                    found = True
                    os.environ[cfg_file_env_var] = f
                    break
            if not found:
                logging.debug(f"Default configuration file {file_name} NOT found, will try to generate a default in that location.")
                cfg, file_name = prepare_default_configuration(True)
                logging.debug(f"Generating {file_name} as configuration file with content:\n{cfg}")
                with open(file_name, "wt") as f:
                    f.write(cfg)
                logging.debug(
                    f"Default configuration file {file_name} generated! Setting {cfg_file_env_var} to that file")
                os.environ[cfg_file_env_var] = file_name
        else:
            logging.debug(f"{cfg_file_env_var}: {os.environ[cfg_file_env_var]}")

    except Exception as e:
        print(f"{cfg_file_env_var} environment variable not defined!")
        print(e)
        traceback.print_exc()


def get_global_configuration():
    def read_configuration() -> Dict[str, str]:
        """
        If environment variable "MAGIC_NIS_SERVICE_CONFIG_FILE" is defined, and the contents is the name of an existing file,
        read it as a configuration file and return the result

        :return:
        """
        if os.environ.get(cfg_file_env_var):
            fname = os.environ[cfg_file_env_var]
            if os.path.exists(fname):
                with open(fname, 'r') as f:
                    config_string = '[asection]\n' + f.read()
                config = configparser.ConfigParser()
                config.read_string(config_string)
                return {k: expand_paths(k, remove_quotes(v)) for k, v in config.items("asection")}
        else:
            return {}

    global global_configuration
    if global_configuration is None:
        global_configuration = read_configuration()
    return global_configuration


def get_global_configuration_variable(key: str, default: str = None) -> str:
    global global_configuration
    get_global_configuration()
    return global_configuration.get(key.lower(), default)


def set_global_configuration_variable(key: str, value: str):
    """
    Use carefully. The initial intention is to set the "ENABLE_CYTHON_OPTIMIZATIONS" variable to False

    :param key:
    :param value:
    :return:
    """
    global global_configuration
    if global_configuration:
        global_configuration[key.lower()] = value


def remove_quotes(s: str) -> str:
    return s.lstrip('\'"').rstrip('\'"')


def expand_paths(key: str, value: str) -> str:
    if key.endswith('_dir') or key.endswith('_location') or key.endswith('_file'):
        return os.path.expanduser(value)
    else:
        return value

# ##################################
# Commands


class CommandType(Enum):
    input = (1, "Input")
    core = (2, "Core")
    convenience = (3, "Convenience")
    metadata = (4, "Metadata")
    analysis = (5, "Analysis")
    misc = (99, "Miscellaneous")

    def __str__(self):
        return self.value[1]

    @classmethod
    def from_string(cls, s):
        for ct in cls:
            if ct.value[1] == s:
                return ct
        raise ValueError(cls.__name__ + ' has no value matching "' + s + '"')


@attrs(cmp=False)  # Constant and Hashable by id
class Command:
    # Name, the lowercase unique name
    name = attrib()  # type: str
    # Allowed names for the worksheet. Used for simple regular expressions.
    allowed_names = attrib()  # type: List[str]
    # Name of the subclass of IExecutableCommand in charge of the execution
    execution_class_name = attrib()  # type: Optional[str]
    # Command type
    cmd_type = attrib()  # type: CommandType
    # Direct examples
    direct_examples = attrib(default=[])  # type: Optional[List[str]]
    # URLs of files where it is used
    files = attrib(default=[])  # type: Optional[List[str]]
    # Alternative regular expression for worksheet name, otherwise the simple_regex() is used
    alt_regex = attrib(default=None)
    # Parse function, having params (Worksheet, Area) and returning a tuple (issues, label, content)
    # Callable[[Worksheet, AreaTupleType, str, ...], IssuesLabelContentTripleType] = attrib(default=None)
    parse_function: Callable[..., IssuesLabelContentTripleType] = attrib(default=None)
    # In which version is this command allowed?
    is_v1 = attrib(default=False)  # type: bool
    is_v2 = attrib(default=False)  # type: bool

    @property
    def regex(self):
        if self.alt_regex:
            pattern = self.alt_regex
        else:
            pattern = simple_regex(self.allowed_names)

        return re.compile("^"+pattern, flags=re.IGNORECASE)

    @property
    def execution_class(self):
        if self.execution_class_name:
            module_name, class_name = self.execution_class_name.rsplit(".", 1)
            return getattr(importlib.import_module(module_name), class_name)
        else:
            return None


@attrs(cmp=False)  # Constant and Hashable by id
class CommandField:
    # Allowed names for the column
    allowed_names = attrib()  # type: List[str]
    # Internal name used during the parsing
    name = attrib()  # type: str
    # Parser for the column
    parser = attrib()
    # Flag indicating if the column is mandatory or optional. It can also be an expression (string).
    mandatory = attrib(default=False)  # type: Union[bool, str]
    # A default value for the field
    default_value = attrib(default=None)
    # Some columns have a predefined set of allowed strings
    allowed_values = attrib(default=None)  # type: Optional[list[str]]
    # Many values or just one
    many_values = attrib(default=True)
    # Many appearances (the field can appear multiple times). A convenience to define a list
    many_appearances = attrib(default=False)
    # Examples
    examples = attrib(default=None)  # type: List[str]
    # Description text
    description = attrib(default=None)  # type: str
    # Compiled regex
    # regex_allowed_names = attrib(default=None)
    # Is it directly an attribute of a Musiasem type? Which one?
    attribute_of = attrib(default=None)  # type: Type
    deprecated = attrib(default=False)  # type: bool

    @property
    def regex_allowed_names(self):
        def contains_any(s, setc):
            return 1 in [c in s for c in setc]

        # Compile the regular expressions of column names
        rep = [(r if contains_any(r, ".+") else re.escape(r))+"$" for r in self.allowed_names]

        return re.compile("|".join(rep), flags=re.IGNORECASE)


# Embedded commands, assumed to be present in every submitted workbook
default_cmds = """
[
    {
      "command": "cat_hierarchies",
      "label": "Hierarchies by default",
      "content":
        {
            "command_name": "Hierarchies",
            "items": [
            {
                "_complex": false,
                "_expandable": [],
                "_row": 2,
                "code": "FlowGraph",
                "description": "Flow graph algorithm for the solving",
                "hierarchy_name": "SolvingAlgorithms",
                "label": "Solve graph converting the MuSIASEM into overlapping flow graphs"
            },
            {
                "_complex": false,
                "_expandable": [],
                "_row": 2,
                "code": "TakeUpper",
                "description": "Policy: take the value upper instead of the value coming from lower level accumulation",
                "hierarchy_name": "AggregationConflictResolutionPolicies",
                "label": "Take value from the upper level"
            },
            {
                "_complex": false,
                "_expandable": [],
                "_row": 3,
                "code": "TakeLowerAggregation",
                "description": "Policy: take the value computed by aggregation lower levels of a hierarchy",
                "hierarchy_name": "AggregationConflictResolutionPolicies",
                "label": "Take value accumulated from lower levels"
            },
            {
                "_complex": false,
                "_expandable": [],
                "_row": 3,
                "code": "UseZero",
                "description": "Policy: use the zero value when a child is missing during hierarchy aggregation",
                "hierarchy_name": "AggregationMissingValueResolutionPolicies",
                "label": "Use zero in aggregations"
            },
            {
                "_complex": false,
                "_expandable": [],
                "_row": 3,
                "code": "Invalidate",
                "description": "Policy: invalidate the aggregation for a node in hierarchy if any of the children's value is missing",
                "hierarchy_name": "AggregationMissingValueResolutionPolicies",
                "label": "Invalidate aggregation"
            }

            ]
        }
    },
    {
      "command": "parameters",
      "label": "Parameters by default",
      "content":
        {
            "command_name": "Parameters",
                    "items": [
                        {
                            "_complex": false,
                            "_expandable": [],
                            "_row": 2,
                            "description": "Which algorithm to use for the solving",
                            "domain": "SolvingAlgorithms",
                            "group": "NISSolverParameters",
                            "name": "NISSolverType",
                            "type": "Code",
                            "value": "FlowGraph"
                        },
                        {
                            "_complex": false,
                            "_expandable": [],
                            "_row": 2,
                            "description": "Ordered list of consideration of observations",
                            "domain": null,
                            "group": "NISSolverParameters",
                            "name": "NISSolverObserversPriority",
                            "type": "String",
                            "value": ""
                        },
                        {
                            "_complex": false,
                            "_expandable": [],
                            "_row": 2,
                            "description": "Conflicting data resolution policy",
                            "domain": "AggregationConflictResolutionPolicies",
                            "group": "NISSolverParameters",
                            "name": "NISSolverAggregationConflictResolutionPolicy",
                            "type": "Code",
                            "value": "TakeUpper"
                        },
                        {
                            "_complex": false,
                            "_expandable": [],
                            "_row": 2,
                            "description": "Missing value resolution policy",
                            "domain": "AggregationMissingValueResolutionPolicies",
                            "group": "NISSolverParameters",
                            "name": "NISSolverMissingValueResolutionPolicy",
                            "type": "Code",
                            "value": "UseZero"
                        },
                        {
                            "_complex": false,
                            "_expandable": [],
                            "_row": 2,
                            "description": "Ordered list of computation sources to consider on conflicts",
                            "domain": null,
                            "group": "NISSolverParameters",
                            "name": "NISSolverComputationSourcesPriority",
                            "type": "String",
                            "value": "Scale, PartOfAggregation, InterfaceTypeAggregation, ScaleChange, Flow"
                        }
                    ]
        }
    }
]
"""