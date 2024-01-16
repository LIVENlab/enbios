from pathlib import Path

import os
import sys

# Get the absolute path of the entry-point script
entry_point_script = Path(os.path.abspath(sys.argv[0]))

PROJECT_PATH = entry_point_script.parent

BASE_DATA_PATH = PROJECT_PATH / "data"
BASE_ECOINVENT_DATASETS_PATH = BASE_DATA_PATH / "ecoinvent"
BASE_DATABASES_PATH = BASE_DATA_PATH / "databases"
MAIN_DATABASE_PATH = BASE_DATABASES_PATH / "meta.sqlite"
BASE_SCHEMA_PATH = BASE_DATA_PATH / "schema"

BASE_TEST_DATA_PATH = Path(__file__).parent.parent / "test/data"
