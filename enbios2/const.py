from pathlib import Path


# enbios2/enbios2/const.py
PROJECT_PATH = Path(__file__).parent.parent.absolute()
BASE_DATA_PATH = PROJECT_PATH / "data"
BASE_ECOINVENT_DATASETS_PATH = BASE_DATA_PATH / "ecoinvent"
BASE_DATABASES_PATH = BASE_DATA_PATH / "databases"
MAIN_DATABASE_PATH = BASE_DATABASES_PATH / "meta.sqlite"
BASE_SCHEMA_PATH = BASE_DATA_PATH / "schema"

BASE_TEST_DATA_PATH = BASE_DATA_PATH / "test_data"

# Important data paths/files
BW_PROJECT_INDEX_FILE = BASE_DATA_PATH / "bw_project_index.json"
