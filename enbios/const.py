
from pathlib import Path


# Get the absolute path of the entry-point script
PROJECT_PATH = Path(__file__)
while PROJECT_PATH.name != "enbios":
    if PROJECT_PATH == PROJECT_PATH.parent:
        raise Exception("Could not find enbios project path")
    PROJECT_PATH = PROJECT_PATH.parent
# cuz we have a enbios subfolder in the project...
if PROJECT_PATH.parent.name == "enbios":
    PROJECT_PATH = PROJECT_PATH.parent

BASE_DATA_PATH = PROJECT_PATH / "data"
BASE_TEST_DATA_PATH = Path(__file__).parent.parent / "test/data"
