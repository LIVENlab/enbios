from pathlib import Path

import pytest

from enbios.const import BASE_TEST_DATA_PATH


@pytest.fixture(scope="module")
def tempfolder() -> Path:
    path = BASE_TEST_DATA_PATH / "temp"
    path.mkdir(parents=True, exist_ok=True)
    return path
