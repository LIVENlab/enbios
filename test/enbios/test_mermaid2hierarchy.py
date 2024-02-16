from pathlib import Path
from typing import Generator

import pytest

from enbios.const import BASE_TEST_DATA_PATH
from enbios.generic.mermaid2hierarchy import convert_file


def mermaid_files() -> Generator[Path, None, None]:
    for file in (BASE_TEST_DATA_PATH / "mermaid").glob("*.txt"):
        yield file


def mermaid_files_names():
    for file in (BASE_TEST_DATA_PATH / "mermaid").glob("*.txt"):
        yield file.name


@pytest.mark.parametrize('mermaid_file', argvalues=mermaid_files(), ids=mermaid_files_names())
def test_mermaid_files(mermaid_file):
    convert_file(mermaid_file)
