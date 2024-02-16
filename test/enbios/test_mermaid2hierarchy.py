import json
from pathlib import Path
from typing import Generator

import pytest

from enbios.const import BASE_TEST_DATA_PATH
from enbios.generic.mermaid2hierarchy import convert_file


def mermaid_files() -> Generator[Path, None, None]:
    for file in (BASE_TEST_DATA_PATH / "mermaid/inputs").glob("*.txt"):
        yield file


def mermaid_files_names():
    for file in (BASE_TEST_DATA_PATH / "mermaid/inputs").glob("*.txt"):
        yield file.name


@pytest.mark.parametrize('mermaid_file', argvalues=mermaid_files(), ids=mermaid_files_names())
def test_mermaid_files(mermaid_file: Path):
    hierarchy = convert_file(mermaid_file)
    print(json.dumps(hierarchy, indent=2))
    # json.dump(hierarchy,
    #           (BASE_TEST_DATA_PATH / f"mermaid/validate/{mermaid_file.stem}.json").open("w", encoding="utf-8"),
    #           indent=2)
    validation = json.load(
        (BASE_TEST_DATA_PATH / f"mermaid/validate/{mermaid_file.stem}.json").open(encoding="utf-8"))
    assert hierarchy == validation
