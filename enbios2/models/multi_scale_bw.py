from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union


@dataclass
class BWDatabase:
    name: str
    source: Union[str,Path]
    importer: Optional[str] = None
    format: Optional[str] = None

    def __post_init__(self):
        if self.importer is None and self.format is None:
            raise Exception("Either importer or format must be specified")

@dataclass
class BWSetup:
    project_name: str
    databases: list[BWDatabase]