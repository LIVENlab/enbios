from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union, Literal

from voluptuous import default_factory

from enum import Enum


class EcoInventVersion(Enum):
    _V391Plus = "3.9.1+"
    _V391 = "3.9.1"
    _V39 = "3.9"
    _V38 = "3.8"
    VERSION_UNDEFINED = "version_undefined"
    NOT_ECOINVENT = "not_ecoinvent"


@dataclass
class BWProject:
    project_name: str
    databases: list["BWProjectDatabase"] = default_factory(list)


@dataclass
class BWProjectDatabase:
    name: str
    source: Union[str, Path]
    importer: Optional[str] = None
    format: Optional[Literal[
        "CSV", "Ecospold1", "Ecoinvent", "Excel", "Exiobase 3.3.17 hybrid mrio_common_metadata tidy datapackage",
        "Exiobase 3", "SimaPro", "Ecospold2"]] = None
    ecoinvent_version: Optional[EcoInventVersion] = EcoInventVersion.VERSION_UNDEFINED


def __post_init__(self):
    if self.importer is None and self.format is None:
        raise Exception("Either importer or format must be specified")
    if self.ecoinvent_version == EcoInventVersion._V38:
        import bw2io
        if bw2io.__version__ != (0, 9, "DEV11"):
            raise Exception(f"bw2io version must be 0.9.DEV11 for ecoinvent {self.ecoinvent_version}")
