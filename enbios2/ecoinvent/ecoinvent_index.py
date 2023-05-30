import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Literal, Union, Optional

from enbios2.const import BASE_DATA_PATH, ECOINVENT_INDEX_FILE
from enbios2.generic.files import ReadPath

ecoinvent_versions = Literal["3.8", "3.9", "3.9.1"]
ecoinvent_system_models = Literal["cutoff", "consequential", "apos"]
ecoinvent_dataset_types = Literal["default", "lci", "lcia"]


@dataclass
class EcoinventDatasetDescriptor:
    version: ecoinvent_versions
    system_model: ecoinvent_system_models
    type: ecoinvent_dataset_types = "default"
    xlsx: bool = False
    path: Optional[Path] = None

    def __post_init__(self):
        if isinstance(self.path, str):
            self.path = Path(self.path)

    def __hash__(self):
        return hash((self.version, self.system_model, self.type, self.xlsx))

    def __eq__(self, other: "EcoinventDatasetDescriptor"):
        if isinstance(other, EcoinventDatasetDescriptor):
            return ((self.version, self.system_model, self.type, self.xlsx) ==
                    (other.version, other.system_model, other.type, other.xlsx))
        return False

    @staticmethod
    def path_to_posix(dict_: list[tuple[str, any]]):
        result = {}
        for k, v in dict_:
            if isinstance(v, Path):
                result[k] = v.as_posix()
            else:
                result[k] = v
        return result


def analyze_directory(directory: Path, store_to_index_file: bool = True) -> list[EcoinventDatasetDescriptor]:
    """
    Analyzes a directory and returns the dataset descriptors for the ecoinvent datasets in the folder.
    They should have been downloaded from the ecoinvent website, unzipped and not renamed.
    :param directory:
    :param store_to_index_file:
    :return:
    """
    indexes = []
    existing_indexes = _read_ecoinvent_index_file()

    for file in directory.glob("*"):
        if file.is_dir() and file.name.startswith("ecoinvent"):
            parts = file.name.split("_")
            parts = parts[0].split() + parts[1:]
            version = parts[1]
            system_model = parts[2]
            type_ = parts[-2] if parts[-2] in ["lci", "lcia"] else "default"
            xlsx = parts[-1] == "xlsx"
            descr = EcoinventDatasetDescriptor(version=version,
                                               system_model=system_model,
                                               type=type_,
                                               xlsx=xlsx,
                                               path=file)
            if descr not in existing_indexes:
                indexes.append(descr)
    if store_to_index_file:
        write_descriptor(indexes)
    return indexes


def get_ecoinvent_dataset_path(descr: EcoinventDatasetDescriptor) -> Path:
    """
    get the definite path of the dataset (spold files) or excel file
    :param descr:
    :return:
    """
    for index in _read_ecoinvent_index_file():
        if index == descr:
            if index.xlsx:
                return next(index.path.glob("*.xlsx"))
            else:
                return index.path / f"datasets"


def write_descriptor(descriptor: Union[EcoinventDatasetDescriptor, list[EcoinventDatasetDescriptor]]):
    """
    Writes the descriptor to the index file
    :param descriptor:
    :return:
    """
    indexes: list[EcoinventDatasetDescriptor] = _read_ecoinvent_index_file()
    if isinstance(descriptor, list):
        indexes.extend(descriptor)
    else:
        indexes.append(descriptor)

    ReadPath(ECOINVENT_INDEX_FILE).write_text(json.dumps([
        asdict(index, dict_factory=EcoinventDatasetDescriptor.path_to_posix)
        for index in indexes], indent=2))


def _read_ecoinvent_index_file() -> list[EcoinventDatasetDescriptor]:
    if not Path(ECOINVENT_INDEX_FILE).exists():
        Path(ECOINVENT_INDEX_FILE).write_text("[]")
    return [EcoinventDatasetDescriptor(**descr) for descr in ReadPath(ECOINVENT_INDEX_FILE).read_data()]


p = BASE_DATA_PATH / "ecoinvent"
indexes = analyze_directory(p)

path = get_ecoinvent_dataset_path(EcoinventDatasetDescriptor(version="3.9.1", system_model="cutoff", type="lci"))
