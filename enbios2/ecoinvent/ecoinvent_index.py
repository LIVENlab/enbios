from pathlib import Path
from typing import Optional, Union, Generator

from enbios2.base.databases import init_databases
from enbios2.base.db_models import EcoinventDataset
from enbios2.const import BASE_ECOINVENT_DATASETS_PATH
from enbios2.generic.enbios2_logging import get_logger

logger = get_logger(__file__)


def analyze_directory(directory: Optional[Path] = None,
                      store_to_index_file: bool = True) -> list[EcoinventDataset]:
    """
    Analyzes a directory and returns the dataset descriptors for the ecoinvent datasets in the folder.
    They should have been downloaded from the ecoinvent website, unzipped and not renamed.
    :param directory:
    :param store_to_index_file:
    :return:
    """
    if not directory:
        directory = BASE_ECOINVENT_DATASETS_PATH
    indexes: list[EcoinventDataset] = []

    for directory in directory.glob("*"):
        if directory.is_dir() and directory.name.startswith("ecoinvent"):
            parts = directory.name.split("_")
            parts = parts[0].split() + parts[1:]
            version = parts[1]
            system_model = parts[2]
            type_ = parts[-2] if parts[-2] in ["lci", "lcia"] else "default"
            xlsx = parts[-1] == "xlsx"
            indexes.append(EcoinventDataset(version=version,
                                            system_model=system_model,
                                            type=type_,
                                            xlsx=xlsx,
                                            directory=directory))
    if store_to_index_file:
        for index in indexes:
            if EcoinventDataset.identity_exists(index.identity):
                logger.debug(f"Ecoinvent dataset '{index.identity}' already indexed and will not be added")
                continue
            index.save()
            logger.info(f"Added ecoinvent dataset '{index.identity}'")
    return indexes


def get_ecoinvent_dataset_index(*,
                                version: Optional[Union[str, list[str]]] = None,
                                system_model: Optional[Union[str, list[str]]] = None,
                                type_: Optional[Union[str, list[str]]] = None,
                                xlsx: Optional[bool] = None) -> Generator[EcoinventDataset, None, None]:
    """
    Get the dataset index for the given parameters
    :param version:
    :param system_model:
    :param type_:
    :param xlsx:
    :return:
    """
    # build a query for the given parameters
    query = EcoinventDataset.select()
    if version:
        if isinstance(version, str):
            version = [version]
        query = query.where(EcoinventDataset.version.in_(version))
    if system_model:
        if isinstance(system_model, str):
            system_model = [system_model]
        query = query.where(EcoinventDataset.system_model.in_(system_model))
    if type_:
        if isinstance(type_, str):
            type_ = [type_]
        query = query.where(EcoinventDataset.type.in_(type_))
    if xlsx is not None:
        query = query.where(EcoinventDataset.xlsx == xlsx)
    return query


if __name__ == "__main__":
    init_databases()
    analyze_directory(store_to_index_file=True)

    print(list(get_ecoinvent_dataset_index(xlsx=True)))
    print(list(get_ecoinvent_dataset_index(xlsx=True))[0].dataset_path)
