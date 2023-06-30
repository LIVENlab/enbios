from pathlib import Path
from typing import Optional, Union

import bw2data
import bw2io
from peewee import BackrefAccessor, JOIN

from enbios2.base.databases import init_databases
from enbios2.base.db_models import EcoinventDataset, BWProjectIndex
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


def add_dataset_index(version: str,
                      system_model: str,
                      type: str,
                      xlsx: str,
                      directory: str):
    """
    Add a dataset index
    :param version:
    :param system_model:
    :param type:
    :param xlsx:
    :param directory:
    :return:
    """
    ds = EcoinventDataset(version=version,
                          system_model=system_model,
                          type=type,
                          xlsx=xlsx,
                          directory=directory)
    if EcoinventDataset.identity_exists(ds.identity):
        logger.debug(f"Ecoinvent dataset '{ds.identity}' already indexed and will not be added")
        return
    ds.save()


def get_ecoinvent_dataset_index(*,
                                version: Optional[Union[str, list[str]]] = None,
                                system_model: Optional[Union[str, list[str]]] = None,
                                type_: Optional[Union[str, list[str]]] = None,
                                xlsx: Optional[bool] = None,
                                has_bw_project: Optional[bool] = None) -> list[EcoinventDataset]:
    """
    Get the dataset index for the given parameters
    :param version: ecoinvent version, one or multiple
    :param system_model: system model, one or multiple [cut-off, consequential, apos]
    :param type_: type, one or multiple [lci, lcia, default]
    :param xlsx: True if the dataset is in xlsx format
    :return: list of EcoinventDataset
    """
    # build a query for the given parameters
    init_databases()
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
    if has_bw_project is not None:
        query = query.select().join(BWProjectIndex, JOIN.LEFT_OUTER).where(
            BWProjectIndex.ecoinvent_dataset.is_null(not has_bw_project))
    return list(query)

    def is_resolved_database_available(dataset: EcoinventDataset):
        """
        Checks if the resolved database (lci, lcia from excel) is available for the given dataset
        :param dataset:
        :return:
        """
        pass

    def auto_import(eods: EcoinventDataset,
                    project_name: Optional[str] = "ecoinvent",
                    database_name: Optional[str] = None) -> Optional[bw2io.SingleOutputEcospold2Importer]:
        """
        Automatically imports the given ecoinvent dataset into a new project and database.
        Also creates the BWProjectIndex
        YOU SHOULD MAKE SURE THAT BRIGHTWAY DATA CAN DEAL WITH THAT PARTICULAR VERSION OF ECOINVENT.

        :param eods: eoinvent dataset (should be indexed)
        :param project_name:
        :param database_name:
        :return:
        """
        if eods.xlsx or not eods.type == "default":
            raise ValueError(f"Only default datasets and non xlsx are supported. Passed: {eods}")
        exists = get_ecoinvent_dataset_index(version=eods.version,
                                             system_model=eods.system_model,
                                             type_=eods.type,
                                             xlsx=eods.xlsx)
        if not exists:
            eods.save()
        else:
            eods = exists[0]
        if eods.bw_project_index:
            logger.info(f"Already imported and indexed: {eods.bw_project_index}")
            return
        if project_name in bw2data.projects:
            logger.debug(f"Project already exists: {project_name}. switching to it.")
            bw2data.projects.set_current(project_name)
        else:
            logger.debug(f"Creating new project. {project_name}")
            bw2data.projects.create_project(project_name)
            bw2data.projects.set_current(project_name)
            bw2io.bw2setup()

        if not database_name:
            database_name = eods.identity
        if database_name in bw2data.databases:
            raise ValueError(f"Database already exists: {database_name}")
        logger.info(f"Importing ecoinvent dataset to {project_name}/{database_name}")
        importer = bw2io.SingleOutputEcospold2Importer(eods.dataset_path.as_posix(), database_name)
        importer.apply_strategies()
        importer.statistics()
        if importer.statistics()[2] == 0:
            importer.write_database()
            BWProjectIndex.create(project_name=project_name, database_name=database_name, ecoinvent_dataset=eods)
        else:
            print("There are unlinked exchanges. Database will not be written. Method returns importer "
                  "(you can inspect, manipulable and write it manually).")
        return importer

    def analyse_and_import():
        """
        analyse the ecoinvent directory and import all datasets
        """
        init_databases()
        analyze_directory(store_to_index_file=True)
        indexes = get_ecoinvent_dataset_index()
        for index in indexes:
            if index.version.startswith("3.9"):
                try:
                    auto_import(index)
                except ValueError as e:
                    logger.warning(e)

    if __name__ == "__main__":
        analyse_and_import()

        # bw2data.projects.delete_project("ecoinvent", True)

        # print(list(get_ecoinvent_dataset_index(xlsx=True)))
        # print(list(get_ecoinvent_dataset_index(xlsx=True))[0].dataset_path)
