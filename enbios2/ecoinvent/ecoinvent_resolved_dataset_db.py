"""
Create/Retrieve sqlite databases from the ecoinvent Excel files (lci, lcia)
The databases are stored in the BASE_DATABASES_PATH folder (data/databases)
Both databases have two tables:
    - activities_{lci|lcia}
        - with the fields: code, name, ... data (which is an array with all values, for the exchanges and
            impacts respective order as in the 2nd table)
    - ExchangeInfo | ImpactInfo (names of biosphere flows | impact categories)
        - with the fields:
            ExchangeInfo : ['exchange', 'compartment', 'sub_compartment', 'unit']
            ImpactInfo : ['method', 'category', 'indicator', 'unit']

"""
from enum import Enum
from pathlib import Path
from typing import Type

import openpyxl
from peewee import Model, OperationalError, SqliteDatabase
from playhouse.shortcuts import model_to_dict
from tqdm import tqdm

from enbios2.base.databases import init_databases
from enbios2.base.db_models import EcoinventDatabaseActivity, EcoinventResolvedDataset, ExchangeInfo, ImpactInfo, \
    FTS_BW_ActivitySimple, BW_Activity, EcoinventDataset
from enbios2.const import BASE_DATABASES_PATH
from enbios2.ecoinvent.ecoinvent_index import get_ecoinvent_dataset_index
from enbios2.generic.enbios2_logging import get_logger
from enbios2.generic.util import get_enum_by_value

logger = get_logger(__file__)


class DBTypes(Enum):
    LCI = "lci"
    LCIA = "lcia"
    ActivityFTS = "ActivityFTS"  # experimental


def get_ecoinvent_resolved_entry(name: str) -> EcoinventResolvedDataset:
    db_metadata = EcoinventResolvedDataset.get(EcoinventResolvedDataset.name == name)
    return db_metadata


def set_ecoinvent_resolved_entry(db_path: Path, classes: list[Type[Model]]) -> SqliteDatabase:
    db = SqliteDatabase(db_path)
    for cls in classes:
        cls._meta.database = db
    return db


def get_model_classes(db_type: DBTypes) -> list[Type[Model]]:
    if db_type == DBTypes.LCI:
        return [EcoinventDatabaseActivity, ExchangeInfo]
    if db_type == DBTypes.LCIA:
        return [EcoinventDatabaseActivity, ImpactInfo]
    if db_type == DBTypes.ActivityFTS:
        return [BW_Activity, FTS_BW_ActivitySimple]


def check_exists(name: str) -> bool:
    exists = EcoinventResolvedDataset.select().where(EcoinventResolvedDataset.name == name).exists()
    return exists


def prepare_db(db_name: str, db_type: DBTypes, create_tables: bool = True, force_redo: bool = False) -> \
        tuple[SqliteDatabase, Path, list[Type[Model]]]:
    db_path = BASE_DATABASES_PATH / f"{db_name}.sqlite"
    # call init_ecoinvent_resolved_database
    db = SqliteDatabase(db_path, {'journal_mode': 'wal',
                                  'cache_size': -1024 * 32})
    db_model_classes = get_model_classes(db_type)
    for model in db_model_classes:
        model._meta.database = db
    # set table name to activity_lci or activity_lcia
    db_model_classes[0]._meta.table_name = f"activity_{db_type.value}"

    if create_tables:
        try:
            db.connect()
        except OperationalError as e:
            logger.error(f"Could not connect to database {db_path}")
            raise e

        if force_redo:
            db.drop_tables(db_model_classes)
        if not db_model_classes[0].table_exists():
            db.create_tables(db_model_classes)

    return db, db_path, db_model_classes


def create(dataset: EcoinventDataset, force_redo: bool = False):
    db_name = "_".join(dataset.identity.split("_")[:-1])
    file_path = dataset.dataset_path
    type_ = get_enum_by_value(DBTypes, dataset.type)
    assert file_path.exists() and file_path.suffix == ".xlsx", f"File {file_path} does not exist or is not an xlsx file"
    assert type_ in [DBTypes.LCI, DBTypes.LCIA], f"Type must be either {DBTypes.LCI} or {DBTypes.LCIA}"
    init_databases()
    db, db_path, db_model_classes = prepare_db(db_name, type_,)

    # count the rows
    activity_count = EcoinventDatabaseActivity.select().count()
    if activity_count == 0:

        reader = openpyxl.load_workbook(file_path.as_posix(), read_only=True, data_only=True)
        lci_sheet = reader[type_.value.upper()]

        # impacts/exchanges start from G -> 6
        row_generator = lci_sheet.values
        data_index_fields: list[str] = [next(row_generator)[6:],
                                        next(row_generator)[6:],
                                        next(row_generator)[6:],
                                        next(row_generator)[6:]]
        index_table = db_model_classes[1]
        data_fields_names = list(index_table._meta.fields.keys())[1:]

        if type_ == DBTypes.LCI:
            assert data_fields_names == ['exchange', 'compartment', 'sub_compartment', 'unit']
        if type_ == DBTypes.LCIA:
            assert data_fields_names == ['method', 'category', 'indicator', 'unit']

        batch_size = 1000
        batch = []

        def insert_batch(table: Type[Model], batch: list[dict]):
            table.insert_many(batch).execute()
            batch.clear()

        for index, _ in enumerate(data_index_fields[0]):
            batch.append({k: v[index] for k, v in zip(data_fields_names, data_index_fields)})
            if len(batch) == batch_size:
                insert_batch(index_table, batch)

        insert_batch(index_table, batch)

        fields = list(EcoinventDatabaseActivity._meta.fields.keys())[1:]

        for activities in tqdm(row_generator):
            # print({k: v for k, v in zip(fields, activities[:6])})
            db_act = {k: v for k, v in zip(fields, activities[:6])}
            db_act["data"] = activities[6:]
            batch.append(db_act)
            if len(batch) == batch_size:
                insert_batch(EcoinventDatabaseActivity, batch)
        insert_batch(EcoinventDatabaseActivity, batch)

    else:
        print(f"Database '{db_name}' already exists")
    db.close()

    EcoinventResolvedDataset.create(name=db_name,
                                    path=db_path,
                                    db_type=type_.value,
                                    ecoinvent_dataset=dataset,
                                    metadata={"orig_file_name": file_path.name})


def create_from_ecoinvent_dataset(ds: EcoinventDataset):
    if ds.type == "default":  # from valid_ecoinvent_datatypes
        raise ValueError(f"Dataset type {ds.type} not supported. Must be either 'lci' or 'lcia'")
    if not ds.xlsx:
        raise ValueError(f"Dataset {ds.name} must be an xlsx file")

    db_name = "_".join(ds.identity.split("_")[:-1])
    if not check_exists(db_name):
        logger.info(f"Creating database '{db_name}'")
        create(ds)
    else:
        logger.info(f"Database '{db_name}' already exists")


def get_activity_data(db_name: str, code: str, drop_zero: bool = False):
    """
    Get the data for an activity in a database
    :param db_name:
    :param code:
    :param drop_zero:
    :return:
    """
    if not check_exists(db_name):
        raise ValueError(f"Database '{db_name}' does not exist")

    db_entry = EcoinventResolvedDataset.select().where(EcoinventResolvedDataset.name == db_name).get()
    db_type = get_enum_by_value(DBTypes, db_entry.db_type)

    db, db_path, db_model_classes = prepare_db(db_name, db_type)

    assert db_type in [DBTypes.LCI, DBTypes.LCIA], f"Database type must be either {DBTypes.LCI} or {DBTypes.LCIA}"

    activity = EcoinventDatabaseActivity.get(EcoinventDatabaseActivity.code == code)
    data_index_rows = db_model_classes[1].select()
    data_ = []
    for index, e in enumerate(data_index_rows):
        value = activity.data[index]
        if drop_zero and value == 0:
            continue
        exchange_data = model_to_dict(e, exclude=[db_model_classes[1].id])
        exchange_data["value"] = value
        data_.append(exchange_data)
    return data_


if __name__ == "__main__":
    # EcoinventDataset.select()
    init_databases()
    datasets = get_ecoinvent_dataset_index(type_=["lci", "lcia"], xlsx=True)
    print(list(datasets))
    create_from_ecoinvent_dataset(list(datasets)[1])
    print(get_activity_data("cutoff_3.9.1_lci",
                            "d195d4a3-6ae5-54e4-9954-7f915cf08668_d69294d7-8d64-4915-a896-9996a014c410"))
