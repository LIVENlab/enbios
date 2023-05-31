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
from pathlib import Path
from typing import Type

import openpyxl
from peewee import Model, OperationalError
from playhouse.shortcuts import model_to_dict
from tqdm import tqdm

from enbios2.base.databases import add_db, DBTypes, check_exists, set_db_meta, get_model_classes, \
    prepare_db, set_model_table_name, guarantee_db_dir
from enbios2.base.db_models import EcoinventDatabaseActivity, ExchangeInfo
from enbios2.const import BASE_DATABASES_PATH
from enbios2.ecoinvent.ecoinvent_index import get_ecoinvent_dataset_path, EcoinventDatasetDescriptor
from enbios2.generic.enbios2_logging import get_logger
from enbios2.generic.files import ReadDataPath

logger = get_logger(__file__)


def create(type_: DBTypes, file_path: Path, name: str, force_redo: bool = False):
    guarantee_db_dir()
    assert file_path.exists() and file_path.suffix == ".xlsx", f"File {file_path} does not exist or is not an xlsx file"
    assert type_ in [DBTypes.LCI, DBTypes.LCIA], f"Type must be either {DBTypes.LCI} or {DBTypes.LCIA}"
    db_path = BASE_DATABASES_PATH / f"{name}.sqlite"
    db_model_classes = get_model_classes(type_)
    db = set_db_meta(db_path, db_model_classes)
    # set table name to activity_lci or activity_lcia
    set_model_table_name(db_model_classes[0], f"activity_{type_.value}")
    try:
        db.connect()
    except OperationalError as e:
        logger.error(f"Could not connect to database {db_path}")
        raise e

    if force_redo:
        db.drop_tables(db_model_classes)

    db.create_tables(db_model_classes)

    # count the rows
    activity_count = EcoinventDatabaseActivity.select().count()
    if activity_count == 0:

        reader = openpyxl.load_workbook(file_path.as_posix(), read_only=True, data_only=True)
        lci_sheet = reader[type_.value]

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
            table.insert_many(batch)
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
        print(f"Database '{name}' already exists")
    db.close()

    add_db(name, db_path, DBTypes.LCI.value, {"orig_file_name": file_path.name})


def get_lcia(db_name: str, code: str, drop_zero: bool = False):
    if not check_exists(db_name):
        raise ValueError(f"Database '{db_name}' does not exist")
    prepare_db(db_name)
    activity = EcoinventDatabaseActivity.get(EcoinventDatabaseActivity.code == code)
    exchange_info = ExchangeInfo.select()
    lci = []
    for index, e in enumerate(exchange_info):
        value = activity.data[index]
        if drop_zero and value == 0:
            continue
        exchange_data = model_to_dict(e, exclude=[EcoinventDatabaseActivity.id])
        exchange_data["value"] = value
        lci.append(exchange_data)
    return lci


def create_if_not_exists(file_path: ReadDataPath, name: str):
    if not check_exists(name) or True:
        logger.info(f"Creating database '{name}'")
        create(DBTypes.LCIA, file_path, name)


path = get_ecoinvent_dataset_path(
    EcoinventDatasetDescriptor(version="3.9.1",
                               system_model="cutoff",
                               type="lci",
                               xlsx=True))

create(DBTypes.LCI, path, "ecoinvent3.9.1.cut-off.lci")
