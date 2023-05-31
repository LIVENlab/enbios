from enum import Enum
from pathlib import Path
from typing import Type, Any

from peewee import Model, SqliteDatabase

from enbios2.base.db_models import BW_Activity, FTS_BW_ActivitySimple, EcoinventDatabaseActivity, ExchangeInfo, \
    ImpactInfo, Enbios2DBMetadata, MainDatabase, EcoinventDataset, BWProjectIndex
from enbios2.const import BASE_DATA_PATH, BASE_DATABASES_PATH, MAIN_DATABASE_PATH


# from playhouse.sqlite_ext import JSONField


class DBTypes(Enum):
    LCI = "LCI"
    LCIA = "LCIA"
    ActivityFTS = "ActivityFTS"  # experimental


def type_pragmas(db_type: DBTypes) -> dict[str, Any]:
    return {'journal_mode': 'wal',
            'cache_size': -1024 * 32}


# if db_type == DBTypes.LCI:
#     return []
# if db_type == DBTypes.LCIA:
#     return []
# if db_type == DBTypes.ActivityFTS:
#     return [
#         ('journal_mode', 'wal'),
#         ('cache_size', -1024 * 32)]

def get_model_classes(db_type: DBTypes) -> list[Type[Model]]:
    if db_type == DBTypes.LCI:
        return [EcoinventDatabaseActivity, ExchangeInfo]
    if db_type == DBTypes.LCIA:
        return [EcoinventDatabaseActivity, ImpactInfo]
    if db_type == DBTypes.ActivityFTS:
        return [BW_Activity, FTS_BW_ActivitySimple]


def guarantee_db_dir():
    BASE_DATABASES_PATH.mkdir(parents=True, exist_ok=True)


def init_databases() -> SqliteDatabase:
    guarantee_db_dir()
    database = MainDatabase._meta.database
    database.connect()
    database.create_tables([Enbios2DBMetadata, EcoinventDataset, BWProjectIndex])
    return database


def add_db(name: str, db_path: Path, db_type: DBTypes, metadata: dict):
    # todo do replace when name and path are the same
    database = init_databases()
    Enbios2DBMetadata(name=name, path=db_path, db_type=db_type, metadata=metadata).save()
    database.close()


def check_exists(name: str) -> bool:
    database = init_databases()
    exists = Enbios2DBMetadata.select().where(Enbios2DBMetadata.name == name).exists()
    database.close()
    return exists


def set_db_meta(db_path: Path, classes: list[Type[Model]]) -> SqliteDatabase:
    db = SqliteDatabase(db_path)
    for cls in classes:
        cls._meta.database = db
    return db

def set_model_table_name(cls: Type[Model], name: str):
    cls._meta.table_name = name


def get_db_meta(name: str) -> Enbios2DBMetadata:
    database = init_databases()
    db_metadata = Enbios2DBMetadata.get(Enbios2DBMetadata.name == name)
    database.close()
    return db_metadata


def create_db(db_path: Path, db_type: DBTypes):
    """
    create a new db, set
    :param db_path:
    :param db_type:
    :return:
    """
    # todo, add to metadata,
    pragmas = type_pragmas(db_type)
    database = SqliteDatabase(db_path, pragmas)
    models = get_model_classes(db_type)
    set_db_meta(db_path, models)
    database.create_tables(models)
    add_db(db_path.stem, db_path, db_type, {})


# todo use "create_db" instead
def prepare_db(name: str):
    db_metadata = get_db_meta(name)
    db = set_db_meta(Path(db_metadata.path), get_model_classes(DBTypes(db_metadata.db_type)))
