from enum import Enum
from pathlib import Path
from typing import Type

from peewee import Model, TextField, SqliteDatabase
from playhouse.sqlite_ext import JSONField

from enbios2.const import BASE_DATA_PATH


class DBTypes(Enum):
    LCI = "LCI"


def get_model_classes(db_type: DBTypes) -> list[Type[Model]]:
    if db_type == DBTypes.LCI:
        from enbios2.experiment.db_models import ActivityLCI, ExchangeInfo
        return [ActivityLCI, ExchangeInfo]


class Metadata(Model):
    name = TextField()
    path = TextField()
    db_type = TextField()
    metadata = JSONField()

    class Meta:
        pass


def init() -> SqliteDatabase:
    database = SqliteDatabase(BASE_DATA_PATH / f"databases/meta.sqlite")
    Metadata._meta.database = database
    database.connection()
    database.create_tables([Metadata])
    return database


def add_db(name: str, db_path: Path, db_type: DBTypes, metadata: dict):
    database = init()
    Metadata(name=name, path=db_path, db_type=db_type, metadata=metadata).save()
    database.close()


def check_exists(name: str) -> bool:
    database = init()
    exists = Metadata.select().where(Metadata.name == name).exists()
    database.close()
    return exists


def set_db_meta(db_path: Path, classes: list[Type[Model]]) -> SqliteDatabase:
    db = SqliteDatabase(db_path)
    for cls in classes:
        cls._meta.database = db
    return db


def get_db_meta(name: str) -> Metadata:
    database = init()
    db_metadata = Metadata.get(Metadata.name == name)
    database.close()
    return db_metadata


def prepare_db(name: str):
    db_metadata = get_db_meta(name)
    db = set_db_meta(Path(db_metadata.path), get_model_classes(DBTypes(db_metadata.db_type)))
