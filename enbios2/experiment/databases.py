from enum import Enum
from pathlib import Path

from peewee import Model, TextField, SqliteDatabase
from playhouse.sqlite_ext import JSONField

from enbios2.const import BASE_DATA_PATH

class DBTypes(Enum):
    LCI = "LCI"

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
    Metadata(name=name, path= db_path, db_type=db_type, metadata=metadata).save()
    database.close()
