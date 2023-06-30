
from peewee import SqliteDatabase

from enbios2.base.db_models import EcoinventResolvedDataset, MainDatabase, EcoinventDataset, BWProjectIndex
from enbios2.const import BASE_DATABASES_PATH


def guarantee_db_dir():
    BASE_DATABASES_PATH.mkdir(parents=True, exist_ok=True)


def init_databases() -> SqliteDatabase:
    guarantee_db_dir()
    database = MainDatabase._meta.database
    database.connect(True)
    if not EcoinventResolvedDataset.table_exists():
        database.create_tables([EcoinventResolvedDataset, EcoinventDataset, BWProjectIndex])
    # EcoinventResolvedDataset.select()
    return database
