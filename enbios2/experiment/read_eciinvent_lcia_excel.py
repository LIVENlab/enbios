from pathlib import Path

import openpyxl
from peewee import TextField, Model, UUIDField, FloatField, SqliteDatabase
from tqdm import tqdm

from enbios2.const import BASE_DATA_PATH
from enbios2.experiment.databases import add_db, DBTypes
from sqlite import TupleJSONField


class ActivityLCI(Model):
    code = UUIDField(unique=True)
    name = TextField(index=True)
    location = TextField()
    product = TextField()
    product_unit = TextField()
    amount = FloatField()
    data = TupleJSONField()

    class Meta:
        pass


class ExchangeInfo(Model):
    exchange_name = TextField()
    compartment_name = TextField()
    sub_compartment_name = TextField()
    unit = TextField()

    class Meta:
        pass




def set_db_meta(db_path: Path):
    db = SqliteDatabase(db_path)
    ActivityLCI._meta.database = db
    ExchangeInfo._meta.database = db


def create(file_path: Path, name: str, force_redo: bool = False):
    db = SqliteDatabase(BASE_DATA_PATH / f"databases/{file_path.stem}.sqlite")

    ActivityLCI._meta.database = db
    ExchangeInfo._meta.database = db

    db.connect()

    if force_redo:
        db.drop_tables([ActivityLCI, ExchangeInfo])

    db.create_tables([ActivityLCI, ExchangeInfo, Metadata])

    # count the rows
    activity_count = ActivityLCI.select().count()
    if activity_count == 0:

        reader = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
        lci_sheet = reader["LCI"]

        # exchanges start from G -> 6
        row_generator = lci_sheet.values
        exchange_names = next(row_generator)[6:]
        compartment_names = next(row_generator)[6:]
        sub_compartment_names = next(row_generator)[6:]
        units = next(row_generator)[6:]

        for index, _ in enumerate(exchange_names):
            ExchangeInfo(exchange_name=exchange_names[index], compartment_name=compartment_names[index],
                         sub_compartment_name=sub_compartment_names[index], unit=units[index]).save()

        fields = list(ActivityLCI._meta.fields.keys())[1:]
        for activities in tqdm(row_generator):
            # print({k: v for k, v in zip(fields, activities[:6])})
            db_activity = ActivityLCI(**{k: v for k, v in zip(fields, activities[:6])})
            db_activity.data = activities[6:]
            db_activity.save()

    else:
        print("Database already exists")
    db.close()

    add_db(name, BASE_DATA_PATH / f"databases/{file_path.stem}.sqlite",
           DBTypes.LCI, {"orig_file_name": file_path.name})


def get(db_name: str, code: str):

    activity = ActivityLCI.get(ActivityLCI.code == code)
    return activity


create(BASE_DATA_PATH / "ecoinvent/ecoinvent 3.9.1_cutoff_cumulative_lci_xlsx/Cut-off Cumulative LCI v3.9.1.xlsx",
       "ecoinvent3.9.1.cut-off")
