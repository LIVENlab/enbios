from pathlib import Path

import openpyxl
from playhouse.shortcuts import model_to_dict
from tqdm import tqdm

from enbios2.const import BASE_DATA_PATH
from enbios2.ecoinvent.ecoinvent_index import get_ecoinvent_dataset_path, EcoinventDatasetDescriptor
from enbios2.experiment.databases import add_db, DBTypes, check_exists, set_db_meta, get_model_classes, \
    prepare_db
from enbios2.experiment.db_models import ActivityLCI, ExchangeInfo


def create(file_path: Path, name: str, force_redo: bool = False):
    db_path = BASE_DATA_PATH / f"databases/{name}.sqlite"
    db_model_classes = get_model_classes(DBTypes.LCI)
    db = set_db_meta(db_path, db_model_classes)

    db.connect()

    if force_redo:
        db.drop_tables(db_model_classes)

    db.create_tables(db_model_classes)

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
            ExchangeInfo(exchange=exchange_names[index], compartment=compartment_names[index],
                         sub_compartment=sub_compartment_names[index], unit=units[index]).save()

        fields = list(ActivityLCI._meta.fields.keys())[1:]
        for activities in tqdm(row_generator):
            # print({k: v for k, v in zip(fields, activities[:6])})
            db_activity = ActivityLCI(**{k: v for k, v in zip(fields, activities[:6])})
            db_activity.data = activities[6:]
            db_activity.save()

    else:
        print(f"Database '{name}' already exists")
    db.close()

    add_db(name, db_path, DBTypes.LCI.value, {"orig_file_name": file_path.name})


def get_lci(db_name: str, code: str, drop_zero: bool = False):
    if not check_exists(db_name):
        raise ValueError(f"Database '{db_name}' does not exist")
    prepare_db(db_name)
    activity = ActivityLCI.get(ActivityLCI.code == code)
    exchange_info = ExchangeInfo.select()
    lci = []
    for index, e in enumerate(exchange_info):
        value = activity.data[index]
        if drop_zero and value == 0:
            continue
        exchange_data = model_to_dict(e, exclude=[ActivityLCI.id])
        exchange_data["value"] = value
        lci.append(exchange_data)
    return lci


path = get_ecoinvent_dataset_path(EcoinventDatasetDescriptor(version="3.9.1", system_model="cutoff", type="LCI"))
create(path, "ecoinvent3.9.1.cut-off.lci")

# a = get_lci("ecoinvent3.9.1.cut-off.lci", "739f38ee-b726-5bc5-b12e-8bb2df5a268c_b7b72951-a57c-4364-9b49-ea3c7cb03d00")
