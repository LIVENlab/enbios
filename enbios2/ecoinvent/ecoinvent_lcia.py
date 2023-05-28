from pathlib import Path

import openpyxl
from playhouse.shortcuts import model_to_dict
from tqdm import tqdm

from enbios2.const import BASE_DATA_PATH
from enbios2.experiment.databases import add_db, DBTypes, check_exists, set_db_meta, get_model_classes, \
    prepare_db
from enbios2.experiment.db_models import ActivityLCI, ExchangeInfo, ImpactInfo
from enbios2.generic.enbios2_logging import get_logger
from enbios2.generic.files import ReadDataPath

logger = get_logger(__file__)


def create(file_path: Path, name: str, force_redo: bool = False):
    db_path = BASE_DATA_PATH / f"databases/{name}.sqlite"
    db_model_classes = get_model_classes(DBTypes.LCIA)
    db = set_db_meta(db_path, db_model_classes)

    db.connect()

    if force_redo:
        db.drop_tables(db_model_classes)

    db.create_tables(db_model_classes)

    # count the rows
    activity_count = ActivityLCI.select().count()
    if activity_count == 0:

        reader = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
        lci_sheet = reader["LCIA"]

        # exchanges start from G -> 6
        row_generator = lci_sheet.values
        method_names = next(row_generator)[6:]
        category_names = next(row_generator)[6:]
        indicator_names = next(row_generator)[6:]
        units = next(row_generator)[6:]

        for index, _ in enumerate(method_names):
            ImpactInfo(method=method_names[index], category=category_names[index],
                       indicator=indicator_names[index], unit=units[index]).save()

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


def get_lcia(db_name: str, code: str, drop_zero: bool = False):
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


def create_if_not_exists(file_path: ReadDataPath, name: str):
    if not check_exists(name):
        logger.info(f"Creating database '{name}'")
        create(file_path, name)


# logger.info("hi")
create_if_not_exists(
    ReadDataPath("ecoinvent/ecoinvent 3.9.1_cutoff_cumulative_lcia_xlsx/Cut-off Cumulative LCIA v3.9.1.xlsx"),
    "ecoinvent3.9.1.cut-off.lcia")
